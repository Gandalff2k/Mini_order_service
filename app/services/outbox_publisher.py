from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.backoff import next_attempt_at
from app.core.events import build_envelope
from app.infra.kafka import MessageProducer
from app.infra.settings import OutboxSettings
from app.models import OutboxMessage, OutboxStatus
from app.repositories.outbox_repository import OutboxRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishOutcome:
    published: int
    rescheduled: int
    failed: int

    @property
    def claimed(self) -> int:
        return self.published + self.rescheduled + self.failed


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: MessageProducer,
        settings: OutboxSettings,
        topic: str,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._settings = settings
        self._topic = topic

    async def publish_pending(self) -> PublishOutcome:
        async with self._session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            now = datetime.now(UTC)
            messages = list(await repository.claim_batch(self._settings.batch_size, now))
            if not messages:
                return PublishOutcome(0, 0, 0)

            errors = await self._send(messages)
            return self._settle(messages, errors, now)

    async def _send(self, messages: Sequence[OutboxMessage]) -> list[BaseException | None]:
        envelopes = [build_envelope(message) for message in messages]
        sends = [
            self._producer.send_and_wait(
                topic=self._topic,
                value=envelope.value,
                key=envelope.key,
                headers=envelope.headers,
            )
            for envelope in envelopes
        ]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*sends, return_exceptions=True),
                timeout=self._settings.publish_timeout_seconds,
            )
        except TimeoutError as timeout:
            return [timeout] * len(messages)
        return [result if isinstance(result, BaseException) else None for result in results]

    def _settle(
        self,
        messages: Sequence[OutboxMessage],
        errors: Sequence[BaseException | None],
        now: datetime,
    ) -> PublishOutcome:
        published = rescheduled = failed = 0
        for message, error in zip(messages, errors, strict=True):
            if error is None:
                message.status = OutboxStatus.PUBLISHED
                message.published_at = now
                message.last_error = None
                published += 1
                continue

            message.attempts += 1
            message.last_error = f"{type(error).__name__}: {error}"
            if message.attempts >= self._settings.max_attempts:
                message.status = OutboxStatus.FAILED
                message.failed_at = now
                failed += 1
                logger.error(
                    "outbox message %s gave up after %s attempts: %s",
                    message.event_id,
                    message.attempts,
                    message.last_error,
                )
            else:
                message.available_at = next_attempt_at(
                    now,
                    message.attempts,
                    base_seconds=self._settings.backoff_base_seconds,
                    cap_seconds=self._settings.backoff_cap_seconds,
                )
                rescheduled += 1
                logger.warning(
                    "outbox message %s will be retried: %s",
                    message.event_id,
                    message.last_error,
                )

        return PublishOutcome(published, rescheduled, failed)
