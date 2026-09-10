from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.backoff import retry_delay
from app.core.events import ORDER_CREATED, parse_event, parse_order_created
from app.core.exceptions import MalformedEventError
from app.infra.kafka import MessageProducer
from app.infra.settings import ConsumerSettings
from app.repositories.notification_repository import NotificationRepository

logger = logging.getLogger(__name__)

TRANSIENT_ERRORS = (OperationalError, InterfaceError)


@dataclass(frozen=True)
class IncomingMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class HandleOutcome(StrEnum):
    STORED = "stored"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"
    DEAD_LETTERED = "dead_lettered"


class NotificationConsumer:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dead_letters: MessageProducer,
        settings: ConsumerSettings,
        dead_letter_topic: str,
    ) -> None:
        self._session_factory = session_factory
        self._dead_letters = dead_letters
        self._settings = settings
        self._dead_letter_topic = dead_letter_topic

    async def process(self, message: IncomingMessage, stop: asyncio.Event) -> bool:
        transient_attempt = 0
        unexpected_attempt = 0

        while not stop.is_set():
            try:
                outcome = await self.handle(message)
            except TRANSIENT_ERRORS as error:
                transient_attempt += 1
                logger.warning(
                    "database unavailable while handling offset %s, retry %s: %s",
                    message.offset,
                    transient_attempt,
                    error,
                )
                await self._pause(stop, transient_attempt)
                continue
            except Exception:
                unexpected_attempt += 1
                logger.exception(
                    "unexpected failure handling offset %s, attempt %s",
                    message.offset,
                    unexpected_attempt,
                )
                if unexpected_attempt >= self._settings.max_attempts:
                    await self.dead_letter(message, f"failed {unexpected_attempt} times")
                    return True
                await self._pause(stop, unexpected_attempt)
                continue

            logger.info("offset %s %s", message.offset, outcome.value)
            return True

        return False

    async def handle(self, message: IncomingMessage) -> HandleOutcome:
        try:
            event = parse_event(message.value)
        except MalformedEventError as error:
            await self.dead_letter(message, str(error))
            return HandleOutcome.DEAD_LETTERED

        if event.event_type != ORDER_CREATED:
            return HandleOutcome.SKIPPED

        try:
            order = parse_order_created(event)
        except MalformedEventError as error:
            await self.dead_letter(message, str(error))
            return HandleOutcome.DEAD_LETTERED

        async with self._session_factory() as session, session.begin():
            stored = await NotificationRepository(session).add_unless_seen(
                event_id=event.event_id,
                order_id=order.order_id,
                customer_id=order.customer_id,
                payload=event.data,
            )
        return HandleOutcome.STORED if stored else HandleOutcome.DUPLICATE

    async def dead_letter(self, message: IncomingMessage, reason: str) -> None:
        logger.error(
            "dead lettering %s[%s]@%s: %s",
            message.topic,
            message.partition,
            message.offset,
            reason,
        )
        await self._dead_letters.send_and_wait(
            topic=self._dead_letter_topic,
            value=message.value or b"",
            key=message.key or b"",
            headers=[
                ("x-original-topic", message.topic.encode()),
                ("x-original-partition", str(message.partition).encode()),
                ("x-original-offset", str(message.offset).encode()),
                ("x-failed-at", datetime.now(UTC).isoformat().encode()),
                ("x-error", reason.encode()),
            ],
        )

    async def _pause(self, stop: asyncio.Event, attempt: int) -> None:
        delay = retry_delay(
            attempt,
            base_seconds=self._settings.backoff_base_seconds,
            cap_seconds=self._settings.backoff_cap_seconds,
        )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)
