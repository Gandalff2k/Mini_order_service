from __future__ import annotations

import asyncio
import json
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.infra.db import create_session_factory
from app.infra.settings import ConsumerSettings
from app.models import Notification
from app.services.notification_consumer import (
    HandleOutcome,
    IncomingMessage,
    NotificationConsumer,
)
from tests.integration.fakes import FakeProducer

DLQ_TOPIC = "orders.events.dlq"
SOURCE_TOPIC = "orders.events"


def consumer_settings(**overrides: object) -> ConsumerSettings:
    defaults: dict[str, object] = {
        "max_attempts": 3,
        "backoff_base_seconds": 0.001,
        "backoff_cap_seconds": 0.01,
    }
    defaults.update(overrides)
    return ConsumerSettings(**defaults)  # type: ignore[arg-type]


def envelope(
    *,
    event_id: UUID,
    order_id: UUID,
    customer_id: UUID,
    event_type: str = "order.created",
    version: int = 1,
    data: dict[str, Any] | None = None,
) -> bytes:
    body = {
        "event_id": str(event_id),
        "event_type": event_type,
        "aggregate_type": "order",
        "aggregate_id": str(order_id),
        "occurred_at": "2026-01-01T00:00:00+00:00",
        "version": version,
        "data": data
        if data is not None
        else {
            "order_id": str(order_id),
            "customer_id": str(customer_id),
            "total_amount": "39.98",
        },
    }
    return json.dumps(body).encode()


def message(value: bytes | None, *, offset: int = 0, key: bytes | None = None) -> IncomingMessage:
    return IncomingMessage(
        topic=SOURCE_TOPIC,
        partition=0,
        offset=offset,
        key=key,
        value=value,
    )


def order_created(
    *, event_id: UUID | None = None, order_id: UUID | None = None, offset: int = 0
) -> tuple[IncomingMessage, UUID, UUID, UUID]:
    resolved_event = event_id or uuid4()
    resolved_order = order_id or uuid4()
    customer_id = uuid4()
    value = envelope(event_id=resolved_event, order_id=resolved_order, customer_id=customer_id)
    return message(value, offset=offset), resolved_event, resolved_order, customer_id


@pytest.fixture
def dead_letters() -> FakeProducer:
    return FakeProducer()


def consumer_for(
    engine: AsyncEngine,
    dead_letters: FakeProducer,
    settings: ConsumerSettings | None = None,
) -> NotificationConsumer:
    return NotificationConsumer(
        create_session_factory(engine),
        dead_letters,
        settings or consumer_settings(),
        DLQ_TOPIC,
    )


async def notifications(session: AsyncSession) -> list[Notification]:
    session.expire_all()
    result = await session.execute(select(Notification).order_by(Notification.created_at))
    return list(result.scalars().all())


class TestHandlingOrderCreated:
    async def test_writes_a_notification(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        incoming, event_id, order_id, customer_id = order_created()

        outcome = await consumer_for(engine, dead_letters).handle(incoming)

        assert outcome is HandleOutcome.STORED
        stored = await notifications(session)
        assert len(stored) == 1
        assert stored[0].event_id == event_id
        assert stored[0].order_id == order_id
        assert stored[0].customer_id == customer_id
        assert stored[0].payload["total_amount"] == "39.98"
        assert dead_letters.sent == []

    async def test_redelivery_of_the_same_event_stores_one_notification(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        incoming, _, _, _ = order_created()
        consumer = consumer_for(engine, dead_letters)

        first = await consumer.handle(incoming)
        second = await consumer.handle(incoming)

        assert first is HandleOutcome.STORED
        assert second is HandleOutcome.DUPLICATE
        assert len(await notifications(session)) == 1

    async def test_the_same_order_delivered_under_two_event_ids_is_not_deduplicated(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        order_id = uuid4()
        first, _, _, _ = order_created(order_id=order_id)
        second, _, _, _ = order_created(order_id=order_id, offset=1)
        consumer = consumer_for(engine, dead_letters)

        await consumer.handle(first)
        await consumer.handle(second)

        assert len(await notifications(session)) == 2

    async def test_ignores_an_event_type_it_does_not_handle(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        value = envelope(
            event_id=uuid4(),
            order_id=uuid4(),
            customer_id=uuid4(),
            event_type="order.cancelled",
        )

        outcome = await consumer_for(engine, dead_letters).handle(message(value))

        assert outcome is HandleOutcome.SKIPPED
        assert await notifications(session) == []
        assert dead_letters.sent == []


class TestDeadLettering:
    @pytest.mark.parametrize(
        "value",
        [
            None,
            b"",
            b"not json at all",
            b"[]",
            json.dumps({"event_type": "order.created"}).encode(),
        ],
    )
    async def test_an_unreadable_message_goes_to_the_dead_letter_topic(
        self,
        engine: AsyncEngine,
        session: AsyncSession,
        dead_letters: FakeProducer,
        value: bytes | None,
    ) -> None:
        outcome = await consumer_for(engine, dead_letters).handle(message(value, offset=17))

        assert outcome is HandleOutcome.DEAD_LETTERED
        assert await notifications(session) == []
        assert len(dead_letters.sent) == 1
        sent = dead_letters.sent[0]
        assert sent.topic == DLQ_TOPIC
        headers = dict(sent.headers)
        assert headers["x-original-topic"] == SOURCE_TOPIC.encode()
        assert headers["x-original-offset"] == b"17"
        assert headers["x-error"]

    async def test_an_order_created_event_without_a_customer_is_dead_lettered(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        order_id = uuid4()
        value = envelope(
            event_id=uuid4(),
            order_id=order_id,
            customer_id=uuid4(),
            data={"order_id": str(order_id)},
        )

        outcome = await consumer_for(engine, dead_letters).handle(message(value))

        assert outcome is HandleOutcome.DEAD_LETTERED
        assert await notifications(session) == []
        assert b"customer_id" in dict(dead_letters.sent[0].headers)["x-error"]

    async def test_an_unsupported_schema_version_is_dead_lettered(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        value = envelope(event_id=uuid4(), order_id=uuid4(), customer_id=uuid4(), version=99)

        outcome = await consumer_for(engine, dead_letters).handle(message(value))

        assert outcome is HandleOutcome.DEAD_LETTERED
        assert await notifications(session) == []
        assert b"99" in dict(dead_letters.sent[0].headers)["x-error"]


class FlakySessionFactory:
    def __init__(
        self,
        delegate: async_sessionmaker[AsyncSession],
        failures: int,
        error: Exception,
        on_failure: asyncio.Event | None = None,
    ) -> None:
        self._delegate = delegate
        self._failures = failures
        self._error = error
        self._on_failure = on_failure
        self.calls = 0

    def __call__(self) -> AsyncSession:
        self.calls += 1
        if self.calls <= self._failures:
            if self._on_failure is not None:
                self._on_failure.set()
            raise self._error
        return self._delegate()


def transient_error() -> OperationalError:
    return OperationalError("SELECT 1", None, Exception("connection refused"))


class TestRetryPolicy:
    async def test_a_database_outage_is_retried_until_it_clears(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        flaky = FlakySessionFactory(create_session_factory(engine), 4, transient_error())
        consumer = NotificationConsumer(
            cast("async_sessionmaker[AsyncSession]", flaky),
            dead_letters,
            consumer_settings(max_attempts=2),
            DLQ_TOPIC,
        )
        incoming, _, _, _ = order_created()

        committed = await consumer.process(incoming, asyncio.Event())

        assert committed is True
        assert flaky.calls == 5
        assert len(await notifications(session)) == 1
        assert dead_letters.sent == []

    async def test_an_unexpected_failure_is_dead_lettered_after_the_attempt_limit(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        flaky = FlakySessionFactory(
            create_session_factory(engine), 100, RuntimeError("bug in the handler")
        )
        consumer = NotificationConsumer(
            cast("async_sessionmaker[AsyncSession]", flaky),
            dead_letters,
            consumer_settings(max_attempts=3),
            DLQ_TOPIC,
        )
        incoming, _, _, _ = order_created()

        committed = await consumer.process(incoming, asyncio.Event())

        assert committed is True
        assert flaky.calls == 3
        assert await notifications(session) == []
        assert len(dead_letters.sent) == 1
        assert b"failed 3 times" in dict(dead_letters.sent[0].headers)["x-error"]

    async def test_shutting_down_mid_retry_leaves_the_offset_uncommitted(
        self, engine: AsyncEngine, session: AsyncSession, dead_letters: FakeProducer
    ) -> None:
        stop = asyncio.Event()
        flaky = FlakySessionFactory(
            create_session_factory(engine), 100, transient_error(), on_failure=stop
        )
        consumer = NotificationConsumer(
            cast("async_sessionmaker[AsyncSession]", flaky),
            dead_letters,
            consumer_settings(),
            DLQ_TOPIC,
        )
        incoming, _, _, _ = order_created()

        committed = await consumer.process(incoming, stop)

        assert committed is False
        assert await notifications(session) == []
        assert dead_letters.sent == []
