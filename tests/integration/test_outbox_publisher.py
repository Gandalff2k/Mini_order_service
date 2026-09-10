from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.infra.db import create_database_engine, create_session_factory
from app.infra.settings import DatabaseSettings, OutboxSettings
from app.models import OutboxMessage, OutboxStatus
from app.services.outbox_publisher import OutboxPublisher
from tests.integration.fakes import FakeProducer
from tests.integration.helpers import make_outbox_message

TOPIC = "orders.events"
MAX_ATTEMPTS = 3


def settings(**overrides: object) -> OutboxSettings:
    defaults: dict[str, object] = {
        "batch_size": 10,
        "poll_interval_seconds": 0.01,
        "max_attempts": MAX_ATTEMPTS,
        "backoff_base_seconds": 1.0,
        "backoff_cap_seconds": 30.0,
        "publish_timeout_seconds": 5.0,
    }
    defaults.update(overrides)
    return OutboxSettings(**defaults)  # type: ignore[arg-type]


@pytest.fixture
def producer() -> FakeProducer:
    return FakeProducer()


def publisher_for(
    engine: AsyncEngine, producer: FakeProducer, outbox_settings: OutboxSettings | None = None
) -> OutboxPublisher:
    return OutboxPublisher(
        create_session_factory(engine),
        producer,
        outbox_settings or settings(),
        TOPIC,
    )


async def stored_messages(session: AsyncSession) -> list[OutboxMessage]:
    session.expire_all()
    result = await session.execute(select(OutboxMessage).order_by(OutboxMessage.id))
    return list(result.scalars().all())


async def warm_up(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


class TestPublishing:
    async def test_publishes_a_pending_message_and_marks_it(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        message = make_outbox_message()
        session.add(message)
        await session.commit()

        outcome = await publisher_for(engine, producer).publish_pending()

        assert outcome.published == 1
        assert len(producer.sent) == 1
        sent = producer.sent[0]
        assert sent.topic == TOPIC
        assert sent.key == str(message.aggregate_id).encode()
        assert sent.event_id == str(message.event_id)

        stored = (await stored_messages(session))[0]
        assert stored.status is OutboxStatus.PUBLISHED
        assert stored.published_at is not None
        assert stored.attempts == 0
        assert stored.last_error is None

    async def test_sends_the_envelope_assembled_from_columns(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        message = make_outbox_message()
        session.add(message)
        await session.commit()
        await session.refresh(message)

        await publisher_for(engine, producer).publish_pending()

        body = json.loads(producer.sent[0].value)
        assert body["event_id"] == str(message.event_id)
        assert body["event_type"] == "order.created"
        assert body["aggregate_type"] == "order"
        assert body["aggregate_id"] == str(message.aggregate_id)
        assert body["version"] == 1
        assert body["occurred_at"] == message.created_at.isoformat()
        assert body["data"] == message.payload
        assert dict(producer.sent[0].headers)["event_type"] == b"order.created"

    async def test_does_not_republish_a_published_message(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        session.add(make_outbox_message())
        await session.commit()
        publisher = publisher_for(engine, producer)

        await publisher.publish_pending()
        outcome = await publisher.publish_pending()

        assert outcome.claimed == 0
        assert len(producer.sent) == 1

    async def test_ignores_a_message_scheduled_for_later(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        session.add(make_outbox_message(available_at=datetime.now(UTC) + timedelta(minutes=5)))
        await session.commit()

        outcome = await publisher_for(engine, producer).publish_pending()

        assert outcome.claimed == 0
        assert producer.sent == []

    async def test_claims_no_more_than_the_batch_size(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        session.add_all([make_outbox_message() for _ in range(7)])
        await session.commit()

        outcome = await publisher_for(engine, producer, settings(batch_size=3)).publish_pending()

        assert outcome.published == 3
        assert len(producer.sent) == 3


class TestFailureHandling:
    async def test_a_failed_send_is_rescheduled_with_backoff(
        self, engine: AsyncEngine, session: AsyncSession
    ) -> None:
        message = make_outbox_message()
        session.add(message)
        await session.commit()
        before = datetime.now(UTC)

        outcome = await publisher_for(engine, FakeProducer(fail_all=True)).publish_pending()

        assert outcome.rescheduled == 1
        stored = (await stored_messages(session))[0]
        assert stored.status is OutboxStatus.PENDING
        assert stored.published_at is None
        assert stored.attempts == 1
        assert stored.last_error is not None
        assert "broker unavailable" in stored.last_error
        assert stored.available_at > before

    async def test_a_message_recovers_once_the_broker_returns(
        self, engine: AsyncEngine, session: AsyncSession
    ) -> None:
        session.add(make_outbox_message(available_at=datetime.now(UTC)))
        await session.commit()

        await publisher_for(engine, FakeProducer(fail_all=True)).publish_pending()
        await session.execute(update(OutboxMessage).values(available_at=datetime.now(UTC)))
        await session.commit()

        healthy = FakeProducer()
        outcome = await publisher_for(engine, healthy).publish_pending()

        assert outcome.published == 1
        assert len(healthy.sent) == 1
        stored = (await stored_messages(session))[0]
        assert stored.status is OutboxStatus.PUBLISHED
        assert stored.attempts == 1

    async def test_gives_up_after_the_attempt_limit(
        self, engine: AsyncEngine, session: AsyncSession
    ) -> None:
        session.add(make_outbox_message())
        await session.commit()
        publisher = publisher_for(engine, FakeProducer(fail_all=True))

        for _ in range(MAX_ATTEMPTS):
            await session.execute(update(OutboxMessage).values(available_at=datetime.now(UTC)))
            await session.commit()
            await publisher.publish_pending()

        stored = (await stored_messages(session))[0]
        assert stored.status is OutboxStatus.FAILED
        assert stored.failed_at is not None
        assert stored.attempts == MAX_ATTEMPTS

    async def test_a_failed_message_is_never_claimed_again(
        self, engine: AsyncEngine, session: AsyncSession, producer: FakeProducer
    ) -> None:
        message = make_outbox_message()
        session.add(message)
        await session.commit()
        await session.execute(
            update(OutboxMessage).values(status=OutboxStatus.FAILED, failed_at=datetime.now(UTC))
        )
        await session.commit()

        outcome = await publisher_for(engine, producer).publish_pending()

        assert outcome.claimed == 0
        assert producer.sent == []

    async def test_one_bad_message_does_not_hold_back_the_batch(
        self, engine: AsyncEngine, session: AsyncSession
    ) -> None:
        messages = [make_outbox_message() for _ in range(3)]
        session.add_all(messages)
        await session.commit()
        poisoned = str(messages[1].event_id)

        outcome = await publisher_for(
            engine, FakeProducer(fail_event_ids=frozenset({poisoned}))
        ).publish_pending()

        assert outcome.published == 2
        assert outcome.rescheduled == 1
        stored = {str(message.event_id): message for message in await stored_messages(session)}
        assert stored[poisoned].status is OutboxStatus.PENDING
        assert [message.status for event_id, message in stored.items() if event_id != poisoned] == [
            OutboxStatus.PUBLISHED,
            OutboxStatus.PUBLISHED,
        ]

    async def test_a_hanging_broker_reschedules_the_batch(
        self, engine: AsyncEngine, session: AsyncSession
    ) -> None:
        session.add_all([make_outbox_message() for _ in range(2)])
        await session.commit()
        slow = FakeProducer(delay_seconds=2.0)

        outcome = await publisher_for(
            engine, slow, settings(publish_timeout_seconds=0.1)
        ).publish_pending()

        assert outcome.rescheduled == 2
        assert all(
            message.status is OutboxStatus.PENDING for message in await stored_messages(session)
        )


class TestConcurrentPublishers:
    async def test_two_publishers_send_each_message_exactly_once(
        self, database_settings: DatabaseSettings, session: AsyncSession
    ) -> None:
        session.add_all([make_outbox_message() for _ in range(20)])
        await session.commit()

        first_engine = create_database_engine(database_settings)
        second_engine = create_database_engine(database_settings)
        first_producer = FakeProducer(delay_seconds=0.3)
        second_producer = FakeProducer(delay_seconds=0.3)
        try:
            await asyncio.gather(warm_up(first_engine), warm_up(second_engine))
            started = time.perf_counter()
            await asyncio.gather(
                publisher_for(
                    first_engine, first_producer, settings(batch_size=10)
                ).publish_pending(),
                publisher_for(
                    second_engine, second_producer, settings(batch_size=10)
                ).publish_pending(),
            )
            elapsed = time.perf_counter() - started
        finally:
            await first_engine.dispose()
            await second_engine.dispose()

        delivered = [message.event_id for message in first_producer.sent + second_producer.sent]
        assert len(delivered) == 20
        assert len(set(delivered)) == 20
        assert len(first_producer.sent) == 10
        assert len(second_producer.sent) == 10
        assert elapsed < 0.5
        stored = await stored_messages(session)
        assert all(message.status is OutboxStatus.PUBLISHED for message in stored)
