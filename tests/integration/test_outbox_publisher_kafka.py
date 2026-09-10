from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.infra.db import create_session_factory
from app.infra.kafka import create_producer
from app.infra.settings import KafkaSettings, OutboxSettings
from app.models import OutboxMessage, OutboxStatus
from app.services.outbox_publisher import OutboxPublisher
from tests.integration.helpers import make_outbox_message

TOPIC = "orders.events.test"
RECEIVE_TIMEOUT = 20.0


@pytest.fixture
async def consumer(kafka_bootstrap: str) -> AsyncIterator[AIOKafkaConsumer]:
    client = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


@pytest.mark.kafka
async def test_the_publisher_delivers_the_event_to_kafka(
    engine: AsyncEngine,
    session: AsyncSession,
    kafka_bootstrap: str,
    consumer: AIOKafkaConsumer,
) -> None:
    message = make_outbox_message()
    session.add(message)
    await session.commit()
    await session.refresh(message)

    producer = create_producer(KafkaSettings(bootstrap_servers=kafka_bootstrap))
    await producer.start()
    try:
        publisher = OutboxPublisher(
            create_session_factory(engine),
            producer,
            OutboxSettings(),
            TOPIC,
        )
        outcome = await publisher.publish_pending()
    finally:
        await producer.stop()

    assert outcome.published == 1

    received = await asyncio.wait_for(consumer.getone(), timeout=RECEIVE_TIMEOUT)
    assert received.key == str(message.aggregate_id).encode()
    assert dict(received.headers)["event_id"] == str(message.event_id).encode()
    body = json.loads(received.value)
    assert body["event_id"] == str(message.event_id)
    assert body["event_type"] == "order.created"
    assert body["data"] == message.payload

    session.expire_all()
    stored = (await session.execute(select(OutboxMessage))).scalars().one()
    assert stored.status is OutboxStatus.PUBLISHED
