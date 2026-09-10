from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.infra.db import create_session_factory
from app.infra.kafka import create_producer
from app.infra.settings import ConsumerSettings, KafkaSettings, OutboxSettings
from app.models import Notification, OutboxMessage, OutboxStatus
from app.services.notification_consumer import (
    HandleOutcome,
    IncomingMessage,
    NotificationConsumer,
)
from app.services.outbox_publisher import OutboxPublisher
from tests.integration.fakes import FakeProducer
from tests.integration.helpers import create_product, key, order_body

TOPIC = "orders.events.e2e"
DLQ_TOPIC = "orders.events.e2e.dlq"
RECEIVE_TIMEOUT = 20.0

pytestmark = pytest.mark.kafka


@pytest.fixture
async def broker_consumer(kafka_bootstrap: str) -> AsyncIterator[AIOKafkaConsumer]:
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


async def test_an_order_travels_from_http_to_a_notification(
    api_client: AsyncClient,
    engine: AsyncEngine,
    session: AsyncSession,
    kafka_bootstrap: str,
    broker_consumer: AIOKafkaConsumer,
) -> None:
    product_id = await create_product(api_client, price="19.99", stock=5)
    customer_id = uuid4()

    placed = await api_client.post(
        "/orders",
        json=order_body(customer_id, (product_id, 2)),
        headers=key("checkout-1"),
    )
    assert placed.status_code == 201
    order_id = UUID(placed.json()["id"])

    producer = create_producer(KafkaSettings(bootstrap_servers=kafka_bootstrap))
    await producer.start()
    try:
        outcome = await OutboxPublisher(
            create_session_factory(engine), producer, OutboxSettings(), TOPIC
        ).publish_pending()
    finally:
        await producer.stop()
    assert outcome.published == 1

    record = await asyncio.wait_for(broker_consumer.getone(), timeout=RECEIVE_TIMEOUT)
    assert record.key == str(order_id).encode()

    delivered = IncomingMessage(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        value=record.value,
    )
    dead_letters = FakeProducer()
    consumer = NotificationConsumer(
        create_session_factory(engine), dead_letters, ConsumerSettings(), DLQ_TOPIC
    )

    first = await consumer.handle(delivered)
    redelivered = await consumer.handle(delivered)

    assert first is HandleOutcome.STORED
    assert redelivered is HandleOutcome.DUPLICATE
    assert dead_letters.sent == []

    session.expire_all()
    notifications = (await session.execute(select(Notification))).scalars().all()
    assert len(notifications) == 1
    assert notifications[0].order_id == order_id
    assert notifications[0].customer_id == customer_id
    assert notifications[0].payload["total_amount"] == "39.98"

    published = (await session.execute(select(OutboxMessage))).scalars().one()
    assert published.status is OutboxStatus.PUBLISHED
    assert published.event_id == notifications[0].event_id
