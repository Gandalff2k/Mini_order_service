from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from app.infra.db import create_database_engine, create_session_factory
from app.infra.kafka import create_consumer, create_producer
from app.infra.settings import get_settings
from app.services.notification_consumer import IncomingMessage, NotificationConsumer
from app.workers.runner import run_worker

logger = logging.getLogger(__name__)

WORKER_NAME = "notification-consumer"
POLL_TIMEOUT_SECONDS = 1.0


def to_incoming(record: Any) -> IncomingMessage:
    return IncomingMessage(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        value=record.value,
    )


async def next_record(consumer: Any) -> Any | None:
    try:
        return await asyncio.wait_for(consumer.getone(), timeout=POLL_TIMEOUT_SECONDS)
    except TimeoutError:
        return None


async def consume_notifications(stop: asyncio.Event) -> None:
    settings = get_settings()
    engine = create_database_engine(settings.database)
    session_factory = create_session_factory(engine)
    consumer = create_consumer(settings.kafka)
    dead_letters = create_producer(settings.kafka)
    handler = NotificationConsumer(
        session_factory,
        dead_letters,
        settings.consumer,
        settings.kafka.dlq_topic,
    )

    await dead_letters.start()
    await consumer.start()
    logger.info(
        "%s reading %s as group %s",
        WORKER_NAME,
        settings.kafka.orders_topic,
        settings.kafka.consumer_group_id,
    )
    try:
        while not stop.is_set():
            record = await next_record(consumer)
            if record is None:
                continue
            if await handler.process(to_incoming(record), stop):
                await consumer.commit()
    finally:
        with contextlib.suppress(Exception):
            await consumer.stop()
        await dead_letters.stop()
        await engine.dispose()


def main() -> None:
    run_worker(WORKER_NAME, consume_notifications)


if __name__ == "__main__":
    main()
