from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from sqlalchemy import text

from app.infra.db import create_database_engine
from app.infra.settings import Settings, get_settings
from app.workers.runner import run_worker

logger = logging.getLogger(__name__)

WORKER_NAME = "notification-consumer"


def build_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka.orders_topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def consume_notifications(stop: asyncio.Event) -> None:
    settings = get_settings()
    engine = create_database_engine(settings.database)
    consumer = build_consumer(settings)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        await consumer.start()
        logger.info(
            "%s subscribed to %s as group %s",
            WORKER_NAME,
            settings.kafka.orders_topic,
            settings.kafka.consumer_group_id,
        )
        await stop.wait()
    finally:
        await consumer.stop()
        await engine.dispose()


def main() -> None:
    run_worker(WORKER_NAME, consume_notifications)


if __name__ == "__main__":
    main()
