from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer
from sqlalchemy import text

from app.infra.db import create_database_engine
from app.infra.settings import Settings, get_settings
from app.workers.runner import run_worker

logger = logging.getLogger(__name__)

WORKER_NAME = "outbox-publisher"


def build_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=settings.kafka.producer_linger_ms,
        request_timeout_ms=settings.kafka.producer_request_timeout_ms,
    )


async def publish_outbox(stop: asyncio.Event) -> None:
    settings = get_settings()
    engine = create_database_engine(settings.database)
    producer = build_producer(settings)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        await producer.start()
        logger.info(
            "%s connected to database and broker %s",
            WORKER_NAME,
            settings.kafka.bootstrap_servers,
        )
        await stop.wait()
    finally:
        await producer.stop()
        await engine.dispose()


def main() -> None:
    run_worker(WORKER_NAME, publish_outbox)


if __name__ == "__main__":
    main()
