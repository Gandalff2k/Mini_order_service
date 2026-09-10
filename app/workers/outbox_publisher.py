from __future__ import annotations

import asyncio
import contextlib
import logging

from app.infra.db import create_database_engine, create_session_factory
from app.infra.kafka import create_producer
from app.infra.settings import get_settings
from app.services.outbox_publisher import OutboxPublisher
from app.workers.runner import run_worker

logger = logging.getLogger(__name__)

WORKER_NAME = "outbox-publisher"


async def wait_for(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def publish_outbox(stop: asyncio.Event) -> None:
    settings = get_settings()
    engine = create_database_engine(settings.database)
    session_factory = create_session_factory(engine)
    producer = create_producer(settings.kafka)
    publisher = OutboxPublisher(
        session_factory,
        producer,
        settings.outbox,
        settings.kafka.orders_topic,
    )

    try:
        await producer.start()
        logger.info(
            "%s publishing to %s via %s",
            WORKER_NAME,
            settings.kafka.orders_topic,
            settings.kafka.bootstrap_servers,
        )
        while not stop.is_set():
            try:
                outcome = await publisher.publish_pending()
            except Exception:
                logger.exception("%s cycle failed", WORKER_NAME)
                await wait_for(stop, settings.outbox.backoff_cap_seconds)
                continue

            if outcome.claimed == 0:
                await wait_for(stop, settings.outbox.poll_interval_seconds)
    finally:
        await producer.stop()
        await engine.dispose()


def main() -> None:
    run_worker(WORKER_NAME, publish_outbox)


if __name__ == "__main__":
    main()
