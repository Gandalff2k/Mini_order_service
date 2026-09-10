from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.infra.settings import KafkaSettings


class MessageProducer(Protocol):
    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: Sequence[tuple[str, bytes]],
    ) -> Any: ...


def create_producer(settings: KafkaSettings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=settings.producer_linger_ms,
        request_timeout_ms=settings.producer_request_timeout_ms,
    )


def create_consumer(settings: KafkaSettings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.orders_topic,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
