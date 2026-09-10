from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.events import build_order_created_message
from app.models import Order, OrderItem, OutboxMessage
from app.repositories.outbox_repository import OutboxRepository
from app.services import order_service
from tests.integration.helpers import create_product, key, make_outbox_message, order_body, stock_of


async def stored(session: AsyncSession, model: Any) -> list[Any]:
    session.expire_all()
    return list((await session.execute(select(model))).scalars().all())


class TestTransactionRollback:
    async def test_a_failure_writing_the_event_undoes_the_order(
        self, api_client: AsyncClient, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)

        def explode(self: OutboxRepository, message: OutboxMessage) -> None:
            raise RuntimeError("outbox is unavailable")

        monkeypatch.setattr(OutboxRepository, "add", explode)

        with pytest.raises(RuntimeError, match="outbox is unavailable"):
            await api_client.post(
                "/orders",
                json=order_body(uuid4(), (product_id, 2)),
                headers=key("checkout-1"),
            )

        assert await stock_of(session, product_id) == 5
        assert await stored(session, Order) == []
        assert await stored(session, OrderItem) == []
        assert await stored(session, OutboxMessage) == []

    async def test_a_database_error_on_the_event_undoes_the_order(
        self, api_client: AsyncClient, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)
        existing = make_outbox_message()
        session.add(existing)
        await session.commit()
        taken_event_id = existing.event_id

        def build_with_a_taken_event_id(order: Order) -> OutboxMessage:
            message = build_order_created_message(order)
            message.event_id = taken_event_id
            return message

        monkeypatch.setattr(
            order_service, "build_order_created_message", build_with_a_taken_event_id
        )

        with pytest.raises(IntegrityError, match="uq_outbox_messages_event_id"):
            await api_client.post(
                "/orders",
                json=order_body(uuid4(), (product_id, 2)),
                headers=key("checkout-1"),
            )

        assert await stock_of(session, product_id) == 5
        assert await stored(session, Order) == []
        assert await stored(session, OrderItem) == []
        remaining = await stored(session, OutboxMessage)
        assert [message.event_id for message in remaining] == [taken_event_id]

    async def test_a_rolled_back_order_leaves_its_idempotency_key_usable(
        self, api_client: AsyncClient, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)
        customer_id = uuid4()
        body = order_body(customer_id, (product_id, 2))

        def explode(self: OutboxRepository, message: OutboxMessage) -> None:
            raise RuntimeError("outbox is unavailable")

        monkeypatch.setattr(OutboxRepository, "add", explode)
        with pytest.raises(RuntimeError):
            await api_client.post("/orders", json=body, headers=key("checkout-1"))

        monkeypatch.undo()
        retried = await api_client.post("/orders", json=body, headers=key("checkout-1"))

        assert retried.status_code == 201
        assert retried.json()["total_amount"] == "39.98"
        assert await stock_of(session, product_id) == 3

    async def test_no_order_is_ever_stored_without_its_event(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=3)

        for attempt in range(5):
            await api_client.post(
                "/orders",
                json=order_body(uuid4(), (product_id, 1)),
                headers=key(f"checkout-{attempt}"),
            )

        order_ids = {order.id for order in await stored(session, Order)}
        aggregate_ids = {message.aggregate_id for message in await stored(session, OutboxMessage)}

        assert len(order_ids) == 3
        assert order_ids == aggregate_ids
        assert await stock_of(session, product_id) == 0
