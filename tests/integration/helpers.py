from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, Product


async def create_product(
    client: AsyncClient,
    *,
    name: str = "Mechanical keyboard",
    price: str = "10.00",
    stock: int = 5,
) -> UUID:
    response = await client.post("/products", json={"name": name, "price": price, "stock": stock})
    assert response.status_code == 201
    return UUID(response.json()["id"])


def order_body(customer_id: UUID, *items: tuple[UUID, int]) -> dict[str, Any]:
    return {
        "customer_id": str(customer_id),
        "items": [
            {"product_id": str(product_id), "quantity": quantity} for product_id, quantity in items
        ],
    }


def key(value: str) -> dict[str, str]:
    return {"Idempotency-Key": value}


async def stock_of(session: AsyncSession, product_id: UUID) -> int:
    product = await session.get(Product, product_id)
    assert product is not None
    await session.refresh(product)
    return product.stock


def make_outbox_message(
    *,
    event_id: UUID | None = None,
    aggregate_id: UUID | None = None,
    available_at: datetime | None = None,
) -> OutboxMessage:
    order_id = aggregate_id or uuid4()
    message = OutboxMessage(
        event_id=event_id or uuid4(),
        aggregate_type="order",
        aggregate_id=order_id,
        event_type="order.created",
        payload={"order_id": str(order_id), "total_amount": "10.00"},
    )
    if available_at is not None:
        message.available_at = available_at
    return message
