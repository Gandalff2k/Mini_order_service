from __future__ import annotations

from typing import Any
from uuid import UUID

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product


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
