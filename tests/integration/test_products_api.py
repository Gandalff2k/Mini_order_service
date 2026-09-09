from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product


def payload(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"name": "Mechanical keyboard", "price": "129.99", "stock": 7}
    body.update(overrides)
    return body


class TestCreateProduct:
    async def test_returns_the_created_product(self, api_client: AsyncClient) -> None:
        response = await api_client.post("/products", json=payload())

        assert response.status_code == 201
        body = response.json()
        assert UUID(body["id"])
        assert body["name"] == "Mechanical keyboard"
        assert body["price"] == "129.99"
        assert body["stock"] == 7
        assert body["created_at"]

    async def test_persists_the_product(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        response = await api_client.post("/products", json=payload(name="Trackball", price="45.50"))

        stored = (await session.execute(select(Product))).scalars().one()
        assert stored.id == UUID(response.json()["id"])
        assert stored.name == "Trackball"
        assert stored.price == Decimal("45.50")
        assert stored.stock == 7

    async def test_normalises_a_price_given_with_one_decimal(self, api_client: AsyncClient) -> None:
        response = await api_client.post("/products", json=payload(price="45.5"))

        assert response.status_code == 201
        assert response.json()["price"] == "45.50"

    async def test_trims_surrounding_whitespace_from_the_name(
        self, api_client: AsyncClient
    ) -> None:
        response = await api_client.post("/products", json=payload(name="  Trackball  "))

        assert response.status_code == 201
        assert response.json()["name"] == "Trackball"

    async def test_accepts_a_product_with_no_stock(self, api_client: AsyncClient) -> None:
        response = await api_client.post("/products", json=payload(stock=0))

        assert response.status_code == 201
        assert response.json()["stock"] == 0

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("price", "-0.01"),
            ("price", "12.345"),
            ("price", "12345678901.00"),
            ("price", "not a number"),
            ("stock", -1),
            ("stock", "many"),
            ("name", ""),
            ("name", "   "),
            ("name", "x" * 256),
        ],
    )
    async def test_rejects_invalid_input(
        self, api_client: AsyncClient, session: AsyncSession, field: str, value: Any
    ) -> None:
        response = await api_client.post("/products", json=payload(**{field: value}))

        assert response.status_code == 422
        assert (await session.execute(select(Product))).scalars().all() == []

    @pytest.mark.parametrize("missing", ["name", "price", "stock"])
    async def test_rejects_a_missing_field(self, api_client: AsyncClient, missing: str) -> None:
        body = payload()
        del body[missing]

        response = await api_client.post("/products", json=body)

        assert response.status_code == 422


class TestListProducts:
    async def test_returns_an_empty_list_when_nothing_exists(self, api_client: AsyncClient) -> None:
        response = await api_client.get("/products")

        assert response.status_code == 200
        assert response.json() == []

    async def test_returns_every_created_product(self, api_client: AsyncClient) -> None:
        await api_client.post("/products", json=payload(name="First", price="1.00"))
        await api_client.post("/products", json=payload(name="Second", price="2.00"))

        response = await api_client.get("/products")

        assert response.status_code == 200
        listed = response.json()
        assert [item["name"] for item in listed] == ["First", "Second"]
        assert [item["price"] for item in listed] == ["1.00", "2.00"]
