from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Order, OutboxMessage, OutboxStatus, Product
from tests.integration.helpers import create_product, key, order_body, stock_of


class TestCreateOrder:
    async def test_creates_an_order_and_decrements_stock(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)
        customer_id = uuid4()

        response = await api_client.post(
            "/orders", json=order_body(customer_id, (product_id, 2)), headers=key("k1")
        )

        assert response.status_code == 201
        body = response.json()
        assert UUID(body["id"])
        assert body["customer_id"] == str(customer_id)
        assert body["total_amount"] == "39.98"
        assert body["items"] == [
            {
                "product_id": str(product_id),
                "quantity": 2,
                "unit_price": "19.99",
                "line_total": "39.98",
            }
        ]
        assert await stock_of(session, product_id) == 3

    async def test_totals_several_lines(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        first = await create_product(api_client, name="First", price="19.99", stock=10)
        second = await create_product(api_client, name="Second", price="0.03", stock=10)

        response = await api_client.post(
            "/orders",
            json=order_body(uuid4(), (first, 3), (second, 7)),
            headers=key("k1"),
        )

        assert response.status_code == 201
        body = response.json()
        assert body["total_amount"] == "60.18"
        assert sum(Decimal(item["line_total"]) for item in body["items"]) == Decimal("60.18")
        assert await stock_of(session, first) == 7
        assert await stock_of(session, second) == 3

    async def test_keeps_the_price_recorded_at_order_time(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)
        created = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("k1")
        )
        order_id = created.json()["id"]

        await session.execute(
            update(Product).where(Product.id == product_id).values(price=Decimal("99.99"))
        )
        await session.commit()

        response = await api_client.get(f"/orders/{order_id}")

        assert response.json()["items"][0]["unit_price"] == "19.99"
        assert response.json()["total_amount"] == "19.99"

    async def test_writes_the_outbox_message_with_the_order(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, price="19.99", stock=5)
        customer_id = uuid4()

        response = await api_client.post(
            "/orders", json=order_body(customer_id, (product_id, 2)), headers=key("k1")
        )
        order_id = UUID(response.json()["id"])

        message = (await session.execute(select(OutboxMessage))).scalars().one()
        assert message.aggregate_type == "order"
        assert message.aggregate_id == order_id
        assert message.event_type == "order.created"
        assert message.status is OutboxStatus.PENDING
        assert message.schema_version == 1
        assert message.payload["order_id"] == str(order_id)
        assert message.payload["customer_id"] == str(customer_id)
        assert message.payload["total_amount"] == "39.98"
        assert message.payload["items"] == [
            {"product_id": str(product_id), "quantity": 2, "unit_price": "19.99"}
        ]


class TestReadOrders:
    async def test_returns_a_single_order(self, api_client: AsyncClient) -> None:
        product_id = await create_product(api_client)
        created = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("k1")
        )

        response = await api_client.get(f"/orders/{created.json()['id']}")

        assert response.status_code == 200
        assert response.json() == created.json()

    async def test_unknown_order_is_reported_as_missing(self, api_client: AsyncClient) -> None:
        response = await api_client.get(f"/orders/{uuid4()}")

        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "order_not_found"

    async def test_lists_orders_in_creation_order(self, api_client: AsyncClient) -> None:
        product_id = await create_product(api_client, stock=10)
        first = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("k1")
        )
        second = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("k2")
        )

        response = await api_client.get("/orders")

        assert response.status_code == 200
        assert [order["id"] for order in response.json()] == [
            first.json()["id"],
            second.json()["id"],
        ]


class TestOrderFailures:
    async def test_unknown_product_leaves_nothing_behind(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        unknown = uuid4()

        response = await api_client.post(
            "/orders", json=order_body(uuid4(), (unknown, 1)), headers=key("k1")
        )

        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "products_not_found"
        assert response.json()["detail"]["product_ids"] == [str(unknown)]
        assert (await session.execute(select(Order))).scalars().all() == []
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []

    async def test_one_unknown_product_rolls_back_the_whole_order(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        known = await create_product(api_client, stock=5)

        response = await api_client.post(
            "/orders",
            json=order_body(uuid4(), (known, 2), (uuid4(), 1)),
            headers=key("k1"),
        )

        assert response.status_code == 404
        assert await stock_of(session, known) == 5
        assert (await session.execute(select(Order))).scalars().all() == []

    async def test_insufficient_stock_is_reported_with_the_shortage(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=2)

        response = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 3)), headers=key("k1")
        )

        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["code"] == "insufficient_stock"
        assert detail["shortages"] == [
            {"product_id": str(product_id), "requested": 3, "available": 2}
        ]
        assert await stock_of(session, product_id) == 2
        assert (await session.execute(select(Order))).scalars().all() == []
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []

    async def test_a_shortage_on_one_line_spares_the_other(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        plenty = await create_product(api_client, name="Plenty", stock=10)
        scarce = await create_product(api_client, name="Scarce", stock=1)

        response = await api_client.post(
            "/orders",
            json=order_body(uuid4(), (plenty, 2), (scarce, 5)),
            headers=key("k1"),
        )

        assert response.status_code == 409
        assert await stock_of(session, plenty) == 10
        assert await stock_of(session, scarce) == 1

    async def test_a_product_with_no_stock_cannot_be_ordered(self, api_client: AsyncClient) -> None:
        product_id = await create_product(api_client, stock=0)

        response = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("k1")
        )

        assert response.status_code == 409

    @pytest.mark.parametrize(
        "body",
        [
            {"customer_id": str(uuid4()), "items": []},
            {"customer_id": "not-a-uuid", "items": [{"product_id": str(uuid4()), "quantity": 1}]},
            {"customer_id": str(uuid4()), "items": [{"product_id": str(uuid4()), "quantity": 0}]},
            {"customer_id": str(uuid4()), "items": [{"product_id": str(uuid4()), "quantity": -1}]},
            {"customer_id": str(uuid4()), "items": [{"product_id": "nope", "quantity": 1}]},
            {"items": [{"product_id": str(uuid4()), "quantity": 1}]},
        ],
    )
    async def test_rejects_a_malformed_body(
        self, api_client: AsyncClient, body: dict[str, Any]
    ) -> None:
        response = await api_client.post("/orders", json=body, headers=key("k1"))

        assert response.status_code == 422

    async def test_rejects_the_same_product_twice_in_one_order(
        self, api_client: AsyncClient
    ) -> None:
        product_id = await create_product(api_client, stock=10)

        response = await api_client.post(
            "/orders",
            json=order_body(uuid4(), (product_id, 1), (product_id, 2)),
            headers=key("k1"),
        )

        assert response.status_code == 422

    async def test_requires_an_idempotency_key(self, api_client: AsyncClient) -> None:
        product_id = await create_product(api_client)

        response = await api_client.post("/orders", json=order_body(uuid4(), (product_id, 1)))

        assert response.status_code == 422


class TestIdempotency:
    async def test_repeating_a_request_returns_the_first_order(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=5)
        customer_id = uuid4()
        body = order_body(customer_id, (product_id, 2))

        first = await api_client.post("/orders", json=body, headers=key("checkout-1"))
        second = await api_client.post("/orders", json=body, headers=key("checkout-1"))

        assert first.status_code == 201
        assert second.status_code == 200
        assert second.json() == first.json()
        assert await stock_of(session, product_id) == 3
        assert len((await session.execute(select(Order))).scalars().all()) == 1
        assert len((await session.execute(select(OutboxMessage))).scalars().all()) == 1

    async def test_item_order_does_not_change_the_fingerprint(
        self, api_client: AsyncClient
    ) -> None:
        first_product = await create_product(api_client, name="First", stock=10)
        second_product = await create_product(api_client, name="Second", stock=10)
        customer_id = uuid4()

        first = await api_client.post(
            "/orders",
            json=order_body(customer_id, (first_product, 1), (second_product, 2)),
            headers=key("checkout-1"),
        )
        second = await api_client.post(
            "/orders",
            json=order_body(customer_id, (second_product, 2), (first_product, 1)),
            headers=key("checkout-1"),
        )

        assert first.status_code == 201
        assert second.status_code == 200
        assert second.json()["id"] == first.json()["id"]

    async def test_the_same_key_with_a_different_body_is_a_conflict(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=10)
        customer_id = uuid4()

        await api_client.post(
            "/orders",
            json=order_body(customer_id, (product_id, 1)),
            headers=key("checkout-1"),
        )
        response = await api_client.post(
            "/orders",
            json=order_body(customer_id, (product_id, 2)),
            headers=key("checkout-1"),
        )

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "idempotency_key_conflict"
        assert len((await session.execute(select(Order))).scalars().all()) == 1

    async def test_two_customers_may_reuse_the_same_key(self, api_client: AsyncClient) -> None:
        product_id = await create_product(api_client, stock=10)

        first = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("checkout-1")
        )
        second = await api_client.post(
            "/orders", json=order_body(uuid4(), (product_id, 1)), headers=key("checkout-1")
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["id"] != second.json()["id"]

    async def test_a_failed_request_does_not_burn_its_key(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=1)
        customer_id = uuid4()
        body = order_body(customer_id, (product_id, 3))

        rejected = await api_client.post("/orders", json=body, headers=key("checkout-1"))
        assert rejected.status_code == 409

        await session.execute(update(Product).where(Product.id == product_id).values(stock=5))
        await session.commit()

        retried = await api_client.post("/orders", json=body, headers=key("checkout-1"))

        assert retried.status_code == 201
        assert retried.json()["total_amount"] == "30.00"
