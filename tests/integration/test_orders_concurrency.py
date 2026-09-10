from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from uuid import uuid4

from httpx import AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Order, OutboxMessage
from tests.integration.helpers import create_product, key, order_body, stock_of

ATTEMPTS = 10


async def count_of(session: AsyncSession, model: type[Order] | type[OutboxMessage]) -> int:
    return await session.scalar(select(func.count()).select_from(model)) or 0


class TestConcurrentStockDecrement:
    async def test_only_one_order_takes_the_last_unit(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=1)

        responses: list[Response] = await asyncio.gather(
            *(
                api_client.post(
                    "/orders",
                    json=order_body(uuid4(), (product_id, 1)),
                    headers=key(f"attempt-{attempt}"),
                )
                for attempt in range(ATTEMPTS)
            )
        )

        statuses = [response.status_code for response in responses]
        assert statuses.count(201) == 1
        assert statuses.count(409) == ATTEMPTS - 1
        assert await stock_of(session, product_id) == 0
        assert await count_of(session, Order) == 1
        assert await count_of(session, OutboxMessage) == 1

    async def test_sells_exactly_the_available_quantity(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        available = 4
        product_id = await create_product(api_client, stock=available)

        responses: list[Response] = await asyncio.gather(
            *(
                api_client.post(
                    "/orders",
                    json=order_body(uuid4(), (product_id, 1)),
                    headers=key(f"attempt-{attempt}"),
                )
                for attempt in range(ATTEMPTS)
            )
        )

        statuses = [response.status_code for response in responses]
        assert statuses.count(201) == available
        assert statuses.count(409) == ATTEMPTS - available
        assert await stock_of(session, product_id) == 0
        assert await count_of(session, Order) == available
        assert await count_of(session, OutboxMessage) == available

    async def test_locking_products_in_a_fixed_order_avoids_deadlocks(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        first = await create_product(api_client, name="First", stock=ATTEMPTS)
        second = await create_product(api_client, name="Second", stock=ATTEMPTS)

        def request(attempt: int) -> Awaitable[Response]:
            items = ((first, 1), (second, 1)) if attempt % 2 == 0 else ((second, 1), (first, 1))
            return api_client.post(
                "/orders",
                json=order_body(uuid4(), *items),
                headers=key(f"attempt-{attempt}"),
            )

        responses: list[Response] = await asyncio.gather(
            *(request(attempt) for attempt in range(ATTEMPTS))
        )

        assert [response.status_code for response in responses] == [201] * ATTEMPTS
        assert await stock_of(session, first) == 0
        assert await stock_of(session, second) == 0


class TestConcurrentIdempotency:
    async def test_concurrent_duplicates_create_a_single_order(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=ATTEMPTS)
        customer_id = uuid4()
        body = order_body(customer_id, (product_id, 1))

        responses: list[Response] = await asyncio.gather(
            *(
                api_client.post("/orders", json=body, headers=key("checkout-1"))
                for _ in range(ATTEMPTS)
            )
        )

        statuses = [response.status_code for response in responses]
        assert statuses.count(201) == 1
        assert statuses.count(200) == ATTEMPTS - 1
        assert len({response.json()["id"] for response in responses}) == 1
        assert await stock_of(session, product_id) == ATTEMPTS - 1
        assert await count_of(session, Order) == 1
        assert await count_of(session, OutboxMessage) == 1

    async def test_a_concurrent_duplicate_still_replays_when_stock_runs_out(
        self, api_client: AsyncClient, session: AsyncSession
    ) -> None:
        product_id = await create_product(api_client, stock=1)
        customer_id = uuid4()
        body = order_body(customer_id, (product_id, 1))

        responses: list[Response] = await asyncio.gather(
            *(
                api_client.post("/orders", json=body, headers=key("checkout-1"))
                for _ in range(ATTEMPTS)
            )
        )

        statuses = [response.status_code for response in responses]
        assert statuses.count(201) == 1
        assert statuses.count(200) == ATTEMPTS - 1
        assert 409 not in statuses
        assert await count_of(session, Order) == 1
