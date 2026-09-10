from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Order


class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, order: Order) -> None:
        self._session.add(order)

    async def get(self, order_id: UUID) -> Order | None:
        return await self._session.get(Order, order_id)

    async def list_all(self) -> Sequence[Order]:
        result = await self._session.execute(select(Order).order_by(Order.created_at, Order.id))
        return result.scalars().all()

    async def find_by_idempotency_key(
        self, customer_id: UUID, idempotency_key: str
    ) -> Order | None:
        result = await self._session.execute(
            select(Order).where(
                Order.customer_id == customer_id,
                Order.idempotency_key == idempotency_key,
            )
        )
        return result.scalars().one_or_none()
