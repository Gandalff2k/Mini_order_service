from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product


class ProductRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, product: Product) -> None:
        self._session.add(product)

    async def list_all(self) -> Sequence[Product]:
        result = await self._session.execute(
            select(Product).order_by(Product.created_at, Product.id)
        )
        return result.scalars().all()
