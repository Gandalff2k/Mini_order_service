from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product
from app.repositories.product_repository import ProductRepository


class ProductService:
    def __init__(self, session: AsyncSession, products: ProductRepository) -> None:
        self._session = session
        self._products = products

    async def create_product(self, *, name: str, price: Decimal, stock: int) -> Product:
        product = Product(name=name, price=price, stock=stock)
        async with self._session.begin():
            self._products.add(product)
        return product

    async def list_products(self) -> Sequence[Product]:
        return await self._products.list_all()
