from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.events import build_order_created_message
from app.core.exceptions import (
    IdempotencyKeyConflictError,
    InsufficientStockError,
    OrderNotFoundError,
    ProductsNotFoundError,
    StockShortage,
)
from app.core.money import ZERO
from app.models import Order, OrderItem, Product
from app.repositories.order_repository import OrderRepository
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.product_repository import ProductRepository

IDEMPOTENCY_CONSTRAINT = "uq_orders_customer_id_idempotency_key"


@dataclass(frozen=True)
class RequestedItem:
    product_id: UUID
    quantity: int


def _violated_constraint(error: IntegrityError) -> str | None:
    cause: BaseException | None = error.orig
    while cause is not None:
        name = getattr(cause, "constraint_name", None)
        if isinstance(name, str):
            return name
        cause = cause.__cause__
    return None


class OrderService:
    def __init__(
        self,
        session: AsyncSession,
        orders: OrderRepository,
        products: ProductRepository,
        outbox: OutboxRepository,
    ) -> None:
        self._session = session
        self._orders = orders
        self._products = products
        self._outbox = outbox

    async def create_order(
        self,
        *,
        customer_id: UUID,
        idempotency_key: str,
        request_hash: str,
        items: Sequence[RequestedItem],
    ) -> tuple[Order, bool]:
        try:
            async with self._session.begin():
                replayed = await self._find_replay(customer_id, idempotency_key, request_hash)
                if replayed is not None:
                    return replayed, False
                order = await self._place_order(
                    customer_id=customer_id,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    items=items,
                )
        except IntegrityError as error:
            if _violated_constraint(error) != IDEMPOTENCY_CONSTRAINT:
                raise
            async with self._session.begin():
                replayed = await self._find_replay(customer_id, idempotency_key, request_hash)
            if replayed is None:
                raise
            return replayed, False

        return order, True

    async def get_order(self, order_id: UUID) -> Order:
        order = await self._orders.get(order_id)
        if order is None:
            raise OrderNotFoundError(order_id)
        return order

    async def list_orders(self) -> Sequence[Order]:
        return await self._orders.list_all()

    async def _find_replay(
        self, customer_id: UUID, idempotency_key: str, request_hash: str
    ) -> Order | None:
        existing = await self._orders.find_by_idempotency_key(customer_id, idempotency_key)
        if existing is None:
            return None
        if existing.request_hash != request_hash:
            raise IdempotencyKeyConflictError(idempotency_key)
        return existing

    async def _place_order(
        self,
        *,
        customer_id: UUID,
        idempotency_key: str,
        request_hash: str,
        items: Sequence[RequestedItem],
    ) -> Order:
        order = Order(
            customer_id=customer_id,
            total_amount=ZERO,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            items=[],
        )
        self._orders.add(order)
        await self._session.flush()

        products = await self._products.lock_by_ids([item.product_id for item in items])
        _reject_unknown_products(items, products)
        _reject_insufficient_stock(items, products)

        total = ZERO
        for item in items:
            product = products[item.product_id]
            product.stock -= item.quantity
            order.items.append(
                OrderItem(
                    product_id=product.id,
                    quantity=item.quantity,
                    unit_price=product.price,
                )
            )
            total += product.price * item.quantity

        order.total_amount = total
        await self._session.flush()

        self._outbox.add(build_order_created_message(order))
        return order


def _reject_unknown_products(items: Sequence[RequestedItem], products: dict[UUID, Product]) -> None:
    missing = [item.product_id for item in items if item.product_id not in products]
    if missing:
        raise ProductsNotFoundError(missing)


def _reject_insufficient_stock(
    items: Sequence[RequestedItem], products: dict[UUID, Product]
) -> None:
    shortages = [
        StockShortage(
            product_id=item.product_id,
            requested=item.quantity,
            available=products[item.product_id].stock,
        )
        for item in items
        if products[item.product_id].stock < item.quantity
    ]
    if shortages:
        raise InsufficientStockError(shortages)
