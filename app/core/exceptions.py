from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


class DomainError(Exception):
    code: str


@dataclass(frozen=True)
class StockShortage:
    product_id: UUID
    requested: int
    available: int


class ProductsNotFoundError(DomainError):
    code = "products_not_found"

    def __init__(self, product_ids: list[UUID]) -> None:
        self.product_ids = product_ids
        super().__init__(f"Unknown products: {', '.join(str(one) for one in product_ids)}")


class InsufficientStockError(DomainError):
    code = "insufficient_stock"

    def __init__(self, shortages: list[StockShortage]) -> None:
        self.shortages = shortages
        super().__init__("Not enough stock for the requested quantities")


class IdempotencyKeyConflictError(DomainError):
    code = "idempotency_key_conflict"

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__("This Idempotency-Key was already used with a different request body")


class OrderNotFoundError(DomainError):
    code = "order_not_found"

    def __init__(self, order_id: UUID) -> None:
        self.order_id = order_id
        super().__init__(f"Order {order_id} does not exist")
