from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from app.api.schemas.common import MoneyAmount
from app.models import Order, OrderItem

MAX_ITEM_QUANTITY = 1_000_000


class OrderItemCreate(BaseModel):
    product_id: UUID
    quantity: Annotated[int, Field(gt=0, le=MAX_ITEM_QUANTITY)]


class OrderCreate(BaseModel):
    customer_id: UUID
    items: Annotated[list[OrderItemCreate], Field(min_length=1)]

    @model_validator(mode="after")
    def reject_repeated_products(self) -> OrderCreate:
        product_ids = [item.product_id for item in self.items]
        if len(set(product_ids)) != len(product_ids):
            raise ValueError("each product may appear at most once per order")
        return self

    def fingerprint(self) -> str:
        canonical: dict[str, Any] = {
            "customer_id": str(self.customer_id),
            "items": sorted(
                (
                    {"product_id": str(item.product_id), "quantity": item.quantity}
                    for item in self.items
                ),
                key=lambda item: str(item["product_id"]),
            ),
        }
        encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class OrderItemResponse(BaseModel):
    product_id: UUID
    quantity: int
    unit_price: MoneyAmount
    line_total: MoneyAmount

    @classmethod
    def from_model(cls, item: OrderItem) -> OrderItemResponse:
        return cls(
            product_id=item.product_id,
            quantity=item.quantity,
            unit_price=item.unit_price,
            line_total=item.unit_price * item.quantity, #calculated line
        )


class OrderResponse(BaseModel):
    id: UUID
    customer_id: UUID
    total_amount: MoneyAmount
    created_at: datetime
    items: list[OrderItemResponse]

    @classmethod
    def from_model(cls, order: Order) -> OrderResponse:
        return cls(
            id=order.id,
            customer_id=order.customer_id,
            total_amount=order.total_amount,
            created_at=order.created_at,
            items=[OrderItemResponse.from_model(item) for item in order.items],
        )
