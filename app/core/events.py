from __future__ import annotations

from typing import Any
from uuid import uuid4

from app.core.money import format_amount
from app.models import Order, OutboxMessage

ORDER_AGGREGATE = "order"
ORDER_CREATED = "order.created"
ORDER_CREATED_SCHEMA_VERSION = 1


def build_order_created_message(order: Order) -> OutboxMessage:
    payload: dict[str, Any] = {
        "order_id": str(order.id),
        "customer_id": str(order.customer_id),
        "total_amount": format_amount(order.total_amount),
        "created_at": order.created_at.isoformat(),
        "items": [
            {
                "product_id": str(item.product_id),
                "quantity": item.quantity,
                "unit_price": format_amount(item.unit_price),
            }
            for item in order.items
        ],
    }
    return OutboxMessage(
        event_id=uuid4(),
        aggregate_type=ORDER_AGGREGATE,
        aggregate_id=order.id,
        event_type=ORDER_CREATED,
        schema_version=ORDER_CREATED_SCHEMA_VERSION,
        payload=payload,
    )
