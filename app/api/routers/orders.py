from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Response, status

from app.api.deps import OrderServiceDependency
from app.api.schemas.order import OrderCreate, OrderResponse
from app.services.order_service import RequestedItem

router = APIRouter(prefix="/orders", tags=["orders"])

IdempotencyKey = Annotated[str, Header(min_length=1, max_length=255)]


@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    payload: OrderCreate,
    idempotency_key: IdempotencyKey,
    response: Response,
    orders: OrderServiceDependency,
) -> OrderResponse:
    order, created = await orders.create_order(
        customer_id=payload.customer_id,
        idempotency_key=idempotency_key,
        request_hash=payload.fingerprint(),
        items=[
            RequestedItem(product_id=item.product_id, quantity=item.quantity)
            for item in payload.items
        ],
    )
    if not created:
        response.status_code = status.HTTP_200_OK
    return OrderResponse.from_model(order)


@router.get("", response_model=list[OrderResponse])
async def list_orders(orders: OrderServiceDependency) -> list[OrderResponse]:
    return [OrderResponse.from_model(order) for order in await orders.list_orders()]


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(order_id: UUID, orders: OrderServiceDependency) -> OrderResponse:
    return OrderResponse.from_model(await orders.get_order(order_id))
