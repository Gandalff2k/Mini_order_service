from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.core.exceptions import (
    DomainError,
    IdempotencyKeyConflictError,
    InsufficientStockError,
    OrderNotFoundError,
    ProductsNotFoundError,
)

STATUS_BY_ERROR: dict[type[DomainError], int] = {
    ProductsNotFoundError: status.HTTP_404_NOT_FOUND,
    OrderNotFoundError: status.HTTP_404_NOT_FOUND,
    InsufficientStockError: status.HTTP_409_CONFLICT,
    IdempotencyKeyConflictError: status.HTTP_409_CONFLICT,
}


def _details(error: DomainError) -> dict[str, Any]:
    detail: dict[str, Any] = {"code": error.code, "message": str(error)}
    if isinstance(error, ProductsNotFoundError):
        detail["product_ids"] = [str(one) for one in error.product_ids]
    elif isinstance(error, InsufficientStockError):
        detail["shortages"] = [
            {
                "product_id": str(shortage.product_id),
                "requested": shortage.requested,
                "available": shortage.available,
            }
            for shortage in error.shortages
        ]
    return detail


async def handle_domain_error(_: Request, error: Exception) -> JSONResponse:
    if not isinstance(error, DomainError):
        raise error
    status_code = STATUS_BY_ERROR.get(type(error), status.HTTP_400_BAD_REQUEST)
    return JSONResponse(status_code=status_code, content={"detail": _details(error)})


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, handle_domain_error)
