from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from app.api.schemas.common import MoneyAmount

ProductName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]


class ProductCreate(BaseModel):
    name: ProductName
    price: MoneyAmount
    stock: Annotated[int, Field(ge=0)]


class ProductResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    price: MoneyAmount
    stock: int
    created_at: datetime
