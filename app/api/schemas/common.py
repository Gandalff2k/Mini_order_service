from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import Field, PlainSerializer

from app.core.money import format_amount

MoneyAmount = Annotated[
    Decimal,
    Field(ge=0, max_digits=12, decimal_places=2),
    PlainSerializer(format_amount, return_type=str, when_used="json"),
]
