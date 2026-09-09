from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import Field, PlainSerializer

CENT = Decimal("0.01")


def serialize_money(value: Decimal) -> str:
    return str(value.quantize(CENT))


MoneyAmount = Annotated[
    Decimal,
    Field(ge=0, max_digits=12, decimal_places=2),
    PlainSerializer(serialize_money, return_type=str, when_used="json"),
]
