from __future__ import annotations

from decimal import Decimal

CENT = Decimal("0.01")
ZERO = Decimal("0")


def format_amount(value: Decimal) -> str:
    return str(value.quantize(CENT))
