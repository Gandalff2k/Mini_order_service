from __future__ import annotations

import random
from datetime import datetime, timedelta


def retry_ceiling(attempt: int, *, base_seconds: float, cap_seconds: float) -> float:
    return min(cap_seconds, base_seconds * 2.0 ** max(attempt - 1, 0))


def retry_delay(attempt: int, *, base_seconds: float, cap_seconds: float) -> float:
    ceiling = retry_ceiling(attempt, base_seconds=base_seconds, cap_seconds=cap_seconds)
    return random.uniform(0, ceiling)


def next_attempt_at(
    now: datetime, attempt: int, *, base_seconds: float, cap_seconds: float
) -> datetime:
    delay = retry_delay(attempt, base_seconds=base_seconds, cap_seconds=cap_seconds)
    return now + timedelta(seconds=delay)
