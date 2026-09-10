from __future__ import annotations

import random
from datetime import datetime, timedelta


def retry_ceiling(attempt: int, *, base_seconds: float, cap_seconds: float) -> float:
    return min(cap_seconds, base_seconds * 2.0 ** max(attempt - 1, 0))


def next_attempt_at(
    now: datetime, attempt: int, *, base_seconds: float, cap_seconds: float
) -> datetime:
    ceiling = retry_ceiling(attempt, base_seconds=base_seconds, cap_seconds=cap_seconds)
    return now + timedelta(seconds=random.uniform(0, ceiling))
