from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.core.backoff import next_attempt_at, retry_ceiling

BASE = 1.0
CAP = 30.0


class TestRetryCeiling:
    @pytest.mark.parametrize(
        ("attempt", "expected"),
        [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 16.0), (6, 30.0), (10, 30.0)],
    )
    def test_doubles_until_it_reaches_the_cap(self, attempt: int, expected: float) -> None:
        assert retry_ceiling(attempt, base_seconds=BASE, cap_seconds=CAP) == expected

    def test_never_exceeds_the_cap(self) -> None:
        ceilings = [retry_ceiling(a, base_seconds=BASE, cap_seconds=CAP) for a in range(1, 40)]

        assert max(ceilings) == CAP

    def test_grows_monotonically(self) -> None:
        ceilings = [retry_ceiling(a, base_seconds=BASE, cap_seconds=CAP) for a in range(1, 10)]

        assert ceilings == sorted(ceilings)


class TestNextAttemptAt:
    def test_stays_within_the_jitter_window(self) -> None:
        now = datetime.now(UTC)
        attempt = 3

        delays = [
            (
                next_attempt_at(now, attempt, base_seconds=BASE, cap_seconds=CAP) - now
            ).total_seconds()
            for _ in range(200)
        ]

        assert min(delays) >= 0
        assert max(delays) <= retry_ceiling(attempt, base_seconds=BASE, cap_seconds=CAP)

    def test_spreads_retries_instead_of_returning_one_value(self) -> None:
        now = datetime.now(UTC)

        delays = {next_attempt_at(now, 5, base_seconds=BASE, cap_seconds=CAP) for _ in range(200)}

        assert len(delays) > 100
