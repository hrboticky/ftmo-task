from datetime import UTC, datetime, timedelta

import pytest

from app.candles import MAX_CANDLES, CandleSize, boundaries_for


@pytest.mark.parametrize(
    ("size", "start", "end", "expected"),
    [
        ("1h", "2026-10-25T00:00Z", "2026-10-25T02:00Z", ["2026-10-25T00:00Z", "2026-10-25T01:00Z", "2026-10-25T02:00Z"]),
        ("1h", "2026-11-02T10:15Z", "2026-11-02T12:15Z", ["2026-11-02T11:00Z", "2026-11-02T12:00Z", "2026-11-02T13:00Z"]),
        ("12h", "2026-10-24T22:00Z", "2026-10-25T23:00Z", ["2026-10-24T22:00Z", "2026-10-25T11:00Z", "2026-10-25T23:00Z"]),
        ("1d", "2026-03-28T23:00Z", "2026-03-29T22:00Z", ["2026-03-28T23:00Z", "2026-03-29T22:00Z"]),
        ("1w", "2026-10-24T23:00Z", "2026-11-01T23:00Z", ["2026-10-25T23:00Z", "2026-11-01T23:00Z"]),
        ("1M", "2026-11-01T00:00+01:00", "2026-12-01T00:00+01:00", ["2026-10-31T23:00Z", "2026-11-30T23:00Z"]),
        ("1y", "2026-01-01T00:00+01:00", "2026-01-01T00:01+01:00", ["2025-12-31T23:00Z", "2026-12-31T23:00Z"]),
        ("1h", "2026-11-02T10:05Z", "2026-11-02T10:45Z", []),
    ],
)
def test_prague_candle_boundaries(size: CandleSize, start: str, end: str, expected: list[str]) -> None:
    assert boundaries_for(datetime.fromisoformat(start), datetime.fromisoformat(end), size) == [
        datetime.fromisoformat(value) for value in expected
    ]


def test_candle_limit_counts_starts_and_excludes_the_terminal_boundary() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(minutes=MAX_CANDLES)
    assert len(boundaries_for(start, end, "1m")) == MAX_CANDLES + 1
    with pytest.raises(ValueError, match="20,000"):
        boundaries_for(start, end + timedelta(microseconds=1), "1m")
