"""Generate the deterministic replay fixture (fixtures/ticks.jsonl).

Part of the homework infrastructure. Don't change; see CONTRACT.md.

The fixture spans 2026-10-23 to 2026-11-03 UTC, so it covers:
- a weekend (FX, gold and indices closed; BTCUSD keeps trading),
- the end of summer time in Prague (2026-10-25: 03:00 CEST becomes 02:00 CET),
  which makes that day 25 hours long,
- the start of November in Prague time (2026-10-31 23:00 UTC),
- ticks placed exactly on Prague midnight, noon, week and month boundaries,
- a few ticks that share a timestamp with another tick of the same instrument,
- naturally missing bid or ask prices.

Ticks that share a timestamp are always strictly inside their minute, never the first
or last tick of it. Every candle boundary is a minute boundary, so a tie can never be a
candle's open or close, and the expected candles don't depend on how ties are ordered.

The same seed always produces the same file, so the acceptance suite can compare a
candidate's API output with a precomputed expected result.
"""

from typing import Final

import argparse
import bisect
import calendar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import math
import operator
from pathlib import Path
import random

# The image's fixture uses this seed; the checks compare against the same one.
FIXTURE_SEED: Final = 2026
START_AT: Final = datetime(2026, 10, 23, tzinfo=UTC)
END_AT: Final = datetime(2026, 11, 3, tzinfo=UTC)

# Interesting moments in Europe/Prague, written in UTC.
BOUNDARIES: Final = (
    datetime(2026, 10, 23, 10, 0, tzinfo=UTC),  # Fri 12:00 CEST (12h candle boundary)
    datetime(2026, 10, 23, 21, 59, 59, 999_000, tzinfo=UTC),  # Fri 23:59:59.999 CEST
    datetime(2026, 10, 24, 22, 0, tzinfo=UTC),  # Sun 25 Oct 00:00 CEST (start of the 25-hour day)
    datetime(2026, 10, 25, 0, 0, tzinfo=UTC),  # 02:00 CEST, first time
    datetime(2026, 10, 25, 0, 59, 59, 999_000, tzinfo=UTC),  # 02:59:59.999 CEST, last moment of summer time
    datetime(2026, 10, 25, 1, 0, tzinfo=UTC),  # 02:00 CET, second time
    datetime(2026, 10, 25, 22, 59, 59, 999_000, tzinfo=UTC),  # Sun 23:59:59.999 CET
    datetime(2026, 10, 25, 23, 0, tzinfo=UTC),  # Mon 26 Oct 00:00 CET (week start)
    datetime(2026, 10, 26, 11, 0, tzinfo=UTC),  # Mon 12:00 CET (12h candle boundary)
    datetime(2026, 10, 31, 22, 59, 59, 999_000, tzinfo=UTC),  # Sat 31 Oct 23:59:59.999 CET
    datetime(2026, 10, 31, 23, 0, tzinfo=UTC),  # Sun 1 Nov 00:00 CET (month start)
    datetime(2026, 11, 1, 23, 0, tzinfo=UTC),  # Mon 2 Nov 00:00 CET (week start)
)
TIE_COUNT: Final = 5
BOUNDARY_OFFSET: Final = 0.0002

MARKET_CLOSE_HOUR: Final = 21
NULL_PRICE_RATE: Final = 0.02
RELATIVE_SPREAD: Final = 0.00004


@dataclass(frozen=True, slots=True)
class Stream:
    source: str
    symbol: str
    mid: float
    decimals: int
    weekend_trading: bool
    mean_interval_s: float


STREAMS: Final = (
    Stream("ctrader", "EURUSD", 1.16050, 5, weekend_trading=False, mean_interval_s=90.0),
    Stream("ctrader", "GBPJPY", 201.250, 3, weekend_trading=False, mean_interval_s=150.0),
    Stream("ctrader", "XAUUSD", 4012.15, 2, weekend_trading=False, mean_interval_s=120.0),
    Stream("mt5", "EURUSD", 1.16052, 5, weekend_trading=False, mean_interval_s=110.0),
    Stream("mt5", "US500", 6810.5, 1, weekend_trading=False, mean_interval_s=180.0),
    Stream("dxtrade", "BTCUSD", 104_250.0, 1, weekend_trading=True, mean_interval_s=120.0),
)


def market_open(moment_at: datetime, weekend_trading: bool) -> bool:
    """Deliberately simplified: one weekly close for every market, no holidays."""
    if weekend_trading:
        return True
    match moment_at.weekday():
        case calendar.FRIDAY:
            return moment_at.hour < MARKET_CLOSE_HOUR
        case calendar.SATURDAY:
            return False
        case calendar.SUNDAY:
            return moment_at.hour >= MARKET_CLOSE_HOUR
        case _:
            return True


def to_ms(moment_at: datetime) -> int:
    return int(moment_at.timestamp() * 1000)


type Row = tuple[int, str, str, float]


def _arrange_ties(rows: list[Row], rng: random.Random) -> list[Row]:
    """Leave exactly TIE_COUNT timestamp ties, each strictly inside its minute."""
    by_instrument: dict[tuple[str, str], list[Row]] = {}
    for row in rows:
        by_instrument.setdefault((row[1], row[2]), []).append(row)

    def interior(timestamps: list[int], ms: int) -> bool:
        # timestamps is sorted: is there an earlier and a later tick in the same minute?
        minute_start = ms // 60_000 * 60_000
        before = bisect.bisect_left(timestamps, ms)
        after = bisect.bisect_right(timestamps, ms)
        return (
            before > 0
            and timestamps[before - 1] >= minute_start
            and (after < len(timestamps) and timestamps[after] < minute_start + 60_000)
        )

    arranged: list[Row] = []
    ties: list[Row] = []
    for instrument_rows in by_instrument.values():
        instrument_rows.sort(key=operator.itemgetter(0))
        timestamps = [row[0] for row in instrument_rows]
        seen: set[int] = set()
        unique: list[Row] = []
        for original_ms, source, symbol, mid in instrument_rows:
            # Accidental ties (sub-millisecond gaps in busy periods) are moved apart.
            ms = original_ms
            while ms in seen:
                ms += 1
            seen.add(ms)
            unique.append((ms, source, symbol, mid))
        candidates = [row for row in unique if interior(timestamps, row[0])]
        ties.extend(candidates)
        arranged.extend(unique)

    for ms, source, symbol, mid in rng.sample(sorted(ties), TIE_COUNT):
        # A second, slightly different price at exactly the same moment.
        arranged.append((ms, source, symbol, mid * (1 + 0.0001)))
    return arranged


def generate(seed: int) -> list[dict[str, object]]:
    rng = random.Random(seed)
    rows: list[tuple[int, str, str, float]] = []

    for stream in STREAMS:
        mid = stream.mid
        tick_at = START_AT
        busy_until_at = START_AT
        next_boundary = 0
        while tick_at < END_AT:
            if tick_at >= busy_until_at and rng.random() < 0.01:
                busy_until_at = tick_at + timedelta(minutes=10)
            mean_interval_s = 4.0 if tick_at < busy_until_at else stream.mean_interval_s
            tick_at += timedelta(seconds=rng.expovariate(1 / mean_interval_s))
            while next_boundary < len(BOUNDARIES) and BOUNDARIES[next_boundary] < tick_at:
                boundary_at = BOUNDARIES[next_boundary]
                if market_open(boundary_at, stream.weekend_trading):
                    # Off the walk by a fixed step, alternating up and down, so a boundary
                    # tick put in the wrong candle changes it, and the ticks a millisecond
                    # apart on either side of a boundary have different prices.
                    offset = BOUNDARY_OFFSET if next_boundary % 2 == 0 else -BOUNDARY_OFFSET
                    rows.append((to_ms(boundary_at), stream.source, stream.symbol, mid * (1 + offset)))
                next_boundary += 1
            if tick_at >= END_AT or not market_open(tick_at, stream.weekend_trading):
                continue
            mid = max(mid * math.exp(rng.gauss(0.0, 0.0004)), 10**-stream.decimals)
            rows.append((to_ms(tick_at), stream.source, stream.symbol, mid))

    rows = _arrange_ties(rows, rng)
    rows.sort(key=operator.itemgetter(0, 1, 2))

    decimals_by_key = {(stream.source, stream.symbol): stream.decimals for stream in STREAMS}
    message_ids = rng.sample(range(2**32), len(rows))  # unique within the fixture
    ticks: list[dict[str, object]] = []
    for (ms, source, symbol, mid), message_id in zip(rows, message_ids, strict=True):
        decimals = decimals_by_key[(source, symbol)]
        spread = mid * RELATIVE_SPREAD
        bid: float | None = round(mid - spread / 2, decimals)
        ask: float | None = round(mid + spread / 2, decimals)
        if rng.random() < NULL_PRICE_RATE:
            if rng.random() < 0.5:
                bid = None
            else:
                ask = None
        ticks.append(
            {
                "messageId": f"{message_id:08X}",
                "timestampMsc": ms,
                "eventType": "symbolTick",
                "source": source,
                "mayBeOutOfSequence": False,
                "payload": {"symbol": symbol, "bid": bid, "ask": ask},
            }
        )
    return ticks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("fixtures/ticks.jsonl"))
    parser.add_argument("--seed", type=int, default=FIXTURE_SEED)
    arguments = parser.parse_args()

    ticks = generate(arguments.seed)
    arguments.out.parent.mkdir(parents=True, exist_ok=True)
    with arguments.out.open("w", encoding="utf-8") as handle:
        for tick in ticks:
            handle.write(json.dumps(tick, separators=(",", ":")) + "\n")
    print(f"wrote {len(ticks)} ticks to {arguments.out}")  # noqa: T201


if __name__ == "__main__":
    main()
