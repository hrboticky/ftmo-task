from typing import Final, Literal

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import DateTime, Interval, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY

type CandleSize = Literal["1m", "5m", "15m", "1h", "12h", "1d", "1w", "1M", "1y"]

PRAGUE: Final = ZoneInfo("Europe/Prague")
MINUTES: Final = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
MAX_CANDLES: Final = 20_000
LATENESS: Final = timedelta(seconds=1500)

CANDLES_QUERY: Final = text("""
    WITH bounds AS (
        SELECT at, lead(at) OVER (ORDER BY at) AS end_at
        FROM unnest(CAST(:boundaries AS timestamptz[])) AS edges(at)
    )
    SELECT b.at, candle.open, candle.high, candle.low, candle.close
    FROM bounds AS b
    CROSS JOIN LATERAL (
        SELECT first_value(t.bid) OVER prices AS open,
               max(t.bid) OVER prices AS high,
               min(t.bid) OVER prices AS low,
               last_value(t.bid) OVER prices AS close
        FROM ticks AS t
        WHERE t.source = :source AND t.symbol = :symbol AND t.bid IS NOT NULL
          AND t.timestamp >= :start_at AND t.timestamp < :end_at
          AND t.timestamp >= b.at AND t.timestamp < b.end_at
          AND (t.prior_event_time IS NULL OR t.prior_event_time < b.end_at + :lateness)
        WINDOW prices AS (
            ORDER BY t.timestamp, t.message_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
        LIMIT 1
    ) AS candle
    WHERE b.end_at IS NOT NULL
    ORDER BY b.at
""").bindparams(
    bindparam("boundaries", type_=ARRAY(DateTime(timezone=True))),
    bindparam("start_at", type_=DateTime(timezone=True)),
    bindparam("end_at", type_=DateTime(timezone=True)),
    bindparam("lateness", type_=Interval()),
)


def floor_boundary(moment: datetime, size: CandleSize) -> datetime:
    local = moment.astimezone(PRAGUE)
    step = MINUTES.get(size)
    if step is not None:
        return local.replace(minute=local.minute // step * step, second=0, microsecond=0).astimezone(UTC)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0, fold=0)
    match size:
        case "12h":
            return midnight.replace(hour=local.hour // 12 * 12).astimezone(UTC)
        case "1w":
            return (midnight - timedelta(days=local.weekday())).astimezone(UTC)
        case "1M":
            return midnight.replace(day=1).astimezone(UTC)
        case "1y":
            return midnight.replace(month=1, day=1).astimezone(UTC)
        case _:
            return midnight.astimezone(UTC)


def next_boundary(start: datetime, size: CandleSize) -> datetime:
    step = MINUTES.get(size)
    if step is not None:
        return start + timedelta(minutes=step)
    local = start.astimezone(PRAGUE)
    match size:
        case "12h":
            boundary = local + timedelta(hours=12)
        case "1w":
            boundary = local + timedelta(weeks=1)
        case "1M":
            boundary = local.replace(year=local.year + local.month // 12, month=local.month % 12 + 1)
        case "1y":
            boundary = local.replace(year=local.year + 1)
        case _:
            boundary = local + timedelta(days=1)
    return boundary.astimezone(UTC)


def boundaries_for(start: datetime, end: datetime, size: CandleSize) -> list[datetime]:
    try:
        boundary = floor_boundary(start, size)
        if boundary < start:
            boundary = next_boundary(boundary, size)
        boundaries: list[datetime] = []
        while boundary < end and len(boundaries) <= MAX_CANDLES:
            boundaries.append(boundary)
            boundary = next_boundary(boundary, size)
    except (OverflowError, ValueError) as error:
        raise ValueError("Candle boundaries are out of range") from error
    if len(boundaries) > MAX_CANDLES:
        raise ValueError("Candle ranges may contain at most 20,000 interval starts")
    if boundaries:
        boundaries.append(boundary)
    return boundaries
