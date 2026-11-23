"""Part of the homework infrastructure. Don't change; see CONTRACT.md."""

from collections.abc import Iterator
from typing import Final, TypedDict

from dataclasses import dataclass, replace
import json
from pathlib import Path
import random

MESSAGE_ID_SPACE: Final = 2**32
# Odd, so adding it modulo 2**32 visits every id once before repeating; large, so
# consecutive ids look unrelated.
MESSAGE_ID_STRIDE: Final = 0x9E3779B1
RELATIVE_STEP_STDDEV: Final = 0.00005
RELATIVE_SPREAD: Final = 0.00004


class TickPayload(TypedDict):
    symbol: str
    bid: float | None
    ask: float | None


class Tick(TypedDict):
    messageId: str
    timestampMsc: int
    eventType: str
    source: str
    mayBeOutOfSequence: bool
    payload: TickPayload


@dataclass(slots=True)
class Instrument:
    source: str
    symbol: str
    mid: float
    decimals: int


# The replay fixture's instruments, so live mode streams the same ones.
CORE_INSTRUMENTS: Final = (
    Instrument(source="ctrader", symbol="EURUSD", mid=1.16050, decimals=5),
    Instrument(source="ctrader", symbol="GBPJPY", mid=201.250, decimals=3),
    Instrument(source="ctrader", symbol="XAUUSD", mid=4012.15, decimals=2),
    Instrument(source="mt5", symbol="EURUSD", mid=1.16052, decimals=5),
    Instrument(source="mt5", symbol="US500", mid=6810.5, decimals=1),
    Instrument(source="dxtrade", symbol="BTCUSD", mid=104_250.0, decimals=1),
)
SOURCES: Final = ("ctrader", "mt5", "dxtrade")


def build_universe(symbol_count: int) -> list[Instrument]:
    # Copies, because the random walk mutates each instrument's mid price.
    instruments = [replace(instrument) for instrument in CORE_INSTRUMENTS]
    for index in range(symbol_count - len(instruments)):
        instruments.append(
            Instrument(
                source=SOURCES[index % len(SOURCES)],
                symbol=f"SYN{index:03d}",
                mid=100.0 + index,
                decimals=3,
            )
        )
    return instruments[:symbol_count]


class MessageIds:
    """Ids in the production gateway's format."""

    def __init__(self, start: int) -> None:
        self._next = start % MESSAGE_ID_SPACE

    def __call__(self) -> str:
        value = self._next
        self._next = (self._next + MESSAGE_ID_STRIDE) % MESSAGE_ID_SPACE
        return f"{value:08X}"


class LiveSource:
    def __init__(self, symbol_count: int, rng: random.Random) -> None:
        self._universe = build_universe(symbol_count)
        self._rng = rng
        self._message_ids = MessageIds(rng.getrandbits(32))

    def next_tick(self, timestamp_msc: int) -> Tick:
        instrument = self._rng.choice(self._universe)
        step = instrument.mid * self._rng.gauss(0.0, RELATIVE_STEP_STDDEV)
        instrument.mid = max(instrument.mid + step, 10 ** (-instrument.decimals))
        spread = instrument.mid * RELATIVE_SPREAD
        bid = round(instrument.mid - spread / 2, instrument.decimals)
        ask = round(instrument.mid + spread / 2, instrument.decimals)
        return {
            "messageId": self._message_ids(),
            "timestampMsc": timestamp_msc,
            "eventType": "symbolTick",
            "source": instrument.source,
            "mayBeOutOfSequence": False,
            "payload": {"symbol": instrument.symbol, "bid": bid, "ask": ask},
        }


def read_fixture(path: Path) -> Iterator[Tick]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if stripped := line.strip():
                tick: Tick = json.loads(stripped)
                yield tick
