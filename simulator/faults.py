"""Part of the homework infrastructure. Don't change; see CONTRACT.md.

Faults are applied to each tick before it is sent. Disconnects are per connection
(see server.py).
"""

from typing import Final

from collections import Counter
import copy
from dataclasses import dataclass, field
import heapq
import itertools
import json
import random

from simulator.config import FaultConfig
from simulator.sources import Tick

OUT_OF_ORDER_MIN_DELAY_S: Final = 0.1
MALFORMED_MESSAGES: Final = (
    '{"messageId": "DEADBEEF", "eventType": "symbolTick", "payload": {',
    json.dumps({"eventType": "heartbeat", "timestampMsc": 0}),
    json.dumps({"messageId": "0BADC0DE", "eventType": "symbolTick", "source": "ctrader"}),
    "not json at all",
)


@dataclass(order=True, slots=True)
class _Delayed:
    release_at: float
    sequence: int
    tick: Tick = field(compare=False)


class FaultInjector:
    def __init__(self, faults: FaultConfig, rng: random.Random, *, mutate_prices: bool) -> None:
        self._faults = faults
        self._rng = rng
        # Null prices change the data itself, so they are only applied in live mode;
        # the replay fixture already contains its own nulls and a known expected output.
        self._mutate_prices = mutate_prices
        self._delayed: list[_Delayed] = []
        self._sequence = itertools.count()
        self.counts: Counter[str] = Counter()

    def process(self, tick: Tick, now: float) -> list[Tick]:
        faults = self._faults
        rng = self._rng
        ready: list[Tick] = []
        sent = tick

        if self._mutate_prices and faults.null_price_rate and rng.random() < faults.null_price_rate:
            if rng.random() < 0.5:
                tick["payload"]["bid"] = None
            else:
                tick["payload"]["ask"] = None
            self.counts["null_price"] += 1

        if faults.out_of_order_rate and rng.random() < faults.out_of_order_rate:
            sent = copy.deepcopy(tick)
            sent["mayBeOutOfSequence"] = rng.random() < faults.out_of_order_flag_rate
            delay = rng.uniform(OUT_OF_ORDER_MIN_DELAY_S, faults.out_of_order_max_delay_ms / 1000)
            self._push(sent, now + delay)
            self.counts["out_of_order"] += 1
        else:
            ready.append(tick)

        if faults.duplicate_rate and rng.random() < faults.duplicate_rate:
            delay = rng.uniform(0.0, faults.duplicate_max_delay_ms / 1000)
            # A copy of what was sent, flag included: both copies of a tick look the same.
            self._push(copy.deepcopy(sent), now + delay)
            self.counts["duplicate"] += 1

        return ready

    def due(self, now: float) -> list[Tick]:
        ready: list[Tick] = []
        while self._delayed and self._delayed[0].release_at <= now:
            ready.append(heapq.heappop(self._delayed).tick)
        return ready

    def flush(self) -> list[Tick]:
        ready = [delayed.tick for delayed in sorted(self._delayed)]
        self._delayed.clear()
        return ready

    def maybe_malformed(self) -> str | None:
        malformed_rate = self._faults.malformed_rate
        if not malformed_rate or self._rng.random() >= malformed_rate:
            return None
        self.counts["malformed"] += 1
        return self._rng.choice(MALFORMED_MESSAGES)

    def burst_active(self, elapsed_s: float) -> bool:
        faults = self._faults
        if not faults.burst_every_s:
            return False
        return elapsed_s % faults.burst_every_s >= faults.burst_every_s - faults.burst_duration_s

    def _push(self, tick: Tick, release_at: float) -> None:
        heapq.heappush(
            self._delayed,
            _Delayed(release_at=release_at, sequence=next(self._sequence), tick=tick),
        )
