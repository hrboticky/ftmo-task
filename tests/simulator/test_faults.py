import random

from simulator.config import FaultConfig
from simulator.faults import FaultInjector
from simulator.sources import Tick


def make_tick(index: int) -> Tick:
    return {
        "messageId": f"{index:08X}",
        "timestampMsc": 1_000 + index,
        "eventType": "symbolTick",
        "source": "ctrader",
        "mayBeOutOfSequence": False,
        "payload": {"symbol": "EURUSD", "bid": 1.1, "ask": 1.2},
    }


def drain(injector: FaultInjector, count: int) -> list[Tick]:
    sent: list[Tick] = []
    for index in range(count):
        sent += injector.process(make_tick(index), now=float(index))
        sent += injector.due(float(index))
    return sent + injector.flush()


def test_no_faults_passes_ticks_through_unchanged() -> None:
    injector = FaultInjector(FaultConfig(), random.Random(1), mutate_prices=True)
    assert drain(injector, 100) == [make_tick(index) for index in range(100)]


def test_duplicates_repeat_the_same_message_id() -> None:
    injector = FaultInjector(FaultConfig(duplicate_rate=0.2), random.Random(1), mutate_prices=False)
    message_ids = [tick["messageId"] for tick in drain(injector, 1_000)]
    assert len(message_ids) == 1_000 + injector.counts["duplicate"]
    assert len(set(message_ids)) == 1_000


def test_out_of_order_ticks_keep_their_original_timestamp() -> None:
    injector = FaultInjector(
        FaultConfig(out_of_order_rate=0.1, out_of_order_max_delay_ms=5_000),
        random.Random(3),
        mutate_prices=False,
    )
    timestamps = [tick["timestampMsc"] for tick in drain(injector, 1_000)]
    assert timestamps != sorted(timestamps)
    assert sorted(timestamps) == [1_000 + index for index in range(1_000)]


def test_every_copy_of_a_held_tick_carries_the_same_flag() -> None:
    injector = FaultInjector(
        FaultConfig(duplicate_rate=0.5, out_of_order_rate=0.5, out_of_order_flag_rate=0.5),
        random.Random(1),
        mutate_prices=False,
    )
    flags: dict[str, set[bool]] = {}
    for tick in drain(injector, 1_000):
        flags.setdefault(tick["messageId"], set()).add(tick["mayBeOutOfSequence"])
    assert any(True in flag for flag in flags.values())
    assert all(len(flag) == 1 for flag in flags.values())


def test_null_prices_are_not_injected_without_mutate_prices() -> None:
    injector = FaultInjector(FaultConfig(null_price_rate=1.0), random.Random(1), mutate_prices=False)
    assert all(tick["payload"] == make_tick(0)["payload"] for tick in drain(injector, 50))
