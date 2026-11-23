from collections import Counter

import pytest

from simulator.generate_fixture import BOUNDARIES, FIXTURE_SEED, TIE_COUNT, generate


@pytest.fixture(scope="module")
def ticks() -> list[dict[str, object]]:
    return generate(FIXTURE_SEED)


def test_fixture_is_deterministic(ticks: list[dict[str, object]]) -> None:
    assert generate(FIXTURE_SEED) == ticks


def test_message_ids_are_unique(ticks: list[dict[str, object]]) -> None:
    assert len({tick["messageId"] for tick in ticks}) == len(ticks)


def test_ties_are_deliberate_and_never_open_or_close_a_minute(ticks: list[dict[str, object]]) -> None:
    by_instrument: dict[tuple[object, object], list[int]] = {}
    for tick in ticks:
        payload = tick["payload"]
        assert isinstance(payload, dict)
        timestamp = tick["timestampMsc"]
        assert isinstance(timestamp, int)
        by_instrument.setdefault((tick["source"], payload["symbol"]), []).append(timestamp)
    ties = 0
    for timestamps in by_instrument.values():
        for tied_timestamp, count in Counter(timestamps).items():
            if count < 2:
                continue
            ties += 1
            minute = [timestamp for timestamp in timestamps if timestamp // 60_000 == tied_timestamp // 60_000]
            assert min(minute) < tied_timestamp < max(minute)
    assert ties == TIE_COUNT


def test_boundary_ticks_continue_the_price_walk_with_a_distinct_price(ticks: list[dict[str, object]]) -> None:
    boundaries = {round(boundary_at.timestamp() * 1000) for boundary_at in BOUNDARIES}
    previous_price: dict[tuple[object, object], float] = {}
    checked = 0
    for tick in ticks:
        payload = tick["payload"]
        assert isinstance(payload, dict)
        if payload["bid"] is None or payload["ask"] is None:
            continue
        instrument = (tick["source"], payload["symbol"])
        price = (payload["bid"] + payload["ask"]) / 2
        if tick["timestampMsc"] in boundaries and instrument in previous_price:
            relative_change = abs(price / previous_price[instrument] - 1)
            # Not a jump to the end-of-fixture price, but not the same price either: a
            # boundary tick in the wrong candle must change that candle.
            assert 0 < relative_change < 0.005, (tick, previous_price[instrument])
            checked += 1
        previous_price[instrument] = price
    assert checked > 20
