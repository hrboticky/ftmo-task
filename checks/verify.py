"""`make verify`: replays the fixture through your system and compares /ticks with it.

Part of the homework infrastructure. Don't change; see CONTRACT.md.

Steps:
1. delete all data (`docker compose down -v`) and start your system again, with the
   simulator in replay mode, the `replay` fault preset (duplicates, reordering, malformed
   messages; no disconnects) and manual start, so only this replay is in the database,
2. wait until your system has subscribed to every fixture instrument, then start the replay,
3. wait until the replay has finished and every stage's staleness gauge has stopped
   advancing,
4. read /ticks for the six fixture instruments, day by day, and compare with the fixture.

It doesn't check candle values. Afterwards the simulator is restarted with your usual
settings.
"""

from collections.abc import Callable, Iterator
from typing import Final

from collections import Counter
from datetime import UTC, datetime, timedelta
import os
import subprocess
import sys
import time

from checks.common import (
    API_URL,
    SIMULATOR_URL,
    Report,
    as_dict,
    as_list,
    compose,
    gauge_values,
    http_get,
    iso,
    load_decisions,
    metrics_entries,
    parse_ms,
)
from simulator.generate_fixture import END_AT, FIXTURE_SEED, START_AT, generate

RATE: Final = os.environ.get("VERIFY_RATE", "2000")
REPLAY_ENV: Final = {"MODE": "replay", "RATE": RATE, "FAULTS": "replay", "REPLAY_START": "manual", "REPLAY_LOOP": "false"}
# Includes building the images and starting every service.
SUBSCRIBE_TIMEOUT_S: Final = 300
REPLAY_TIMEOUT_S: Final = 600
SETTLE_S: Final = 5
SETTLE_TIMEOUT_S: Final = 180
PAGE_LIMIT: Final = 10_000
EXAMPLES: Final = 5

type TickKey = tuple[str, str, int, str]  # source, symbol, timestampMsc, messageId


def wait_for(description: str, timeout_s: float, condition: Callable[[], bool]) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout_s:.0f} s waiting for {description}")
        time.sleep(1)


def reset_system() -> None:
    compose("down", "-v", "--remove-orphans")
    compose("up", "-d", "--build", env=dict(os.environ) | REPLAY_ENV)


def restart_simulator() -> None:
    compose("up", "-d", "--force-recreate", "--no-deps", "--wait", "simulator", env=dict(os.environ))


def start_replay() -> None:
    def started() -> bool:
        try:
            return http_get(f"{SIMULATOR_URL}/replay/start").status == 200
        except OSError:
            return False

    wait_for("your system to subscribe to every fixture instrument", SUBSCRIBE_TIMEOUT_S, started)


def replay_finished() -> bool:
    stats = http_get(f"{SIMULATOR_URL}/stats").json()
    return isinstance(stats, dict) and stats.get("replayState") == "finished"


def wait_until_settled(urls: list[str]) -> set[str]:
    """Until no staleness gauge has changed for SETTLE_S seconds; returns the URLs that never answered."""
    previous: dict[str, float] = {}
    answered: set[str] = set()
    stable_since = time.monotonic()
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    while time.monotonic() - stable_since < SETTLE_S:
        if time.monotonic() > deadline:
            raise TimeoutError(f"your gauges were still advancing after {SETTLE_TIMEOUT_S} s")
        current: dict[str, float] = {}
        for url in urls:
            try:
                current |= {f"{url} {stage}": value for stage, value in gauge_values(http_get(url).body).items()}
            except OSError:
                continue
            answered.add(url)
        if current != previous:
            previous = current
            stable_since = time.monotonic()
        time.sleep(1)
    return set(urls) - answered


def fetch_ticks(source: str, symbol: str, start: datetime, end: datetime) -> Iterator[dict[str, object]]:
    params = {"source": source, "symbol": symbol, "from": iso(start), "to": iso(end), "limit": str(PAGE_LIMIT)}
    response = http_get(f"{API_URL}/ticks", params, timeout=60)
    if response.status != 200:
        raise RuntimeError(f"/ticks returned {response.status} for {source} {symbol} {iso(start)}: {response.body[:200]}")
    items = [as_dict(item, "tick") for item in as_list(as_dict(response.json(), "/ticks response")["items"], "items")]
    if len(items) >= PAGE_LIMIT:
        middle = start + (end - start) / 2
        yield from fetch_ticks(source, symbol, start, middle)
        yield from fetch_ticks(source, symbol, middle, end)
    else:
        yield from items


def stored_ticks(instruments: set[tuple[str, str]]) -> tuple[Counter[TickKey], dict[TickKey, tuple[object, object]]]:
    counts: Counter[TickKey] = Counter()
    prices: dict[TickKey, tuple[object, object]] = {}
    for source, symbol in sorted(instruments):
        day = START_AT
        while day < END_AT:
            for item in fetch_ticks(source, symbol, day, day + timedelta(days=1)):
                key = (str(item["source"]), str(item["symbol"]), parse_ms(item["timestamp"]), str(item["messageId"]))
                counts[key] += 1
                prices[key] = (item["bid"], item["ask"])
            day += timedelta(days=1)
    return counts, prices


def compare(report: Report) -> None:
    expected: dict[TickKey, tuple[object, object]] = {}
    for tick in generate(FIXTURE_SEED):
        payload = tick["payload"]
        timestamp = tick["timestampMsc"]
        if not isinstance(payload, dict) or not isinstance(timestamp, int):
            raise TypeError(f"unexpected fixture tick: {tick}")
        key = (str(tick["source"]), str(payload["symbol"]), timestamp, str(tick["messageId"]))
        expected[key] = (payload["bid"], payload["ask"])
    counts, prices = stored_ticks({(key[0], key[1]) for key in expected})

    missing = sorted(expected.keys() - counts.keys())
    duplicated = sorted(key for key, count in counts.items() if count > 1)
    unexpected = sorted(counts.keys() - expected.keys())
    wrong_prices = sorted(key for key in expected.keys() & prices.keys() if prices[key] != expected[key])

    def examples(keys: list[TickKey]) -> str:
        shown = [
            f"{source} {symbol} {iso(datetime.fromtimestamp(ms / 1000, UTC))} {message_id}"
            for source, symbol, ms, message_id in keys[:EXAMPLES]
        ]
        return "; e.g. " + ", ".join(shown) if shown else ""

    report.add(
        "PASS" if not missing else "FAIL", "no lost ticks", f"{len(missing)} of {len(expected)} missing{examples(missing)}"
    )
    report.add(
        "PASS" if not duplicated else "FAIL",
        "no duplicated ticks",
        f"{len(duplicated)} returned more than once{examples(duplicated)}",
    )
    report.add(
        "PASS" if not unexpected else "FAIL",
        "no unexpected ticks",
        f"{len(unexpected)} not in the fixture{examples(unexpected)}",
    )
    report.add(
        "PASS" if not wrong_prices else "FAIL",
        "prices as sent",
        f"{len(wrong_prices)} with a different bid or ask{examples(wrong_prices)}",
    )


def main() -> None:
    report = Report()
    try:
        decisions = load_decisions()
    except (OSError, ValueError, TypeError) as error:
        sys.exit(str(error))
    metrics_urls = sorted({url for _, url in metrics_entries(decisions)})
    try:
        print(  # noqa: T201
            f"Deleting all data and starting your system with a replay at {RATE} ticks/s ...", flush=True
        )
        reset_system()
        start_replay()
        report.add("PASS", "replay started")
        wait_for("the replay to finish", REPLAY_TIMEOUT_S, replay_finished)
        report.add("PASS", "replay finished")
        if silent := wait_until_settled(metrics_urls):
            report.add("WARN", "metrics endpoints", f"never answered: {', '.join(sorted(silent))}")
        report.add("PASS", "pipeline settled", f"no gauge changed for {SETTLE_S} s")
        compare(report)
    except (TimeoutError, RuntimeError, TypeError, OSError, subprocess.CalledProcessError) as error:
        report.add("FAIL", "verify", str(error))
    finally:
        print("Restarting the simulator with your usual settings ...", flush=True)  # noqa: T201
        try:
            restart_simulator()
        except Exception as error:
            print(f"could not restart the simulator: {error}")  # noqa: T201
    print(f"\n{report.summary()}")  # noqa: T201
    sys.exit(1 if report.failed else 0)


if __name__ == "__main__":
    main()
