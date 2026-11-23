"""`make check`: a quick check that your system matches CONTRACT.md.

Part of the homework infrastructure. Don't change; see CONTRACT.md.

Run it with your system up (`make up`). It checks shapes and wiring, not correctness;
`make verify` replays the fixture and compares what your API stores.
"""

from datetime import UTC, datetime
import sys

from checks import protected
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
    require,
    validate_decisions,
)

SAMPLE_FROM = datetime(2026, 10, 26, 8, tzinfo=UTC)
SAMPLE_TO = datetime(2026, 10, 26, 9, tzinfo=UTC)
TICK_FIELDS = {"messageId", "timestamp", "source", "symbol", "bid", "ask"}
CANDLE_FIELDS = {"at", "open", "high", "low", "close"}


def check_protected(report: Report) -> None:
    if changes := protected.differences():
        report.add("FAIL", "protected files unchanged", f"{len(changes)} difference(s): {'; '.join(changes[:5])}")
    else:
        report.add("PASS", "protected files unchanged")


def check_decisions(report: Report) -> dict[str, object] | None:
    try:
        decisions = load_decisions()
    except (OSError, ValueError, TypeError) as error:
        report.add("FAIL", "decisions.yaml", str(error))
        return None
    if problems := validate_decisions(decisions):
        for problem in problems:
            report.add("FAIL", "decisions.yaml", problem)
    else:
        report.add("PASS", "decisions.yaml")
    return decisions


def check_services(report: Report, decisions: dict[str, object] | None) -> None:
    try:
        services = set(compose("config", "--services").stdout.split())
    except (RuntimeError, OSError) as error:
        report.add("WARN", "compose services", f"skipped: {error}")
        return
    except Exception as error:
        report.add("FAIL", "compose services", f"docker compose config failed: {error}")
        return
    ok = True
    for required in ("simulator", "kafka", "kafka-init", "timescaledb", "migrate", "api"):
        if required not in services:
            report.add("FAIL", "compose services", f"missing service {required!r}")
            ok = False
    if decisions is not None:
        listed = decisions.get("services")
        if isinstance(listed, dict):
            for key in ("ws_readers", "db_writers"):
                for name in listed.get(key) or []:
                    if isinstance(name, str) and name != "TODO" and name not in services:
                        report.add("FAIL", "compose services", f"services.{key} lists {name!r}, which isn't a compose service")
                        ok = False
    if ok:
        report.add("PASS", "compose services")


def check_up(report: Report) -> bool:
    simulator = _status(f"{SIMULATOR_URL}/healthz")
    report.add("PASS" if simulator == 200 else "FAIL", "simulator /healthz", f"status {simulator}")
    api = _status(f"{API_URL}/healthz")
    report.add("PASS" if api == 200 else "FAIL", "api /healthz", f"status {api}")
    return api == 200


def _status(url: str) -> int | str:
    try:
        return http_get(url).status
    except OSError as error:
        return f"unreachable ({error})"


def check_metrics(report: Report, decisions: dict[str, object] | None) -> None:
    api = http_get(f"{API_URL}/metrics/")
    report.add("PASS" if api.status == 200 else "FAIL", "api /metrics", f"status {api.status}")
    if decisions is None:
        return
    for stage, url in metrics_entries(decisions):
        try:
            response = http_get(url)
        except OSError as error:
            report.add("FAIL", f"metrics for stage {stage!r}", f"{url} unreachable: {error}")
            continue
        values = gauge_values(response.body)
        if response.status != 200:
            report.add("FAIL", f"metrics for stage {stage!r}", f"{url} returned {response.status}")
        elif stage not in values:
            report.add(
                "WARN",
                f"metrics for stage {stage!r}",
                f'no last_tick_processed_timestamp_seconds{{stage="{stage}"}} yet at {url}',
            )
        else:
            report.add("PASS", f"metrics for stage {stage!r}")


def check_ticks(report: Report) -> None:
    params = {"from": iso(SAMPLE_FROM), "to": iso(SAMPLE_TO), "source": "ctrader", "symbol": "EURUSD", "limit": "5"}
    response = http_get(f"{API_URL}/ticks", params)
    if response.status == 501:
        report.add("WARN", "/ticks", "not implemented yet (501)")
        return
    if response.status != 200:
        report.add("FAIL", "/ticks", f"status {response.status}: {response.body[:200]}")
        return
    try:
        items = as_list(as_dict(response.json(), "body")["items"], "items")
        require(len(items) <= 5, f"limit=5 returned {len(items)} items")
        for raw in items:
            item = as_dict(raw, "item")
            require(item.keys() >= TICK_FIELDS, f"item lacks fields: {item}")
            parse_ms(item["timestamp"])
    except (KeyError, TypeError, ValueError) as error:
        report.add("FAIL", "/ticks", f"unexpected response: {error}")
        return
    report.add("PASS", "/ticks", f"{len(items)} item(s) for a sample hour")
    invalid = http_get(f"{API_URL}/ticks", params | {"from": iso(SAMPLE_TO), "to": iso(SAMPLE_FROM)})
    report.add("PASS" if invalid.status == 422 else "FAIL", "/ticks rejects from >= to", f"status {invalid.status}")


def check_candles(report: Report) -> None:
    params = {"from": iso(SAMPLE_FROM), "to": iso(SAMPLE_TO), "source": "ctrader", "symbol": "EURUSD", "size": "5m"}
    response = http_get(f"{API_URL}/candles", params)
    if response.status == 501:
        report.add("WARN", "/candles", "not implemented yet (501)")
        return
    if response.status != 200:
        report.add("FAIL", "/candles", f"status {response.status}: {response.body[:200]}")
        return
    try:
        body = as_dict(response.json(), "body")
        require(body.keys() >= {"source", "symbol", "size", "items"}, "missing source, symbol, size or items")
        items = as_list(body["items"], "items")
        for raw in items:
            item = as_dict(raw, "item")
            require(item.keys() >= CANDLE_FIELDS, f"item lacks fields: {item}")
            require(parse_ms(item["at"]) % 300_000 == 0, f"5m candle not aligned: {item['at']}")
    except (KeyError, TypeError, ValueError) as error:
        report.add("FAIL", "/candles", f"unexpected response: {error}")
        return
    report.add("PASS", "/candles", f"{len(items)} candle(s) for a sample hour")
    invalid = http_get(f"{API_URL}/candles", params | {"size": "2m"})
    report.add("PASS" if invalid.status == 422 else "FAIL", "/candles rejects an unknown size", f"status {invalid.status}")


def main() -> None:
    report = Report()
    check_protected(report)
    decisions = check_decisions(report)
    check_services(report, decisions)
    if check_up(report):
        check_metrics(report, decisions)
        check_ticks(report)
        check_candles(report)
    print(f"\n{report.summary()}")  # noqa: T201
    sys.exit(1 if report.failed else 0)


if __name__ == "__main__":
    main()
