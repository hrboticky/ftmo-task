from collections.abc import Callable, Iterator

import asyncio
from collections import defaultdict
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

from aiokafka import AIOKafkaProducer
from httpx2 import AsyncClient
import pytest
from websockets.asyncio.client import connect

from app.models import Tick, TickPayload
from checks import verify
from checks.common import Report, as_dict, as_list, gauge_values, http_get, iso
from simulator.generate_fixture import END_AT, FIXTURE_SEED, START_AT, generate
from simulator.sources import CORE_INSTRUMENTS

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parent.parent
PORTS = ("API_PORT", "SIMULATOR_PORT", "KAFKA_PORT", "POSTGRES_PORT", "READER_METRICS_PORT", "WRITER_METRICS_PORT")
SIZES = ("1m", "5m", "15m", "1h", "12h", "1d", "1w", "1M", "1y")
PRAGUE = ZoneInfo("Europe/Prague")


@dataclass(frozen=True)
class Stack:
    env: dict[str, str]
    override: Path

    def url(self, port: str) -> str:
        return f"http://localhost:{self.env[port]}"

    def compose(self, *args: str) -> str:
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", str(self.override), *args],
            cwd=ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Compose {args} failed:\n{result.stdout}\n{result.stderr}")
        return result.stdout


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[Stack]:
    with ExitStack() as sockets:
        ports: dict[str, str] = {}
        for name in PORTS:
            listener = sockets.enter_context(socket.socket())
            listener.bind(("127.0.0.1", 0))
            ports[name] = str(listener.getsockname()[1])
    override = tmp_path / "compose.yml"
    override.write_text(
        "services:\n"
        "  reader:\n"
        "    environment:\n"
        "      TZ: UTC\n"
        "      ws_proxy: http://127.0.0.1:1\n"
        '      no_proxy: ""\n'
        '      NO_PROXY: ""\n'
        "  writer:\n    environment:\n      TZ: UTC\n"
        "  api:\n    environment:\n      TZ: UTC\n",
        encoding="utf-8",
    )
    env = {key: value for key, value in os.environ.items() if not key.startswith("FAULT_")}
    instance = Stack(env | ports | {"COMPOSE_PROJECT_NAME": f"ftmo-it-{uuid4().hex[:12]}", "REPLAY_LOOP": "false"}, override)
    try:
        yield instance
    finally:
        instance.compose("down", "--volumes", "--remove-orphans")


def eventually(condition: Callable[[], bool], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except OSError:
            time.sleep(0.2)
            continue
        time.sleep(0.2)
    pytest.fail(f"Condition did not become true within {timeout} s")


def gauge(stack: Stack, port: str, stage: str) -> float:
    return gauge_values(http_get(f"{stack.url(port)}/metrics", timeout=2).body).get(stage, 0)


def replay_stats(stack: Stack) -> dict[str, object]:
    return as_dict(http_get(f"{stack.url('SIMULATOR_PORT')}/stats", timeout=2).json(), "simulator stats")


def tick_count(stack: Stack) -> int:
    return int(
        stack.compose(
            "exec", "-T", "timescaledb", "psql", "-U", "candlekeeper", "-d", "ticks", "-Atc", "SELECT count(*) FROM ticks"
        )
    )


def test_replay_survives_writer_and_database_restarts(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    stack.env.update(MODE="replay", RATE="2000", FAULTS="replay", REPLAY_START="manual")
    stack.compose("up", "-d", "--build")
    api = stack.url("API_PORT")
    simulator = stack.url("SIMULATOR_PORT")
    eventually(lambda: http_get(f"{api}/healthz", timeout=2).status == 200)
    eventually(lambda: http_get(f"{simulator}/replay/start", timeout=2).status == 200)
    eventually(lambda: gauge(stack, "WRITER_METRICS_PORT", "writer") > 0)
    assert replay_stats(stack)["replayState"] == "playing"
    stack.compose("restart", "-t", "0", "writer")
    assert replay_stats(stack)["replayState"] == "playing"
    stack.compose("restart", "-t", "1", "timescaledb")
    eventually(lambda: replay_stats(stack)["replayState"] == "finished", timeout=90)
    eventually(lambda: http_get(f"{api}/healthz", timeout=2).status == 200)
    expected_count = len(generate(FIXTURE_SEED))
    eventually(lambda: tick_count(stack) == expected_count, timeout=90)
    monkeypatch.setattr(verify, "API_URL", api)
    report = Report()
    verify.compare(report)
    assert not report.failed
    assert_replay_candles(stack)
    assert_late_tick_keeps_sealed_minute_stable(stack, expected_count)
    assert_full_candle_range(stack)


async def sample_live_freshness(stack: Stack) -> None:
    uri = stack.url("SIMULATOR_PORT").replace("http://", "ws://", 1) + "/market/v1/ticks/ws/"
    subscription = {"eventType": "wsTickStreamAddSymbols", "payload": {"symbols": [{"source": "mt5", "symbol": "EURUSD"}]}}
    async with connect(uri) as ws, AsyncClient(base_url=stack.url("API_PORT"), timeout=2) as client:
        await ws.send(json.dumps(subscription))
        async with asyncio.timeout(5):
            samples = [Tick.model_validate_json(await ws.recv()) for _ in range(8)]
        start = samples[0].timestamp
        end = samples[-1].timestamp + timedelta(milliseconds=1)
        params = {"source": "mt5", "symbol": "EURUSD", "from": iso(start), "to": iso(end)}
        outstanding = {sample.message_id: sample.timestamp for sample in samples}
        async with asyncio.timeout(2):
            while outstanding:
                response = await client.get("/ticks", params=params)
                response.raise_for_status()
                items = as_list(as_dict(response.json(), "live ticks")["items"], "items")
                now = datetime.now(UTC)
                for raw in items:
                    item = as_dict(raw, "live tick")
                    sent_at = outstanding.pop(str(item["messageId"]), None)
                    if sent_at is not None:
                        assert now - sent_at < timedelta(seconds=2)
                await asyncio.sleep(0.02)


def test_live_freshness_and_stage_staleness_during_database_outage(stack: Stack) -> None:
    stack.env.update(MODE="live", RATE="2000", FAULTS="none", SYMBOL_COUNT="6")
    stack.compose("up", "-d", "--build")
    eventually(lambda: gauge(stack, "WRITER_METRICS_PORT", "writer") > 0)
    asyncio.run(sample_live_freshness(stack))

    stack.compose("stop", "timescaledb")
    assert http_get(f"{stack.url('API_PORT')}/healthz", timeout=5).status == 503
    db = gauge(stack, "WRITER_METRICS_PORT", "writer")
    ws = gauge(stack, "READER_METRICS_PORT", "ws_received")
    kafka = gauge(stack, "READER_METRICS_PORT", "reader")
    eventually(
        lambda: (
            gauge(stack, "READER_METRICS_PORT", "ws_received") > ws + 1
            and gauge(stack, "READER_METRICS_PORT", "reader") > kafka + 1
        )
    )
    assert gauge(stack, "WRITER_METRICS_PORT", "writer") == db
    stack.compose("start", "timescaledb")
    eventually(lambda: gauge(stack, "WRITER_METRICS_PORT", "writer") > db)


def candle_items(
    stack: Stack, size: str, start: str, end: str, *, source: str = "dxtrade", symbol: str = "BTCUSD"
) -> list[object]:
    response = http_get(
        f"{stack.url('API_PORT')}/candles",
        {"source": source, "symbol": symbol, "size": size, "from": start, "to": end},
        timeout=10,
    )
    assert response.status == 200, response.body
    return as_list(as_dict(response.json(), "candles")["items"], "items")


def assert_full_candle_range(stack: Stack) -> None:
    start = datetime(2031, 1, 1, tzinfo=UTC)
    stack.compose(
        "exec",
        "-T",
        "timescaledb",
        "psql",
        "-U",
        "candlekeeper",
        "-d",
        "ticks",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        """
        INSERT INTO ticks (source, symbol, timestamp, message_id, bid, may_be_out_of_sequence)
        SELECT 'dxtrade', 'BTCUSD', timestamptz '2031-01-01T00:00Z' + i * interval '20 seconds',
               upper(lpad(to_hex(i), 8, '0')), i, false
        FROM generate_series(0, 59999) AS i
        """,
    )
    items = candle_items(stack, "1m", iso(start), iso(start + timedelta(minutes=20_000)))
    assert len(items) == 20_000
    for i, row in enumerate(items):
        opening = 3 * i
        assert row == {
            "at": iso(start + timedelta(minutes=i)),
            "open": opening,
            "high": opening + 2,
            "low": opening,
            "close": opening + 2,
        }


def reference_bucket(timestamp: datetime, size: str) -> datetime:
    seconds = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}.get(size)
    if seconds is not None:
        return datetime.fromtimestamp(int(timestamp.timestamp()) // seconds * seconds, UTC)
    local = timestamp.astimezone(PRAGUE)
    date = local.date()
    hour = 0
    match size:
        case "12h":
            hour = 12 if local.hour >= 12 else 0
        case "1w":
            date -= timedelta(days=date.weekday())
        case "1M":
            date = date.replace(day=1)
        case "1y":
            date = date.replace(month=1, day=1)
    return datetime(date.year, date.month, date.day, hour, tzinfo=PRAGUE).astimezone(UTC)


def assert_replay_candles(stack: Stack) -> None:
    fixture = [Tick.model_validate(raw) for raw in generate(FIXTURE_SEED)]
    for size in SIZES:
        buckets: dict[tuple[str, str], dict[datetime, list[tuple[int, str, float]]]] = defaultdict(lambda: defaultdict(list))
        for tick in fixture:
            if tick.payload.bid is not None:
                key = (tick.source, tick.payload.symbol)
                at = reference_bucket(tick.timestamp, size)
                buckets[key][at].append((tick.timestamp_msc, tick.message_id, tick.payload.bid))
        start = iso(reference_bucket(START_AT, size))
        for instrument in CORE_INSTRUMENTS:
            source, symbol = instrument.source, instrument.symbol
            expected: list[dict[str, str | float]] = []
            for at, prices in sorted(buckets[(source, symbol)].items()):
                values = [price for _, _, price in sorted(prices)]
                expected.append({"at": iso(at), "open": values[0], "high": max(values), "low": min(values), "close": values[-1]})
            actual = candle_items(stack, size, start, iso(END_AT), source=source, symbol=symbol)
            assert actual == expected, (size, source, symbol)


async def publish_ticks(stack: Stack, ticks: list[tuple[int, datetime, float | None]]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=f"localhost:{stack.env['KAFKA_PORT']}", enable_idempotence=True)
    try:
        await producer.start()
        for message_number, timestamp, bid in ticks:
            tick = Tick(
                message_id=f"{message_number:08X}",
                timestamp_msc=int(timestamp.timestamp() * 1000),
                event_type="symbolTick",
                source="dxtrade",
                may_be_out_of_sequence=message_number == 6,
                payload=TickPayload(symbol="BTCUSD", bid=bid, ask=99.0),
            )
            await producer.send_and_wait("ticks.raw", tick.model_dump_json().encode(), key=b'["dxtrade","BTCUSD"]')
    finally:
        await producer.stop()


def assert_late_tick_keeps_sealed_minute_stable(stack: Stack, replay_count: int) -> None:
    minute = datetime(2027, 10, 31, 1, tzinfo=UTC)
    bounds = iso(minute), iso(minute + timedelta(minutes=1))
    at = minute + timedelta(seconds=10)
    asyncio.run(
        publish_ticks(
            stack,
            [
                (1, at, 10.0),
                (2, at, 15.0),
                (3, minute + timedelta(seconds=30), None),
                (4, minute + timedelta(minutes=1), 11.0),
                (5, minute + timedelta(minutes=26), 12.0),
            ],
        )
    )
    eventually(lambda: tick_count(stack) == replay_count + 5)
    expected = [{"at": iso(minute), "open": 10.0, "high": 15.0, "low": 10.0, "close": 15.0}]
    assert candle_items(stack, "1m", *bounds) == expected
    assert candle_items(stack, "1m", iso(minute), iso(minute + timedelta(seconds=5))) == expected
    asyncio.run(publish_ticks(stack, [(6, minute + timedelta(seconds=20), 80.0)]))
    eventually(lambda: tick_count(stack) == replay_count + 6)
    assert candle_items(stack, "1m", *bounds) == expected
    day_start = datetime(2027, 10, 30, 22, tzinfo=UTC)
    assert candle_items(stack, "1d", iso(day_start), iso(day_start + timedelta(hours=25))) == [
        {"at": iso(day_start), "open": 10.0, "high": 80.0, "low": 10.0, "close": 12.0}
    ]
