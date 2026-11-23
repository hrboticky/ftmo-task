from collections.abc import AsyncIterator
from typing import Final

import asyncio
from contextlib import asynccontextmanager
from http import HTTPStatus
import json
from pathlib import Path
import urllib.error
import urllib.request

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from simulator.config import Config, FaultConfig, Mode, ReplayStart
from simulator.server import WS_PATH, ReplayState, Simulator
from simulator.sources import Tick

SUBSCRIBE: Final = {
    "eventType": "wsTickStreamAddSymbols",
    "payload": {"symbols": [{"symbol": "EURUSD", "source": "ctrader"}]},
}
REPLAY_START_MSC: Final = 1_792_713_600_000


@asynccontextmanager
async def running(config: Config) -> AsyncIterator[tuple[Simulator, str]]:
    simulator = Simulator(config)
    async with serve(simulator.handle, "127.0.0.1", 0, process_request=simulator.process_request) as server:
        port = server.sockets[0].getsockname()[1]
        task = asyncio.create_task(simulator.run_live() if config.mode is Mode.LIVE else simulator.run_replay())
        yield simulator, f"ws://127.0.0.1:{port}{WS_PATH}"
        task.cancel()


def write_fixture(path: Path, count: int, step_ms: int = 1_000) -> list[Tick]:
    ticks: list[Tick] = [
        {
            "messageId": f"{index:08X}",
            "timestampMsc": REPLAY_START_MSC + index * step_ms,
            "eventType": "symbolTick",
            "source": "ctrader",
            "mayBeOutOfSequence": False,
            "payload": {"symbol": "EURUSD", "bid": 1.1, "ask": 1.10004},
        }
        for index in range(count)
    ]
    path.write_text("".join(json.dumps(tick) + "\n" for tick in ticks))
    return ticks


async def http_get(url: str) -> tuple[int, str]:
    def fetch() -> tuple[int, str]:
        try:
            with urllib.request.urlopen(url) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()

    return await asyncio.to_thread(fetch)


async def test_live_sends_only_subscribed_symbols_at_the_configured_rate() -> None:
    received: list[Tick] = []
    async with (
        running(Config(mode=Mode.LIVE, rate=2_000, symbol_count=6)) as (simulator, url),
        connect(url) as websocket,
    ):
        await websocket.send(json.dumps(SUBSCRIBE))
        loop = asyncio.get_running_loop()
        end = loop.time() + 1.0
        while (remaining := end - loop.time()) > 0:
            try:
                received.append(json.loads(await asyncio.wait_for(websocket.recv(), remaining)))
            except TimeoutError:
                break
    assert {(tick["source"], tick["payload"]["symbol"]) for tick in received} == {("ctrader", "EURUSD")}
    # ticks_sent counts the whole generated universe, not just the subscribed symbol.
    assert 1_600 <= simulator.counts["ticks_sent"] <= 2_400


async def test_replay_delivers_every_fixture_tick(tmp_path: Path) -> None:
    fixture_path = tmp_path / "ticks.jsonl"
    ticks = write_fixture(fixture_path, 200)
    config = Config(
        mode=Mode.REPLAY,
        rate=1_000,
        fixture_path=fixture_path,
        fault=FaultConfig(duplicate_rate=0.1, out_of_order_rate=0.1),
    )
    expected = {tick["messageId"] for tick in ticks}
    seen: set[str] = set()
    async with running(config) as (simulator, url):
        async with connect(url) as websocket:
            await websocket.send(json.dumps(SUBSCRIBE))
            while seen != expected:
                message = json.loads(await asyncio.wait_for(websocket.recv(), 5))
                seen.add(message["messageId"])
        for _ in range(50):
            if simulator.replay_state is ReplayState.FINISHED:
                break
            await asyncio.sleep(0.1)
        assert simulator.stats()["replayState"] is ReplayState.FINISHED


async def test_manual_replay_waits_for_the_start_request(tmp_path: Path) -> None:
    fixture_path = tmp_path / "ticks.jsonl"
    write_fixture(fixture_path, 50)
    config = Config(mode=Mode.REPLAY, rate=1_000, fixture_path=fixture_path, replay_start=ReplayStart.MANUAL)
    async with running(config) as (simulator, url):
        start_url = url.replace("ws://", "http://").replace(WS_PATH, "/replay/start")
        status, _ = await http_get(start_url)
        assert status == HTTPStatus.CONFLICT  # nobody has subscribed yet
        async with connect(url) as websocket:
            await websocket.send(json.dumps(SUBSCRIBE))
            await asyncio.sleep(0.2)
            assert simulator.replay_state is ReplayState.WAITING_FOR_START
            assert simulator.counts["ticks_sent"] == 0
            status, _ = await http_get(start_url)
            assert status == HTTPStatus.OK
            first = json.loads(await asyncio.wait_for(websocket.recv(), 5))
        assert first["messageId"] == "00000000"


async def test_manual_replay_starts_only_when_every_fixture_instrument_is_subscribed(tmp_path: Path) -> None:
    fixture_path = tmp_path / "ticks.jsonl"
    write_fixture(fixture_path, 50)
    with fixture_path.open("a") as fixture:
        fixture.write(json.dumps(write_fixture(tmp_path / "other.jsonl", 1)[0] | {"source": "mt5"}) + "\n")
    config = Config(mode=Mode.REPLAY, rate=1_000, fixture_path=fixture_path, replay_start=ReplayStart.MANUAL)
    mt5 = {"eventType": "wsTickStreamAddSymbols", "payload": {"symbols": [{"symbol": "EURUSD", "source": "mt5"}]}}
    async with running(config) as (_, url), connect(url) as websocket:
        start_url = url.replace("ws://", "http://").replace(WS_PATH, "/replay/start")
        await websocket.send(json.dumps(SUBSCRIBE))
        await asyncio.sleep(0.2)
        status, body = await http_get(start_url)
        assert (status, body) == (HTTPStatus.CONFLICT, "waiting for subscriptions to: mt5 EURUSD\n")
        await websocket.send(json.dumps(mt5))
        await asyncio.sleep(0.2)
        status, _ = await http_get(start_url)
        assert status == HTTPStatus.OK


async def test_replay_on_subscribe_waits_for_every_fixture_instrument(tmp_path: Path) -> None:
    fixture_path = tmp_path / "ticks.jsonl"
    write_fixture(fixture_path, 50)
    with fixture_path.open("a") as fixture:
        fixture.write(json.dumps(write_fixture(tmp_path / "other.jsonl", 1)[0] | {"source": "mt5"}) + "\n")
    config = Config(mode=Mode.REPLAY, rate=1_000, fixture_path=fixture_path)
    async with running(config) as (simulator, url), connect(url) as websocket:
        await websocket.send(json.dumps(SUBSCRIBE))
        await asyncio.sleep(0.2)
        assert simulator.replay_state is ReplayState.WAITING
        assert simulator.counts["ticks_sent"] == 0


async def test_replay_delays_are_measured_in_tick_time(tmp_path: Path) -> None:
    # Ticks one minute apart; a delay of up to 30 s of tick time is at most one tick late.
    fixture_path = tmp_path / "ticks.jsonl"
    ticks = write_fixture(fixture_path, 300, step_ms=60_000)
    config = Config(
        mode=Mode.REPLAY,
        rate=2_000,
        fixture_path=fixture_path,
        fault=FaultConfig(out_of_order_rate=0.2, out_of_order_max_delay_ms=30_000),
    )
    order = {tick["messageId"]: index for index, tick in enumerate(ticks)}
    received: list[int] = []
    async with running(config) as (_, url), connect(url) as websocket:
        await websocket.send(json.dumps(SUBSCRIBE))
        while len(received) < len(ticks):
            message = json.loads(await asyncio.wait_for(websocket.recv(), 5))
            received.append(order[message["messageId"]])
    lateness = [max(received[: position + 1]) - index for position, index in enumerate(received)]
    assert received != sorted(received)
    assert max(lateness) <= 1
