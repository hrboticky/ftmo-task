"""Part of the homework infrastructure. Don't change; see CONTRACT.md."""

from collections.abc import Callable
from typing import Final, TypedDict

import asyncio
from collections import Counter, defaultdict
from enum import StrEnum
from http import HTTPStatus
import json
import logging
import random
import signal
import time
from urllib.parse import urlsplit

from websockets.asyncio.server import ServerConnection, broadcast, serve
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode
from websockets.http11 import Request, Response

from simulator.config import Config, Mode, ReplayStart
from simulator.faults import FaultInjector
from simulator.sources import LiveSource, Tick, read_fixture

logger = logging.getLogger(__name__)

HOST: Final = "0.0.0.0"
WS_PATH: Final = "/market/v1/ticks/ws/"

# The pacing loop wakes up every TICK_INTERVAL_S and sends every message that is due by
# then. Sleeping 1/rate between single messages does not work in Python at 2k msg/s:
# the event loop's timer granularity makes the real rate far too low or far too high.
TICK_INTERVAL_S: Final = 0.001

type SubscriptionKey = tuple[str, str]


class ReplayState(StrEnum):
    WAITING = "waiting_for_subscriber"
    WAITING_FOR_START = "waiting_for_start"
    PLAYING = "playing"
    FINISHED = "finished"


class Stats(TypedDict):
    mode: Mode
    uptimeSeconds: float
    connectedClients: int
    replayState: ReplayState | None
    counts: dict[str, int]
    faults: dict[str, int]


class Simulator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._rng = random.Random(config.seed)
        self._subscribers: dict[SubscriptionKey, set[ServerConnection]] = defaultdict(set)
        self._connections: set[ServerConnection] = set()
        self._injector = FaultInjector(config.fault, self._rng, mutate_prices=config.mode is Mode.LIVE)
        self.counts: Counter[str] = Counter()
        self.replay_state = ReplayState.WAITING
        self._started_at = time.time()
        # A replay starts only when every fixture instrument has a subscriber, so a system
        # with one reader per source doesn't miss the start of the replay.
        self._replay_instruments: frozenset[SubscriptionKey] = (
            frozenset((tick["source"], tick["payload"]["symbol"]) for tick in read_fixture(config.fixture_path))
            if config.mode is Mode.REPLAY
            else frozenset()
        )
        self._all_subscribed = asyncio.Event()
        self._start_requested = asyncio.Event()

    async def handle(self, connection: ServerConnection) -> None:
        self._connections.add(connection)
        self.counts["connections"] += 1
        logger.info("client connected: %s", connection.remote_address)
        disconnect = (
            asyncio.create_task(self._scheduled_disconnect(connection)) if self.config.fault.disconnect_every_s else None
        )
        try:
            async for raw in connection:
                self._on_client_message(connection, raw)
        except ConnectionClosed:
            pass
        finally:
            if disconnect is not None:
                disconnect.cancel()
            self._connections.discard(connection)
            for subscribers in self._subscribers.values():
                subscribers.discard(connection)
            logger.info("client disconnected: %s", connection.remote_address)

    def _on_client_message(self, connection: ServerConnection, raw: str | bytes) -> None:
        try:
            message = json.loads(raw)
            event = message["eventType"]
            symbols = message["payload"]["symbols"]
            keys = [(str(symbol["source"]), str(symbol["symbol"])) for symbol in symbols]
        except ValueError, KeyError, TypeError:
            logger.warning("ignoring invalid client message: %.200r", raw)
            return
        if event == "wsTickStreamAddSymbols":
            for key in keys:
                self._subscribers[key].add(connection)
            if not self._unsubscribed():
                self._all_subscribed.set()
            logger.info("subscribed %s to %d symbols", connection.remote_address, len(keys))
        elif event == "wsTickStreamRemoveSymbols":
            for key in keys:
                self._subscribers[key].discard(connection)
        else:
            logger.warning("ignoring unknown client event %r", event)

    async def _scheduled_disconnect(self, connection: ServerConnection) -> None:
        every = self.config.fault.disconnect_every_s
        await asyncio.sleep(self._rng.uniform(0.5 * every, 1.5 * every))
        self.counts["disconnects"] += 1
        if self._rng.random() < 0.5:
            logger.info("fault: closing connection %s", connection.remote_address)
            await connection.close(code=CloseCode.GOING_AWAY, reason="server going away")
        else:
            logger.info("fault: dropping connection %s without a close frame", connection.remote_address)
            connection.transport.abort()

    def _send_tick(self, tick: Tick) -> None:
        self.counts["ticks_sent"] += 1
        if subscribers := self._subscribers.get((tick["source"], tick["payload"]["symbol"])):
            broadcast(subscribers, json.dumps(tick, separators=(",", ":")))

    def _emit(self, tick: Tick, now: float) -> None:
        for ready in self._injector.process(tick, now):
            self._send_tick(ready)
        malformed = self._injector.maybe_malformed()
        subscribers = self._subscribers.get((tick["source"], tick["payload"]["symbol"]))
        if malformed is not None and subscribers:
            # Sent to the same clients as the tick, so the malformed rate is relative to
            # what each client actually receives.
            broadcast(subscribers, malformed)

    async def run_live(self) -> None:
        source = LiveSource(self.config.symbol_count, self._rng)
        await self._pace(lambda: source.next_tick(time.time_ns() // 1_000_000))

    async def run_replay(self) -> None:
        path = self.config.fixture_path
        while True:
            if self.config.replay_start is ReplayStart.MANUAL:
                self.replay_state = ReplayState.WAITING_FOR_START
                logger.info("replay: waiting for GET /replay/start")
                await self._start_requested.wait()
            else:
                self.replay_state = ReplayState.WAITING
                logger.info("replay: waiting for subscriptions to all %d instruments", len(self._replay_instruments))
                await self._all_subscribed.wait()
            self.replay_state = ReplayState.PLAYING
            logger.info("replay: playing %s at %.0f msg/s", path, self.config.rate)
            await self._pace(read_fixture(path).__next__, tick_time=True)
            for tick in self._injector.flush():
                self._send_tick(tick)
            self.replay_state = ReplayState.FINISHED
            logger.info("replay: finished after %d ticks", self.counts["ticks_sent"])
            if not self.config.replay_loop:
                return
            self._start_requested.clear()
            self._all_subscribed.clear()
            if not self._unsubscribed():
                self._all_subscribed.set()

    async def _pace(self, next_tick: Callable[[], Tick], *, tick_time: bool = False) -> None:
        """Send ticks at the configured rate until next_tick raises StopIteration.

        With tick_time, fault delays are measured on the ticks' own timestamps (replay),
        otherwise on the event loop's clock (live).
        """
        loop = asyncio.get_running_loop()
        start = last = loop.time()
        budget = 0.0
        stream_time = 0.0
        while True:
            now = loop.time()
            burst = self._injector.burst_active(now - start)
            rate = self.config.fault.burst_rate if burst else self.config.rate
            budget += (now - last) * rate
            last = now
            due = int(budget)
            budget -= due
            for _ in range(due):
                try:
                    tick = next_tick()
                except StopIteration:
                    return
                if tick_time:
                    stream_time = max(stream_time, tick["timestampMsc"] / 1000)
                    self._emit(tick, stream_time)
                    for ready in self._injector.due(stream_time):
                        self._send_tick(ready)
                else:
                    self._emit(tick, now)
            if not tick_time:
                for ready in self._injector.due(now):
                    self._send_tick(ready)
            await asyncio.sleep(TICK_INTERVAL_S)

    def request_start(self) -> tuple[HTTPStatus, str]:
        if self.config.mode is not Mode.REPLAY or self.config.replay_start is not ReplayStart.MANUAL:
            return HTTPStatus.CONFLICT, "replay is not in manual start mode\n"
        if self.replay_state is not ReplayState.WAITING_FOR_START:
            return HTTPStatus.CONFLICT, f"replay is {self.replay_state.value}\n"
        if missing := self._unsubscribed():
            return HTTPStatus.CONFLICT, f"waiting for subscriptions to: {', '.join(missing)}\n"
        self._start_requested.set()
        return HTTPStatus.OK, "replay started\n"

    def _unsubscribed(self) -> list[str]:
        """Replay instruments without a subscriber, as `source symbol`."""
        return [
            f"{source} {symbol}"
            for source, symbol in sorted(self._replay_instruments)
            if not self._subscribers.get((source, symbol))
        ]

    def stats(self) -> Stats:
        return {
            "mode": self.config.mode,
            "uptimeSeconds": round(time.time() - self._started_at, 1),
            "connectedClients": len(self._connections),
            "replayState": self.replay_state if self.config.mode is Mode.REPLAY else None,
            "counts": dict(self.counts),
            "faults": dict(self._injector.counts),
        }

    def process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        path = urlsplit(request.path).path
        if path == WS_PATH:
            return None
        if path == "/healthz":
            return connection.respond(HTTPStatus.OK, "ok\n")
        if path == "/replay/start":
            # A GET, because the WebSocket server only accepts GET requests.
            status, body = self.request_start()
            return connection.respond(status, body)
        if path == "/stats":
            response = connection.respond(HTTPStatus.OK, json.dumps(self.stats(), indent=2) + "\n")
            # Headers is a multidict: assignment would add a second Content-Type.
            del response.headers["Content-Type"]
            response.headers["Content-Type"] = "application/json"
            return response
        return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")


async def run(config: Config) -> None:
    simulator = Simulator(config)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stop.set)

    async with serve(
        simulator.handle,
        HOST,
        config.port,
        process_request=simulator.process_request,
        max_queue=None,
    ):
        logger.info(
            "tick simulator listening on ws://%s:%d%s (mode=%s, rate=%.0f/s)",
            HOST,
            config.port,
            WS_PATH,
            config.mode,
            config.rate,
        )
        producer = asyncio.create_task(simulator.run_live() if config.mode is Mode.LIVE else simulator.run_replay())
        await stop.wait()
        producer.cancel()
    logger.info("tick simulator stopped")
