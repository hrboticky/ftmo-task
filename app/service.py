"""Runs a long-lived async service until SIGTERM or SIGINT."""

from collections.abc import Awaitable, Callable

import asyncio
import logging
import signal

from prometheus_client import start_http_server

from app.settings import Settings

type Service = Callable[[Settings, asyncio.Event], Awaitable[None]]


async def _run(service: Service, settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stop.set)
    await service(settings, stop)


def run_service(service: Service) -> None:
    settings = Settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # /metrics on a background thread; the API serves its own instead.
    start_http_server(settings.metrics_port)
    asyncio.run(_run(service, settings))
