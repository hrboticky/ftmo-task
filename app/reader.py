"""Reads ticks from the simulator's WebSocket and produces them to Kafka.

Run with `python3 -m app.reader` (the `reader` compose service).
"""

import asyncio
import logging

from app.service import run_service
from app.settings import Settings

logger = logging.getLogger(__name__)


async def run(settings: Settings, stop: asyncio.Event) -> None:
    # Your code: connect to settings.simulator_url, subscribe to settings.instruments, parse
    # messages with app.models.Tick, produce them to Kafka, call
    # app.metrics.mark_tick_processed("reader"), and return cleanly once `stop` is set.
    logger.warning("reader is not implemented yet")
    await stop.wait()


if __name__ == "__main__":
    run_service(run)
