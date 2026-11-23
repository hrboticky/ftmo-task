"""Consumes ticks from Kafka and stores them in TimescaleDB.

Run with `python3 -m app.writer` (the `writer` compose service).
"""

import asyncio
import logging

from app.service import run_service
from app.settings import Settings

logger = logging.getLogger(__name__)


async def run(settings: Settings, stop: asyncio.Event) -> None:
    # Your code: consume settings.ticks_topic, write ticks to the schema in migrations/, call
    # app.metrics.mark_tick_processed("writer"), and return cleanly once `stop` is set.
    logger.warning("writer is not implemented yet")
    await stop.wait()


if __name__ == "__main__":
    run_service(run)
