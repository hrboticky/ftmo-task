"""The staleness gauge that CONTRACT.md requires."""

from typing import Final

import time

from prometheus_client import Gauge

LAST_TICK_PROCESSED: Final = Gauge(
    "last_tick_processed_timestamp_seconds",
    "Wall-clock Unix time when this stage last processed a tick",
    ["stage"],
)


def mark_tick_processed(stage: str) -> None:
    """Call whenever a stage has processed a tick (or a batch of ticks)."""
    LAST_TICK_PROCESSED.labels(stage=stage).set(time.time())
