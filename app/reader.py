from collections.abc import Awaitable

import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer
from prometheus_client import Counter
from pydantic import ValidationError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from app.metrics import LAST_TICK_PROCESSED, mark_tick_processed
from app.models import Tick
from app.service import run_service
from app.settings import Settings

logger = logging.getLogger(__name__)
invalid_messages = Counter("tick_reader_invalid_messages_total", "Invalid WebSocket messages discarded by the reader")


async def read_feed(
    settings: Settings,
    producer: AIOKafkaProducer,
    pending: asyncio.Queue[Awaitable[object]],
    capacity: asyncio.BoundedSemaphore,
) -> None:
    instruments = {(item.source, item.symbol) for item in settings.instruments}
    subscription = json.dumps(
        {
            "eventType": "wsTickStreamAddSymbols",
            "payload": {"symbols": [item.model_dump() for item in settings.instruments]},
        }
    )
    async for socket in connect(settings.simulator_url, proxy=None, close_timeout=2):
        try:
            await socket.send(subscription)
            async for raw in socket:
                try:
                    tick = Tick.model_validate_json(raw)
                except ValidationError:
                    invalid_messages.inc()
                    continue
                instrument = (tick.source, tick.payload.symbol)
                if instrument not in instruments:
                    invalid_messages.inc()
                    continue
                mark_tick_processed("ws_received")
                await capacity.acquire()
                delivery = await producer.send(
                    settings.ticks_topic,
                    tick.model_dump_json().encode(),
                    key=json.dumps(instrument, separators=(",", ":")).encode(),
                )
                pending.put_nowait(delivery)
        except ConnectionClosed as error:
            logger.warning("WebSocket disconnected, received close code: %s", error.rcvd.code if error.rcvd else None)


async def acknowledge(pending: asyncio.Queue[Awaitable[object]], capacity: asyncio.BoundedSemaphore) -> None:
    while True:
        delivery = await pending.get()
        try:
            await asyncio.shield(delivery)
            mark_tick_processed("reader")
        finally:
            pending.task_done()
            capacity.release()


async def run(settings: Settings, stop: asyncio.Event) -> None:
    LAST_TICK_PROCESSED.labels(stage="ws_received")
    LAST_TICK_PROCESSED.labels(stage="reader")
    pending: asyncio.Queue[Awaitable[object]] = asyncio.Queue()
    capacity = asyncio.BoundedSemaphore(2048)
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        enable_idempotence=True,
        linger_ms=5,
        request_timeout_ms=5000,
    )
    try:
        await producer.start()
        async with asyncio.TaskGroup() as tasks:
            receiver = tasks.create_task(read_feed(settings, producer, pending, capacity))
            confirmations = tasks.create_task(acknowledge(pending, capacity))
            await stop.wait()
            receiver.cancel()
            await asyncio.wait({receiver})
            try:
                await asyncio.wait_for(pending.join(), timeout=5)
            except TimeoutError:
                logger.warning("Kafka deliveries still pending at the reader drain deadline")
            confirmations.cancel()
    finally:
        try:
            await asyncio.wait_for(producer.stop(), timeout=3)
        except TimeoutError:
            logger.error("Kafka producer did not stop before the shutdown deadline")


if __name__ == "__main__":
    run_service(run)
