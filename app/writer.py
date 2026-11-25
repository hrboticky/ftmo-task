from collections.abc import Awaitable, Callable, Sequence
from typing import TypedDict, cast

import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError
from aiokafka.structs import ConsumerRecord, TopicPartition
from prometheus_client import Counter
from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.metrics import LAST_TICK_PROCESSED, mark_tick_processed
from app.models import Tick
from app.service import run_service
from app.settings import Settings
from app.storage import checkpoints, create_engine, ticks

logger = logging.getLogger(__name__)
invalid_messages = Counter("tick_writer_invalid_messages_total", "Invalid Kafka records skipped by the writer")

type Record = ConsumerRecord[bytes | None, bytes]
type CommitOffsets = Callable[[dict[TopicPartition, int]], Awaitable[None]]
type SaveBatch = Callable[[TopicPartition, Sequence[Record]], Awaitable[int]]


class TickRow(TypedDict):
    source: str
    symbol: str
    timestamp: datetime
    message_id: str
    bid: float | None
    ask: float | None
    may_be_out_of_sequence: bool
    prior_event_time: datetime | None


@dataclass(frozen=True, slots=True)
class PreparedBatch:
    rows: list[TickRow]
    last_offset: int
    max_event_time: datetime | None
    invalid_offsets: list[int]


def prepare_rows(records: Sequence[Record], last_offset: int, max_event_time: datetime | None) -> PreparedBatch:
    rows: list[TickRow] = []
    invalid_offsets: list[int] = []
    for record in sorted(records, key=lambda item: item.offset):
        if record.offset <= last_offset:
            continue
        last_offset = record.offset
        try:
            tick = Tick.model_validate_json(record.value)
        except ValidationError:
            invalid_offsets.append(last_offset)
            continue
        timestamp = tick.timestamp
        rows.append(
            TickRow(
                source=tick.source,
                symbol=tick.payload.symbol,
                timestamp=timestamp,
                message_id=tick.message_id,
                bid=tick.payload.bid,
                ask=tick.payload.ask,
                may_be_out_of_sequence=tick.may_be_out_of_sequence,
                prior_event_time=max_event_time,
            )
        )
        if max_event_time is None or timestamp > max_event_time:
            max_event_time = timestamp
    return PreparedBatch(rows, last_offset, max_event_time, invalid_offsets)


async def store_batch(engine: AsyncEngine, partition: TopicPartition, records: Sequence[Record]) -> int:
    checkpoint_key = checkpoints.c.topic == partition.topic, checkpoints.c.partition == partition.partition
    async with engine.begin() as connection:
        await connection.execute(
            insert(checkpoints)
            .values(topic=partition.topic, partition=partition.partition, last_offset=-1)
            .on_conflict_do_nothing()
        )
        state = (await connection.execute(select(checkpoints).where(*checkpoint_key).with_for_update())).one()
        last_offset = cast("int", state.last_offset)
        batch = prepare_rows(records, last_offset, state.max_event_time)
        if batch.rows:
            await connection.execute(insert(ticks).on_conflict_do_nothing(), [dict(row) for row in batch.rows])
        if batch.last_offset > last_offset:
            await connection.execute(
                update(checkpoints)
                .where(*checkpoint_key)
                .values(last_offset=batch.last_offset, max_event_time=batch.max_event_time)
            )
    if batch.rows:
        mark_tick_processed("writer")
    if batch.invalid_offsets:
        invalid_messages.inc(len(batch.invalid_offsets))
        logger.warning(
            "Skipped %d invalid records in %s:%d from offset %d to %d",
            len(batch.invalid_offsets),
            partition.topic,
            partition.partition,
            batch.invalid_offsets[0],
            batch.invalid_offsets[-1],
        )
    return batch.last_offset + 1


async def persist_and_acknowledge(
    partition: TopicPartition,
    records: Sequence[Record],
    save: SaveBatch,
    commit: CommitOffsets,
    stop: asyncio.Event,
) -> None:
    backoff = 0.1
    while not stop.is_set():
        try:
            next_offset = await save(partition, records)
        except (DBAPIError, OSError, TimeoutError) as error:
            sqlstate = None
            if isinstance(error, DBAPIError) and not error.connection_invalidated:
                sqlstate = getattr(error.orig, "sqlstate", None)
                if sqlstate and not sqlstate.startswith(("08", "57", "40", "53")):
                    raise
            logger.warning("Database batch failed, retrying: %s, SQLSTATE=%s", type(error).__name__, sqlstate)
            try:
                await asyncio.wait_for(stop.wait(), backoff)
            except TimeoutError:
                backoff = min(backoff * 2, 2.0)
        else:
            await commit({partition: next_offset})
            return


async def fetch_batch(consumer: AIOKafkaConsumer) -> dict[TopicPartition, list[Record]]:
    batch: dict[TopicPartition, list[Record]] = await consumer.getmany(timeout_ms=100, max_records=500)
    count = sum(map(len, batch.values()))
    if not count:
        return batch
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.05
    while count < 500:
        remaining_ms = int((deadline - loop.time()) * 1000)
        if remaining_ms <= 0:
            break
        extra: dict[TopicPartition, list[Record]] = await consumer.getmany(timeout_ms=remaining_ms, max_records=500 - count)
        if not extra:
            break
        for partition, records in extra.items():
            batch.setdefault(partition, []).extend(records)
            count += len(records)
    return batch


async def consume(settings: Settings, engine: AsyncEngine, stop: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(
        settings.ticks_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="candlekeeper-writer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=5000,
    )

    async def save(partition: TopicPartition, records: Sequence[Record]) -> int:
        return await store_batch(engine, partition, records)

    try:
        await consumer.start()
        while not stop.is_set():
            batch = await fetch_batch(consumer)
            for partition, records in batch.items():
                if records and partition in consumer.assignment():
                    await persist_and_acknowledge(partition, records, save, consumer.commit, stop)
    finally:
        await consumer.stop()


async def run(settings: Settings, stop: asyncio.Event) -> None:
    LAST_TICK_PROCESSED.labels(stage="writer")
    engine = create_engine(settings)
    try:
        while not stop.is_set():
            try:
                await consume(settings, engine, stop)
            except CommitFailedError as error:
                logger.warning("Consumer lost its assignment, rejoining: %s", error)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    run_service(run)
