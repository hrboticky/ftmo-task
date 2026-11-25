from collections.abc import Sequence
from typing import cast

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock

from aiokafka.errors import CommitFailedError
from aiokafka.structs import ConsumerRecord, TopicPartition
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from app.writer import Record, persist_and_acknowledge, prepare_rows, store_batch


def record_at(offset: int, event_ms: int) -> Record:
    value = (
        f'{{"messageId":"{offset:08X}","timestampMsc":{event_ms},"eventType":"symbolTick","source":"mt5",'
        '"mayBeOutOfSequence":false,"payload":{"symbol":"EURUSD","bid":1.1,"ask":1.2}}'
    ).encode()
    return ConsumerRecord("ticks.raw", 0, offset, 0, 0, None, value, None, -1, len(value), ())


async def test_kafka_ack_failure_replays_committed_batch_safely() -> None:
    records = [record_at(4, 10_000), record_at(5, 5000)]
    partition = TopicPartition("ticks.raw", 0)
    stop = asyncio.Event()
    last_offset = 3
    max_event_time: datetime | None = None
    durable_prices: list[float | None] = []
    acknowledgements: list[dict[TopicPartition, int]] = []

    async def save(topic: TopicPartition, batch: Sequence[Record]) -> int:
        nonlocal last_offset, max_event_time
        prepared = prepare_rows(batch, last_offset, max_event_time)
        durable_prices.extend(row["bid"] for row in prepared.rows)
        if prepared.rows:
            assert prepared.rows[0]["prior_event_time"] is None
            assert prepared.rows[1]["prior_event_time"] == datetime(1970, 1, 1, 0, 0, 10, tzinfo=UTC)
        last_offset, max_event_time = prepared.last_offset, prepared.max_event_time
        return last_offset + 1

    async def acknowledge(offsets: dict[TopicPartition, int]) -> None:
        assert last_offset == 5
        assert durable_prices == [1.1, 1.1]
        acknowledgements.append(offsets)
        if len(acknowledgements) == 1:
            raise CommitFailedError

    with pytest.raises(CommitFailedError):
        await persist_and_acknowledge(partition, records, save, acknowledge, stop)

    await persist_and_acknowledge(partition, records, save, acknowledge, stop)
    assert acknowledgements == [{partition: 6}, {partition: 6}]


def test_writer_orders_overlapping_offsets_and_accounts_for_invalid_records() -> None:
    earlier = record_at(5, 10_000)
    invalid = replace(record_at(6, 50_000), value=None)
    later = record_at(7, 5000)

    prepared = prepare_rows([later, invalid, earlier, invalid], last_offset=4, max_event_time=None)

    assert [row["message_id"] for row in prepared.rows] == ["00000005", "00000007"]
    assert prepared.invalid_offsets == [6]
    assert prepared.last_offset == 7
    assert prepared.rows[-1]["prior_event_time"] == datetime(1970, 1, 1, 0, 0, 10, tzinfo=UTC)

    invalid_only = prepare_rows([invalid], last_offset=5, max_event_time=prepared.max_event_time)
    assert not invalid_only.rows
    assert invalid_only.last_offset == 6
    assert invalid_only.max_event_time == prepared.max_event_time


async def test_invalid_only_batch_checkpoints_without_marking_a_tick_processed(monkeypatch: pytest.MonkeyPatch) -> None:
    mark_tick = Mock()
    monkeypatch.setattr("app.writer.mark_tick_processed", mark_tick)
    connection = AsyncMock()
    connection.execute.return_value.one = Mock(return_value=Mock(last_offset=4, max_event_time=None))
    transaction = MagicMock()
    transaction.__aenter__.return_value = connection
    engine = cast("AsyncEngine", Mock(begin=Mock(return_value=transaction)))
    invalid = replace(record_at(5, 50_000), value=None)

    next_offset = await store_batch(engine, TopicPartition("ticks.raw", 0), [invalid])

    assert next_offset == 6
    assert connection.execute.await_count == 3
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    mark_tick.assert_not_called()
