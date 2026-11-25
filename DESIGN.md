# CandleKeeper design

## 1. Architecture

```text
WebSocket → reader → Kafka ticks.raw → writer → TimescaleDB ← API /ticks, /candles
                          └── separate consumer groups for other systems
```

## 2. Key decisions

- **Topic and ordering.** Kafka messages are keyed by (source, symbol). One partition preserves the global arrival order used to seal candles. Partitioning by instrument would scale writes but would require a different watermark. The topic retains raw events for 24 hours from broker receipt.
- **Deduplication.** PostgreSQL enforces uniqueness on (source, symbol, timestamp, message ID). Kafka producer idempotence covers transport retries. The database key also covers repeated source messages and writer retries across sessions.
- **Offset commits.** Automatic Kafka commits can advance past a failed database write. Write ticks and the locked checkpoint in one transaction, then acknowledge the next offset. A failed acknowledgement can safely replay committed data.
- **Storage.** Partition the hypertable by event time. Each candle looks up its instrument and time bounds through the primary key. A time-first index supports unfiltered tick scans. Compute OHLC on demand until workload measurements justify rollups.

## 3. Ambiguities and how I handled them

| Ambiguity | Decision and rationale |
|---|---|
| Candle price and nulls | Use bid, skip null bids, and retain both sides in raw history. Ask the product owner whether consumers need ask candles too. |
| Late data and prior responses | A provisional candle may change regardless of whether someone queried it. Seal at the end plus 1,500 seconds of feed event time. A tick can miss the minute cutoff and still count toward the day. |
| Event-time clock and silence | Use the max timestamp of earlier Kafka records, globally for the one partition. Persist it through restarts. Silence pauses it. Ask the gateway team for an authoritative watermark, idle-feed semantics, and handling of clock jumps. |
| Out-of-sequence flag | Apply the same event-time rule to flagged and unflagged ticks. The flag only covers some reordering. |
| Empty intervals | Omit them. Null rows and carry-forward prices imply different downstream expectations. |
| Same-millisecond events and duplicates | Sort ties by message ID within an instrument. The first payload wins for conflicting copies of a key. Ask for a source sequence if true intra-millisecond order matters. |
| Partial ranges and unknown instruments | Select by candle start and calculate the full bucket. Count candidate starts toward the 20,000 limit. Known instruments with no data return an empty result. |
| Price precision | Preserve the feed's JSON values as binary64. Confirm a scaled integer or decimal contract if prices enter accounting calculations. |

## 4. Design-only requirements

- **Deployment and recovery.** Keep overlapping subscriptions during reader rollouts. Require upstream sequence numbers and replay or backfill for a strict no-gap guarantee.
- **Ten-minute database outage.** Kafka holds 1.2 million ticks at peak. Size retained bytes for the outage, catch-up and headroom. Restore with write capacity above incoming rate.
- **Volume and retention.** Measure average and peak duration before sizing for a year. Sustained peak would be roughly 63 billion ticks per year. Benchmark columnstore on older Timescale chunks and precompute finalized aggregates per size if needed.
- **Two future consumers.** Each system reads `ticks.raw` in its own consumer group and deduplicates by (source, symbol, timestamp, message ID). Use replicated topics and quotas in production.
- **Graceful shutdown.** Stop WS intake and drain sends within a deadline. Finish or roll back the database transaction and close the Kafka client. Uvicorn drains requests and disposes the database pool.

## 5. Where data can still be lost

The gateway has no replay. A disconnect, crash, or a full upstream buffer can lose unsent or unacknowledged ticks, roughly the source rate times the gap plus any in-flight messages. The single-broker starter and its local volumes are a single point of failure. Kafka retention bounds the time available to recover a lagging writer. Production needs replicated storage and alerts for consumer lag, offset gaps, and disk capacity.
