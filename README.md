# CandleKeeper

CandleKeeper ingests platform ticks over WebSocket, buffers them in Kafka, and serves tick history and Prague-aligned OHLC from TimescaleDB.

## Run locally

Prerequisites are Docker with Compose, uv, and Python 3.14.

```sh
uv sync --locked --python 3.14
make up
make check
```

The simulator starts in live mode and the API listens on port 8000. Open `/docs` for the API parameters. `/ticks` accepts ranges of up to 14 days and returns up to 10,000 items. `/candles` accepts up to 20,000 interval starts and omits empty intervals. Use an explicit UTC offset in all request timestamps. `CONTRACT.md` describes the response fields and interval sizes.

`/healthz` checks that the API can read the tick table. The observer can scrape the reader on port 9101 and the writer on port 9102. The stage series record the last valid WebSocket receipt, Kafka acknowledgement, and database commit. Alert on the age of each series and use Prometheus `up` to detect unavailable targets. In replay mode, all three series stop advancing after the pipeline drains.

Use `docker compose down` to stop while keeping data. `make down` removes this project's volumes.

## Local security boundary

The Compose bridge binds published ports to IPv4 loopback. Use Docker Engine 28 or newer for this isolation. If you started the stack before the loopback setting was added, run `docker compose down` before starting it again to recreate the network and keep the volumes.

The starter broker uses plaintext and the database has a shared development credential. Production requires patched broker and database images, private networking, authenticated Kafka producers, separate least-privilege database roles, and TLS and access controls at the API edge. The API uses read-only database transactions and bounds concurrent requests, pool checkout, and query duration. Up to four candle queries per process can use the pool at a time.

On 25 November 2026, Trivy found 13 distinct HIGH advisories in the Debian packages of the Linux arm64 application image and zero CRITICAL advisories. Its database listed no fixed Debian version for those 13. The supplied Kafka and TimescaleDB images had CRITICAL findings. Recheck the images when updating the deployment. [Apache Kafka advisories](https://kafka.apache.org/community/cve-list/), [TimescaleDB security fixes](https://github.com/timescale/timescaledb/releases/tag/2.32.1)

The reader connects directly to the simulator. The writer skips records that fail the tick schema, counts them in `tick_writer_invalid_messages_total`, and reports their offsets. Kafka retains the original records until topic expiry. Only the trusted reader should publish to `ticks.raw`. The writer preserves schema-valid backlog across subscription changes. API instrument filters use the configured instrument list. A structurally valid tick can skew event time if an authorized feed clock jumps. Production watermark policy requires an agreed clock or upstream watermark.

## Checks

```sh
make lint
make test
make test-integration
COMPOSE_PROJECT_NAME=candlekeeper-verify make verify
```

`make lint` runs the Ruff format check, Ruff lint and strict MyPy. Unit tests run without Docker. The integration tests use isolated Compose projects with temporary host ports and remove those projects at teardown. Before running `make verify`, stop any stack using the same host ports. The verifier removes the selected project's volumes and starts a fresh replay with duplicates and reordered ticks.

Three production-critical checks are:

- **Replay and recovery.** Restart the writer and database during replay, compare the recovered ticks against the fixture, and exercise an acknowledgement failure after a successful database write. This guards the offset and database transaction boundary.
- **Calendar and candle finality.** Compare all nine sizes against the replay fixture and verify the repeated autumn hour, the shorter spring day, full-bucket aggregation, and late tick eligibility. These rules determine which historical prices users see.
- **Freshness and staleness.** Observe live ticks and time their visibility through the API, then stop the database and confirm which stage stalls. This checks both the user-visible deadline and the signals operators need during an outage.

## Working with the agent

The agent initially focused on the simulator's 600-second delay setting. Reading the release loop and measuring the fixture exposed another source of delay: a held tick only releases when the replay reaches the next event. The fixture has an 851.153-second gap. We sized the allowance against both effects.

I kept the choice of the finality clock as a product decision. I chose event time for deterministic replay and accepted that no watermark advances while the entire feed is silent. For a wall-clock closure guarantee, I would first confirm what time and recovery signals the gateway can supply.

## With more time

I'd add an upstream resume cursor to recover WebSocket gaps, then load-test a full year of representative traffic before changing retention, indexes, or aggregation. I'd also work with the feed owners on event-time watermarks and clock anomaly handling, because a timestamp far ahead of the stream can seal buckets early.
