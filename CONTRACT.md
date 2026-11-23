# Contract

Our acceptance checks talk to your system only through what's described here. Everything behind it is your design. Please don't change this file.

This is a reference: you don't need to read it before you start. The suggested order in `README.md` names the section each step needs.

## Files that are ours

These files are part of the homework infrastructure. **Don't change them**:

- `simulator/`
- `infra/kafka/create-topics.sh`
- `checks/`

Our acceptance runs replace them with our own copies, so a change has no effect on the result. Changing them is a breach of this contract. `make check` lists any protected file that differs from the original.

Everything else is yours, including `infra/kafka/topics.txt`, `alembic.ini` and `migrations/`, the rest of `app/`, `tests/`, `pyproject.toml`, the `Dockerfile`, and the `reader`, `writer` and `api` services in `docker-compose.yml`.

## How we run your system

1. We clone your repository and run `docker compose up -d --build` from its root. Compose starts `kafka-init`, which creates the topics in `infra/kafka/topics.txt`, and `migrate`, which runs `alembic upgrade head` with your revisions in `migrations/versions/`, before your services. Your services must not create topics or change the schema themselves.
2. We add a compose override that:
   - puts the simulator in replay mode with manual start, for example `MODE=replay RATE=2000 FAULTS=replay REPLAY_START=manual`. Your services must not depend on the simulator's mode;
   - sets **`TZ=UTC`** on your services, whatever your `Dockerfile` says.
3. We wait until `GET http://localhost:8000/healthz` returns `200`, then start the replay with `GET http://localhost:8080/replay/start`.
4. During some runs we restart, with `docker compose restart`, the services listed under `services.db_writers` in `decisions.yaml` that are not also in `services.ws_readers`, and the `timescaledb` service.

Requirements for this to work:

- The services `simulator`, `kafka`, `kafka-init`, `timescaledb` and `migrate` keep their names and configuration.
- Your API is a compose service named `api`, published on port `8000`. `API_PORT` changes it on your machine only; our runs use `8000`.
- Your system subscribes to all six fixture instruments (`app/settings.py` lists them).
- `decisions.yaml` is filled in, and `make check` passes.

## Conventions

- **Timestamps** in requests and responses are ISO 8601 strings with an explicit offset, for example `2026-11-01T00:00:00Z` or `2026-11-01T01:00:00+01:00`. Responses use UTC (`Z`) with millisecond precision.
- **Time ranges** are half-open: `from` is inclusive, `to` is exclusive.
- **Invalid requests** (unknown parameter value, `from` not before `to`, missing required parameter, timestamp without an offset, a range larger than you serve) return `422` with a JSON body. The body shape is up to you.
- **Prices** are JSON numbers or `null`.

## `GET /healthz`

Returns `200` when the API can serve requests. The body is up to you.

## `GET /ticks`

Ticks in a time range, oldest first.

| Parameter | Required | Meaning |
|---|---|---|
| `from` | yes | Start of the range (inclusive) |
| `to` | yes | End of the range (exclusive) |
| `source` | no | Filter by platform, e.g. `ctrader` |
| `symbol` | no | Filter by instrument, e.g. `EURUSD` |
| `limit` | no | Maximum number of ticks. Default `1000`, maximum `10000` |

The range is evaluated on `timestampMsc`, the time the price changed at the platform. The response contains the first `limit` ticks of the range:

```json
{
  "items": [
    {
      "messageId": "B2EA3ECA",
      "timestamp": "2026-03-25T08:18:01.019Z",
      "source": "ctrader",
      "symbol": "EURUSD",
      "bid": null,
      "ask": 1.16073
    }
  ]
}
```

- **Duplicates:** a tick sent again with the same `source`, `symbol`, `timestampMsc` and `messageId` appears once. Ticks that differ in any of these are different ticks and each appears.
- **Size:** you must serve ranges of up to 14 days. You may refuse larger ranges with `422`.
- **Optional, nice to have: cursor pagination.** If you implement it, accept a `cursor` parameter and add `"nextCursor"` to the response: a string, or `null` on the last page. Following `nextCursor` must return every tick in the range exactly once, in an order that stays stable when timestamps tie. We don't check it automatically; mention it in your README.

## `GET /candles`

OHLC candles for one instrument, oldest first.

| Parameter | Required | Meaning |
|---|---|---|
| `source` | yes | Platform |
| `symbol` | yes | Instrument |
| `size` | yes | One of `1m`, `5m`, `15m`, `1h`, `12h`, `1d`, `1w`, `1M`, `1y` |
| `from` | yes | Start of the range (inclusive) |
| `to` | yes | End of the range (exclusive) |

A candle is included when its start time `at` is within the range.

```json
{
  "source": "ctrader",
  "symbol": "EURUSD",
  "size": "1h",
  "items": [
    { "at": "2026-11-02T09:00:00.000Z", "open": 1.16051, "high": 1.16112, "low": 1.16020, "close": 1.16098 }
  ]
}
```

- **Prices:** `open` is the first price in the interval and `close` the last, by `timestampMsc`. `high` and `low` are the maximum and minimum. Which price is used, what happens when it's `null`, and how late and `mayBeOutOfSequence` ticks are treated are your decisions; record them in `decisions.yaml`.
- **Time zone: `Europe/Prague`.** Name it explicitly in your code. Don't rely on the container's or the database session's time zone; our runs use `TZ=UTC` for your services, and the database session is in UTC.
- **Alignment:**
  - `1m`, `5m`, `15m` and `1h` start on whole minutes and hours (`5m` at :00, :05, ...).
  - `12h` candles start at 00:00 and 12:00, `1d` at 00:00, `1w` at 00:00 on Monday, `1M` at 00:00 on the first of the month, and `1y` at 00:00 on 1 January, all in `Europe/Prague` local time.
  - On the days the clocks change, `12h` and `1d` candles are one hour shorter or longer (23 or 25 hours for `1d`).
  - `at` is returned in UTC. A daily candle for 2 November 2026 therefore has `at` `2026-11-01T23:00:00.000Z`.
- **Empty intervals**, depending on `candles.empty_intervals`:
  - `omit`: no item.
  - `null`: an item with `at` set and `open`, `high`, `low` and `close` all `null`.
  - `carry_forward`: an item whose four prices all equal the previous candle's close. Intervals before the first tick at or before the range are omitted.
- **Size:** you must serve at least 20,000 candles per request. You may refuse more with `422`.

## `GET /metrics`

Prometheus text format, from `prometheus-client`.

Every stage of your pipeline exposes the gauge `last_tick_processed_timestamp_seconds{stage="<stage>"}`: the **wall-clock** Unix time, in seconds, when that stage last processed a tick (not the tick's own timestamp). `app/metrics.py` defines it. Stages in services other than the API serve it on their own port; list every stage and its URL, as reached from your machine, under `metrics` in `decisions.yaml`.

## Public checks

- `make check` checks this contract's wiring and shapes, `decisions.yaml` and the protected files. Run it before you submit.
- `make verify` deletes all data (`docker compose down -v`), replays the fixture through your system with duplicates and reordering, and compares `/ticks` with it.

`make verify` doesn't check candles. Our acceptance runs do more: they compare candles with what your `decisions.yaml` says, restart services during a replay, and measure freshness and staleness in live mode. Test your candles yourself.
