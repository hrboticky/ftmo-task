# Tick simulator

> **Part of the homework infrastructure. Don't change anything in this folder** (see `CONTRACT.md`). `make check` reports changed files, and our checks run with our own copy.

A WebSocket server that speaks the same protocol as our production tick gateway. Use it instead of a live feed: it works on weekends, it can replay a known data set, and it can misbehave on purpose.

```bash
docker compose up -d simulator          # from the repository root
```

- WebSocket: `ws://localhost:8080/market/v1/ticks/ws/` (inside compose: `ws://simulator:8080/market/v1/ticks/ws/`)
- Health: `GET http://localhost:8080/healthz`
- Counters: `GET http://localhost:8080/stats` (ticks sent, injected faults, replay state)
- Replay start: `GET http://localhost:8080/replay/start` (only with `REPLAY_START=manual`, see below)

## Protocol

After connecting, subscribe to the instruments you want:

```json
{
  "eventType": "wsTickStreamAddSymbols",
  "payload": { "symbols": [ { "symbol": "EURUSD", "source": "ctrader" } ] }
}
```

`wsTickStreamRemoveSymbols` with the same payload unsubscribes. The server sends no acknowledgement. You receive ticks only for the instruments you subscribed to:

```json
{
  "messageId": "B2EA3ECA",
  "timestampMsc": 1774426681019,
  "eventType": "symbolTick",
  "source": "ctrader",
  "mayBeOutOfSequence": false,
  "payload": { "symbol": "EURUSD", "bid": null, "ask": 1.16073 }
}
```

Things worth knowing:

- `timestampMsc` is when the price changed at the platform (milliseconds since the Unix epoch, UTC), not when you received it.
- `bid` or `ask` can be `null`.
- `messageId` is 8 hexadecimal characters. Repeated messages carry the same id.
- The WebSocket has no history: messages sent while you're disconnected are not delivered again.

## Instruments

| Source | Symbols |
|---|---|
| `ctrader` | `EURUSD`, `GBPJPY`, `XAUUSD` |
| `mt5` | `EURUSD`, `US500` |
| `dxtrade` | `BTCUSD` |

In `live` mode, `SYMBOL_COUNT` above 6 adds synthetic instruments (`SYN000`, `SYN001`, ...) spread across the three sources, so you can simulate a larger universe.

## Modes

**`live`** (default): random-walk prices at `RATE` ticks per second across all instruments, timestamped with the current time.

**`replay`**: plays the recorded fixture `/fixtures/ticks.jsonl` (about 80,000 ticks from 23 October to 3 November 2026) at `RATE` ticks per second, keeping the original timestamps. It runs once, unless `REPLAY_LOOP=true`.

When the replay starts depends on `REPLAY_START`:

- `on_subscribe` (default): when every instrument in the fixture has a subscriber.
- `manual`: when something calls `GET /replay/start`. The call returns `409`, naming the missing instruments, while some fixture instrument has no subscriber, or when the replay isn't waiting for a start. `make verify` and our checks use this, so the replay doesn't begin before your system is ready.

`GET /stats` shows `replayState`: `waiting_for_subscriber`, `waiting_for_start`, `playing` or `finished`.

The fixture is generated deterministically when the image is built. To look at it locally:

```bash
uv run python -m simulator.generate_fixture --out fixtures/ticks.jsonl
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `MODE` | `live` | `live` or `replay` |
| `RATE` | `200` | Ticks per second across all instruments |
| `SYMBOL_COUNT` | `20` | Instruments in `live` mode |
| `REPLAY_START` | `on_subscribe` | `on_subscribe` or `manual` |
| `REPLAY_LOOP` | `false` | Restart the replay when it ends |
| `SEED` | `42` | Random seed |
| `FAULTS` | `none` | Fault preset: `none`, `replay`, `live` or `chaos` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `PORT` | `8080` | HTTP and WebSocket port |
| `FIXTURE` | `/fixtures/ticks.jsonl` | Replay fixture path |

## Faults

Choose a preset with `FAULTS`, then override single settings if you want. A rate is a probability per tick; `0` turns a fault off. The simulator refuses to start on an out-of-range value or an unknown `FAULT_*` variable.

Delays are **wall-clock time in `live` mode and tick time in `replay` mode**. In replay, a held-back tick is released once the stream has passed its timestamp plus the delay, so a delay means the same amount of market time at any replay speed.

| Variable | `replay` | `live` | `chaos` | Effect |
|---|---|---|---|---|
| `FAULT_DISCONNECT_EVERY_S` | 0 | 40 | 20 | Drop each connection after 0.5–1.5 times this many seconds, sometimes cleanly, sometimes without a close frame |
| `FAULT_DUPLICATE_RATE` | 0.01 | 0.01 | 0.05 | Send a tick again later, with the same `messageId` |
| `FAULT_DUPLICATE_MAX_DELAY_MS` | 300000 | 2000 | 2000 | Longest delay before a duplicate is sent |
| `FAULT_OUT_OF_ORDER_RATE` | 0.005 | 0.005 | 0.03 | Delay a tick so it arrives after newer ones, keeping its original timestamp. Some of these are flagged `mayBeOutOfSequence: true`, some are not |
| `FAULT_OUT_OF_ORDER_MAX_DELAY_MS` | 600000 | 3000 | 3000 | Longest delay of an out-of-order tick |
| `FAULT_OUT_OF_ORDER_FLAG_RATE` | 0.5 | 0.5 | 0.5 | Share of out-of-order ticks flagged `mayBeOutOfSequence: true` |
| `FAULT_NULL_PRICE_RATE` | 0 | 0.02 | 0.05 | Set `bid` or `ask` to `null` (`live` mode only; the fixture has its own) |
| `FAULT_MALFORMED_RATE` | 0.001 | 0.001 | 0.01 | Send something that isn't a valid tick |
| `FAULT_BURST_EVERY_S` | 0 | 60 | 15 | Raise the rate to `FAULT_BURST_RATE` periodically |
| `FAULT_BURST_DURATION_S` | 2 | 2 | 3 | Length of each burst |
| `FAULT_BURST_RATE` | 5000 | 5000 | 5000 | Ticks per second during a burst |

The `replay` preset never loses data and doesn't depend on timing, so the result can be compared with the fixture. Examples:

```bash
make replay                                         # MODE=replay RATE=2000 FAULTS=replay
MODE=live FAULTS=live docker compose up -d simulator  # disconnects, bursts, null prices
```

## Implementation note

The server paces messages against a schedule: every millisecond it sends everything that is due by then. It sustains well over 10,000 ticks per second in a single Python process.
