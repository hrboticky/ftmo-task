# Homework: Product Engineer (Python) – Trading Systems

**Team:** CandleKeeper, FTMO · **Expected effort:** about 3½ hours. Please don't spend much more: a simple design you can defend beats an elaborate one.

We build with AI coding agents, and we expect you to use one (Claude Code, Cursor, Codex, ...) as you would at work. Writing code is cheap, so we don't measure how much you build. We look at how you design a system to meet its requirements, whether it stays correct when things fail, how you handle incomplete requirements, and how clearly you explain your decisions and their limits. You must understand and be able to defend every line and every decision you submit.

## The task

Trading platforms emit price changes (ticks) for instruments such as `EURUSD`. Design a service that ingests ticks from a WebSocket and serves them over HTTP, including historical queries and candles, and build its core.

This repository runs as is. It contains a tick simulator with the same protocol as our production gateway, Kafka, TimescaleDB, and stubs for your `reader`, `writer` and `api` services, already wired in `docker-compose.yml`. Topics and the schema are yours to decide, but you write no set-up code: `kafka-init` creates the topics you declare and `migrate` runs your Alembic revisions before your services start.

### Why it matters

FTMO traders trade on accounts we fund, under trading rules such as a maximum daily loss and a maximum overall loss. Ticks are the prices their trades are measured against. Today, our support and risk teams use the ticks and candles from this service to answer traders' questions about past trades, for example whether the price really reached a trader's stop loss.

Next quarter, two more internal systems will read the live stream (see *Future consumers* below):

- **trading-rule monitoring** checks every open account against its rules as prices move;
- **activity-threshold alerts** tell the risk team about unusual trading, for example many trades in a short time.

### The API

`CONTRACT.md` has the details; you'll read it section by section as you build.

- `GET /ticks`: ticks by source, symbol and time range, up to a `limit`. Cursor pagination is optional.
- `GET /candles`: OHLC candles for one instrument, with `at` (the start of the interval), `open`, `high`, `low` and `close`. Sizes `1m`, `5m`, `15m`, `1h`, `12h`, `1d`, `1w`, `1M`, `1y`, in the **`Europe/Prague`** time zone.
- `GET /metrics`: Prometheus metrics, including a staleness gauge for every stage of your pipeline.

### Requirements

Build the ones marked **Build**. For the others, write one line each in `DESIGN.md` on how your design meets them.

| Requirement | Target | Scope |
|---|---|---|
| No loss, no duplicates on restart | No tick that reaches Kafka is lost or stored twice when your database writers or the database restart. If a service also reads the WebSocket, ticks sent while it restarts don't count | **Build** |
| Freshness | A tick is queryable within 2 seconds of the simulator sending it (ticks the simulator itself delays are excluded) | **Build** |
| Silence is visible | Each stage's staleness gauge stops advancing when that stage stops, so an operator can tell which stage stopped and since when | **Build** |
| Stable history | A candle doesn't change once its interval has closed plus the lateness you declare in `decisions.yaml` | **Build** |
| No loss during deployments | No tick is lost during a deployment of *any* component, including the one reading the WebSocket | Design |
| Storage outage | Ingestion keeps working through a 10-minute database outage without losing data | Design |
| Volume and retention | 500 symbols across 3 platforms, peaks of 2,000 ticks/s, 1 year of ticks queryable through the API | Design |
| Future consumers | Two other internal systems (trading-rule monitoring and activity-threshold alerts) will need the same live stream next quarter, without affecting your service or each other | Design |
| Graceful shutdown | All components shut down cleanly on `SIGTERM` (they run on Kubernetes) | Design |

If you can't meet a *Build* requirement in the time, say so and explain how you would. An honest gap is better than a hidden one.

### Ambiguities

The requirements are incomplete on purpose, as real ones are. In `DESIGN.md`, **list every ambiguity you notice, not only the ones below**: the options, what you chose and why, in a line or two. If you'd normally ask a product owner, say what you'd ask and what you assumed meanwhile. We value noticing as much as deciding.

At least these four. Record their outcome in `decisions.yaml` too: our checks compute the candles you should return from it.

- Which price do candles use, and what happens when `bid` or `ask` is `null`?
- What happens to a tick that arrives after its candle was already served?
- How do you treat `mayBeOutOfSequence: true`?
- How are empty intervals represented in `/candles`?

## Start here

```bash
uv sync       # local environment
make up       # build and start everything
make check    # wiring, decisions.yaml, endpoint shapes: expect TODOs and 501s at first
```

A suggested order. The times are a guide for pacing, not a limit. Read each reference when its step needs it, not up front. Reviewing what the agent writes is part of every step.

| # | Step | About | Read then |
|---|---|---|---|
| 1 | This README, `make up`, `make check` | 20 min | |
| 2 | First pass of `DESIGN.md` sections 1 and 2: components, topics and partitioning, deduplication, offset commits, schema. Decide these before the agent writes code | 20 min | `simulator/README.md`: the protocol and the faults |
| 3 | Topics in `infra/kafka/topics.txt`; the first Alembic revision (`uv run alembic revision -m "create ticks"`) | 10 min | `migrations/README.md` |
| 4 | The reader, WebSocket to Kafka: `app/reader.py` | 25 min | `CONTRACT.md`, "GET /metrics" |
| 5 | The writer, Kafka to TimescaleDB: `app/writer.py` | 30 min | |
| 6 | `/ticks` in `app/api.py`, then `make verify` | 25 min | `CONTRACT.md`, "Conventions" and "GET /ticks" |
| 7 | `/candles`, and the candle rules in `decisions.yaml` | 30 min | `CONTRACT.md`, "GET /candles" |
| 8 | 2–3 tests, the rest of `DESIGN.md`, your README | 30 min | |
| 9 | `make lint`, `make test`, `make check`, `make verify` | 10 min | `CONTRACT.md`, "How we run your system" |

## What to submit

A ZIP file with all the changes to the repository, started from this one, containing:

- your code, `infra/kafka/topics.txt` and your Alembic revisions in `migrations/versions/`;
- `DESIGN.md`, up to two pages, from the template: architecture with a simple diagram, key decisions with the main alternative, ambiguities, the design-only requirements, and where data can still be lost;
- `decisions.yaml`, filled in;
- the 2–3 tests you'd most want if this went to production, with a sentence each on why;
- your agent instructions file (`CLAUDE.md`, `AGENTS.md` or similar), if you used one;
- **your own `README.md` in place of this one**, with:
  - how to run it;
  - a short **"Working with the agent"** section: one place where the agent was wrong (in code or in design) and how you caught it, and one thing you deliberately didn't delegate and why;
  - what you'd do next with more time, and why. What you consciously left out is as useful to us as what you built.

Before you submit, `make lint` (Mypy strict, Ruff), `make check` and `make verify` must pass.

## How we evaluate it

Our acceptance runs replay the recorded data with faults and compare your ticks and candles, restart your database writers and the database during a replay, and measure freshness and staleness in live mode. `make verify` checks ticks only, so test your candles yourself.

We look at:

- **Design:** it fits the requirements, the trade-offs are explicit, and you know what it can't guarantee.
- **Ambiguities:** the real gaps noticed, decided and explained.
- **Correctness under failure:** the *Build* requirements hold, and the system behaves as `DESIGN.md` claims.
- **The stack:** Kafka, TimescaleDB and SQLAlchemy Core used deliberately, not just present.
- **Code and operability:** strict typing, clear structure, readable async code; metrics an on-call engineer would rely on.
- **Working with AI and communication:** honest reflection, full ownership, and a design and README a teammate could pick up.

If we move forward, the 60-minute interview starts with you walking us through your homework; please bring a laptop with your repository running. Questions about the assignment are welcome and never held against you.

---

## Reference

### What's here

| Path | Whose | What it is |
|---|---|---|
| `docker-compose.yml` | both | The whole system: infrastructure at the top, your `reader`, `writer` and `api` below |
| `infra/kafka/topics.txt` | **yours** | Your topics: names, partitions, config |
| `alembic.ini`, `migrations/` | **yours** | Your schema as Alembic revisions, already configured |
| `app/` | **yours** | Starting points for the reader, writer and API, plus settings, a typed `Tick` model and the staleness gauge. Restructure them as your design needs |
| `tests/` | **yours** | Your tests. `tests/simulator/` tests the simulator |
| `DESIGN.md`, `decisions.yaml` | **yours** | Templates for your design and your key decisions |
| `pyproject.toml`, `uv.lock`, `Dockerfile` | **yours** | Python 3.14 and our team's Mypy and Ruff settings. Change dependencies freely; keep the checks at least this strict. Our containers run with `TZ=Europe/Prague` |
| `CONTRACT.md` | ours | The API, and how our checks run your system |
| `simulator/` | ours | The tick simulator: protocol, replay fixture and faults in `simulator/README.md` |
| `infra/kafka/create-topics.sh` | ours | Topic creation |
| `checks/` | ours | `make check` and `make verify` |

**Don't change `simulator/`, `infra/kafka/create-topics.sh` or `checks/`.** Our runs replace them with our copies, and `make check` reports any change.

### Commands

| Command | What it does |
|---|---|
| `make up` | Build and start everything |
| `make logs` | Follow the logs |
| `make check` | Wiring, `decisions.yaml`, the API's shape |
| `make replay` | Restart the simulator in replay mode: `MODE=replay RATE=2000 FAULTS=replay` |
| `make verify` | **Delete all data**, replay the fixture with duplicates and reordering, and compare `/ticks` with it |
| `make lint`, `make test` | Ruff and Mypy; pytest |
| `make down` | Stop everything and **delete all data**: also how you start over after changing a topic or an applied revision |

The replay plays about 80,000 recorded ticks from 23 October to 3 November 2026, including the end of summer time in Prague. `curl localhost:8080/stats` shows its progress and the faults it injected.

### Connection details

| Service | From your machine | Inside compose |
|---|---|---|
| Simulator | `ws://localhost:8080/market/v1/ticks/ws/` | `ws://simulator:8080/market/v1/ticks/ws/` |
| Kafka | `localhost:29092` | `kafka:9092` |
| TimescaleDB (`candlekeeper` / `candlekeeper`, database `ticks`) | `localhost:5432` | `timescaledb:5432` |
| API | `http://localhost:8000` | `http://api:8000` |
| Reader and writer metrics | `http://localhost:9101/metrics`, `http://localhost:9102/metrics` | port `9100` |

If a port is taken on your machine (a local PostgreSQL, for example), set `API_PORT`, `SIMULATOR_PORT`, `KAFKA_PORT` or `POSTGRES_PORT` before `make up`. `make check` and `make verify` read `API_PORT` and `SIMULATOR_PORT` too. With another `POSTGRES_PORT` or `KAFKA_PORT`, point `DATABASE_URL` or `KAFKA_BOOTSTRAP` at it for anything you run outside compose, such as `alembic`.
