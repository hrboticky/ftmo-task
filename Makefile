.PHONY: all lint format ruff mypy test test-integration up down logs replay migrate check verify

all: lint test

# Checks only; `make format` fixes formatting and import order.
lint: ruff mypy

format:
	uv run ruff format
	uv run ruff check --select I --fix

ruff:
	uv run ruff format --check
	uv run ruff check

mypy:
	uv run mypy .

test:
	uv run --locked pytest -vvv -m 'not integration' tests

test-integration:
	uv run --locked pytest -vvv -m integration tests

# ---- The system ------------------------------------------------------------------

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

# Replays the recorded fixture: MODE=replay RATE=2000 FAULTS=replay
replay:
	MODE=replay RATE=2000 FAULTS=replay docker compose up -d --build

# Applies new files in migrations/ without restarting everything.
migrate:
	docker compose run --rm migrate

# ---- Public checks (see CONTRACT.md) -----------------------------------------------

# Quick check of the contract: wiring, decisions.yaml, endpoint shapes, protected files.
check:
	uv run python -m checks.contract

# Replays the fixture through your system and compares /ticks with it.
verify:
	uv run python -m checks.verify
