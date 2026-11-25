FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv-bin
FROM python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b

WORKDIR /opt/app
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH=/opt/app \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    TZ=Europe/Prague

RUN --mount=type=tmpfs,target=/var/lib/apt/lists \
    apt-get update && apt-get install -y --only-upgrade --no-install-recommends libzstd1 \
    && /usr/local/bin/python -m pip uninstall -y pip \
    && rm -r /usr/local/lib/python3.14/ensurepip

RUN --mount=from=uv-bin,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project --no-managed-python

COPY app app
COPY simulator simulator
COPY migrations migrations
COPY alembic.ini .
RUN chmod -R u=rwX,go=rX /opt/app && python -m simulator.generate_fixture --out /fixtures/ticks.jsonl

USER nobody:nogroup
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
