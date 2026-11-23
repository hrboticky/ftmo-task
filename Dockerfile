# A starting point for your services' image. Change it as you need.
# See https://github.com/astral-sh/uv-docker-example
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /opt/app
ARG IMAGE_USER=nobody
ARG IMAGE_GROUP=nogroup

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV VIRTUAL_ENV=${UV_PROJECT_ENVIRONMENT}
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

ENV PYTHONPATH=/opt/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONFAULTHANDLER=1
# Our production containers run in this time zone.
ENV TZ=Europe/Prague

# Install dependencies first for better layer caching
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Owned by the image user, so it can read files that are private on your machine (umask 077).
COPY --chown=${IMAGE_USER}:${IMAGE_GROUP} . /opt/app/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

RUN python3 -m simulator.generate_fixture --out /fixtures/ticks.jsonl

USER ${IMAGE_USER}:${IMAGE_GROUP}

ENTRYPOINT []
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
