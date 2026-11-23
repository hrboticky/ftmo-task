"""Alembic environment: runs your migrations against DATABASE_URL, read through `app.settings`.

`alembic upgrade head` runs here, as the one-shot `migrate` compose service, before your
services start. Locally: `uv run alembic upgrade head` (uses localhost by default).
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from app.settings import Settings

config = context.config
if config.config_file_name is not None:
    # Keeps the loggers of a process that runs migrations itself, such as a test session.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Set this to your SQLAlchemy MetaData to use `alembic revision --autogenerate`.
# Hypertables, compression and retention policies still need op.execute(...).
target_metadata: MetaData | None = None
database_url = Settings().sqlalchemy_url


def run_migrations_offline() -> None:
    """`alembic upgrade head --sql`: print the SQL instead of running it."""
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(database_url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
