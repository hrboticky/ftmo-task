from typing import Final

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Double, Integer, MetaData, Table, Text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.settings import Settings

metadata: Final = MetaData()
ticks: Final = Table(
    "ticks",
    metadata,
    Column("source", Text, primary_key=True),
    Column("symbol", Text, primary_key=True),
    Column("timestamp", DateTime(timezone=True), primary_key=True),
    Column("message_id", Text, primary_key=True),
    Column("bid", Double),
    Column("ask", Double),
    Column("may_be_out_of_sequence", Boolean, nullable=False),
    Column("prior_event_time", DateTime(timezone=True)),
)
checkpoints: Final = Table(
    "tick_checkpoints",
    metadata,
    Column("topic", Text, primary_key=True),
    Column("partition", Integer, primary_key=True),
    Column("last_offset", BigInteger, nullable=False),
    Column("max_event_time", DateTime(timezone=True)),
)


def create_engine(settings: Settings, *, read_only: bool = False) -> AsyncEngine:
    server_settings = {"default_transaction_read_only": "on"} if read_only else {}
    return create_async_engine(
        settings.sqlalchemy_url,
        pool_pre_ping=True,
        pool_timeout=3,
        hide_parameters=True,
        connect_args={"timeout": 3, "command_timeout": 15, "server_settings": server_settings},
    )
