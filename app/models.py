"""The tick message from the simulator's WebSocket."""

from typing import Literal

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field


class TickPayload(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    bid: float | None
    ask: float | None


class Tick(BaseModel):
    """A `symbolTick` message. Parse with `Tick.model_validate_json(raw)`."""

    model_config = ConfigDict(frozen=True, validate_by_name=True, serialize_by_alias=True)

    message_id: str = Field(alias="messageId", pattern=r"^[0-9A-F]{8}$")
    timestamp_msc: int = Field(alias="timestampMsc")
    event_type: Literal["symbolTick"] = Field(alias="eventType")
    source: str
    may_be_out_of_sequence: bool = Field(alias="mayBeOutOfSequence")
    payload: TickPayload

    @property
    def timestamp(self) -> datetime:
        """When the price changed at the platform, in UTC."""
        return datetime.fromtimestamp(self.timestamp_msc / 1000, tz=UTC)
