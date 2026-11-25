from typing import Annotated, Literal

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
type InstrumentName = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[^\x00]+$")]


class TickPayload(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    symbol: InstrumentName
    bid: FiniteFloat | None
    ask: FiniteFloat | None


class Tick(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, validate_by_name=True, serialize_by_alias=True)

    message_id: str = Field(alias="messageId", pattern=r"^[0-9A-Fa-f]{8}$")
    timestamp_msc: int = Field(alias="timestampMsc", ge=-62_135_596_800_000, le=253_402_300_799_999)
    event_type: Literal["symbolTick"] = Field(alias="eventType")
    source: InstrumentName
    may_be_out_of_sequence: bool = Field(alias="mayBeOutOfSequence")
    payload: TickPayload

    @property
    def timestamp(self) -> datetime:
        return EPOCH + timedelta(milliseconds=self.timestamp_msc)
