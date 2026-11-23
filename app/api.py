"""HTTP API: the contract in CONTRACT.md.

Run with `uvicorn app.api:app` (the `api` compose service). /healthz and /metrics work;
/ticks and /candles return 501 until you implement them.
"""

from typing import Annotated, Literal

from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import AwareDatetime, BaseModel, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel

app = FastAPI(title="Tick service")

type CandleSize = Literal["1m", "5m", "15m", "1h", "12h", "1d", "1w", "1M", "1y"]


def to_utc_milliseconds(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# CONTRACT.md: responses use UTC (`Z`) with millisecond precision.
type ResponseTimestamp = Annotated[AwareDatetime, PlainSerializer(to_utc_milliseconds, return_type=str)]


class ResponseModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, serialize_by_alias=True)


class TickOut(ResponseModel):
    message_id: str
    timestamp: ResponseTimestamp
    source: str
    symbol: str
    bid: float | None
    ask: float | None


class TicksResponse(ResponseModel):
    items: list[TickOut]
    # Only if you implement the optional cursor pagination (see CONTRACT.md).
    next_cursor: str | None = None


class CandleOut(ResponseModel):
    at: ResponseTimestamp
    open: float | None
    high: float | None
    low: float | None
    close: float | None


class CandlesResponse(ResponseModel):
    source: str
    symbol: str
    size: CandleSize
    items: list[CandleOut]


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


# A route rather than a mounted app, which would redirect /metrics to /metrics/.
@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/ticks")
async def get_ticks(
    from_: Annotated[AwareDatetime, Query(alias="from")],
    to: AwareDatetime,
    source: str | None = None,
    symbol: str | None = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
) -> TicksResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "not implemented yet")


@app.get("/candles")
async def get_candles(
    source: str,
    symbol: str,
    size: CandleSize,
    from_: Annotated[AwareDatetime, Query(alias="from")],
    to: AwareDatetime,
) -> CandlesResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "not implemented yet")
