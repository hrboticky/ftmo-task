from collections.abc import AsyncIterator
from typing import Annotated, cast

from asyncio import Semaphore
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
import logging

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.candles import CANDLES_QUERY, LATENESS, CandleSize, boundaries_for
from app.settings import Settings
from app.storage import create_engine, ticks

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    application.state.instruments = {(instrument.source, instrument.symbol) for instrument in settings.instruments}
    application.state.candle_slots = Semaphore(4)
    engine = create_engine(settings, read_only=True)
    application.state.engine = engine
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(title="Tick service", lifespan=lifespan)


def parse_request_time(value: object) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("Expected an ISO 8601 date and time with an explicit offset")
    moment = datetime.fromisoformat(value)
    if moment.utcoffset() is None:
        raise ValueError("An explicit time zone offset is required")
    try:
        return moment.astimezone(UTC)
    except OverflowError as error:
        raise ValueError("Timestamp is out of range") from error


def to_utc_milliseconds(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


type RequestTimestamp = Annotated[AwareDatetime, BeforeValidator(parse_request_time)]
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


def validate_instrument(request: Request, source: str | None, symbol: str | None) -> None:
    instruments = cast("set[tuple[str, str]]", request.app.state.instruments)
    if not any(
        (source is None or source == known_source) and (symbol is None or symbol == known_symbol)
        for known_source, known_symbol in instruments
    ):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Unknown instrument filter")


def validate_range(start: datetime, end: datetime) -> None:
    if start >= end:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "from must be before to")


@asynccontextmanager
async def database(request: Request) -> AsyncIterator[AsyncConnection]:
    engine = cast("AsyncEngine", request.app.state.engine)
    try:
        async with engine.connect() as connection:
            yield connection
    except (DBAPIError, OSError, TimeoutError, PoolTimeoutError) as error:
        logger.warning("Database request failed: %s", type(error).__name__)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Database unavailable") from error


@app.get("/healthz")
async def healthz(request: Request) -> dict[str, str]:
    async with database(request) as connection:
        await connection.execute(select(ticks.c.message_id).limit(0))
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/ticks")
async def get_ticks(
    request: Request,
    *,
    from_: Annotated[RequestTimestamp, Query(alias="from")],
    to: RequestTimestamp,
    source: str | None = None,
    symbol: str | None = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
) -> TicksResponse:
    validate_range(from_, to)
    if to - from_ > timedelta(days=14):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Tick ranges may span at most 14 days")
    validate_instrument(request, source, symbol)
    statement = select(ticks).where(ticks.c.timestamp >= from_, ticks.c.timestamp < to)
    if source is not None:
        statement = statement.where(ticks.c.source == source)
    if symbol is not None:
        statement = statement.where(ticks.c.symbol == symbol)
    statement = statement.order_by(ticks.c.timestamp, ticks.c.source, ticks.c.symbol, ticks.c.message_id).limit(limit)
    async with database(request) as connection:
        result = await connection.execute(statement)
        return TicksResponse(items=[TickOut.model_validate(row) for row in result.mappings()])


@app.get("/candles")
async def get_candles(
    request: Request,
    *,
    source: str,
    symbol: str,
    size: CandleSize,
    from_: Annotated[RequestTimestamp, Query(alias="from")],
    to: RequestTimestamp,
) -> CandlesResponse:
    validate_range(from_, to)
    validate_instrument(request, source, symbol)
    try:
        boundaries = boundaries_for(from_, to, size)
    except ValueError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    items: list[CandleOut] = []
    if boundaries:
        slots = cast("Semaphore", request.app.state.candle_slots)
        if slots.locked():
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Candle queries are busy")
        async with slots, database(request) as connection:
            result = await connection.execute(
                CANDLES_QUERY,
                {
                    "source": source,
                    "symbol": symbol,
                    "boundaries": boundaries,
                    "start_at": boundaries[0],
                    "end_at": boundaries[-1],
                    "lateness": LATENESS,
                },
            )
            items = [CandleOut.model_validate(row) for row in result.mappings()]
    return CandlesResponse(source=source, symbol=symbol, size=size, items=items)
