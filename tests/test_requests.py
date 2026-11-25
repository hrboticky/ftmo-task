from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient
from pydantic import ValidationError
import pytest
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app.api import app
from app.models import Tick, TickPayload

VALID_RANGE = {"from": "2026-11-01T08:00:00.000Z", "to": "2026-11-01T09:00:00.000Z"}


@pytest.mark.parametrize(
    "changes",
    [
        {"from": "1793519999"},
        {"from": "2026-11-01T08:00:00"},
        {"from": "2026-11-01T09:00:00.000Z"},
        {"to": "2026-11-16T08:00:00.000Z"},
        {"source": "mt5", "symbol": "XAUUSD"},
        {"limit": "10001"},
    ],
)
def test_ticks_rejects_invalid_inputs_before_accessing_the_database(changes: dict[str, str]) -> None:
    with TestClient(app) as client:
        response = client.get("/ticks", params=VALID_RANGE | changes)
    assert response.status_code == 422


@pytest.mark.parametrize("invalid_price", ['"1.2"', "NaN", "Infinity"])
def test_invalid_tick_prices_are_rejected(invalid_price: str) -> None:
    raw = (
        '{"messageId":"BA123456","timestampMsc":1793519999000,"eventType":"symbolTick","source":"mt5",'
        f'"mayBeOutOfSequence":false,"payload":{{"symbol":"EURUSD","bid":{invalid_price},"ask":null}}}}'
    )
    with pytest.raises(ValidationError):
        Tick.model_validate_json(raw)


@pytest.mark.parametrize("symbol", ["", "X" * 129, "EUR\x00USD"])
def test_instrument_names_must_fit_the_database_key(symbol: str) -> None:
    with pytest.raises(ValidationError):
        TickPayload(symbol=symbol, bid=1.2, ask=1.3)


def test_pool_timeout_returns_503_without_database_details(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(dispose=AsyncMock())
    engine.connect.side_effect = PoolTimeoutError("pool diagnostic")
    monkeypatch.setattr("app.api.create_engine", Mock(return_value=engine))

    with TestClient(app) as client:
        response = client.get("/ticks", params=VALID_RANGE)

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
