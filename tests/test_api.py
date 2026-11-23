from datetime import UTC, datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.api import CandleOut, TickOut, app


def test_metrics_are_served_without_a_redirect() -> None:
    response = TestClient(app).get("/metrics", follow_redirects=False)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")


def test_ticks_serialize_with_contract_field_names_and_millisecond_utc() -> None:
    tick = TickOut(
        message_id="B2EA3ECA",
        timestamp=datetime(2026, 3, 25, 9, 18, 1, 19_000, tzinfo=timezone(timedelta(hours=1))),
        source="ctrader",
        symbol="EURUSD",
        bid=None,
        ask=1.16073,
    )
    assert tick.model_dump(mode="json") == {
        "messageId": "B2EA3ECA",
        "timestamp": "2026-03-25T08:18:01.019Z",
        "source": "ctrader",
        "symbol": "EURUSD",
        "bid": None,
        "ask": 1.16073,
    }


def test_candle_start_is_utc_with_milliseconds() -> None:
    candle = CandleOut(at=datetime(2026, 11, 1, 23, tzinfo=UTC), open=None, high=None, low=None, close=None)
    assert candle.model_dump(mode="json")["at"] == "2026-11-01T23:00:00.000Z"
