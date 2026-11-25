from typing import Final

from datetime import UTC, datetime

from app.models import Tick

RAW: Final = (
    b'{"messageId":"B2EA3ECA","timestampMsc":1774426681019,"eventType":"symbolTick",'
    b'"source":"ctrader","mayBeOutOfSequence":false,'
    b'"payload":{"symbol":"EURUSD","bid":null,"ask":1.16073}}'
)


def test_parses_the_protocol_example() -> None:
    tick = Tick.model_validate_json(RAW)
    assert tick.message_id == "B2EA3ECA"
    assert tick.payload.bid is None
    assert tick.payload.ask == 1.16073
    assert tick.timestamp == datetime(2026, 3, 25, 8, 18, 1, 19_000, tzinfo=UTC)


def test_mixed_case_hexadecimal_message_id_round_trips_verbatim() -> None:
    tick = Tick.model_validate_json(RAW.replace(b"B2EA3ECA", b"b2Ea3EcA"))
    assert tick.message_id == "b2Ea3EcA"
    assert Tick.model_validate_json(tick.model_dump_json()).message_id == "b2Ea3EcA"
