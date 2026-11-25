"""Settings for your services, read from environment variables (see docker-compose.yml)."""

from typing import Final

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.models import InstrumentName


class Instrument(BaseModel):
    source: InstrumentName
    symbol: InstrumentName


# The instruments in the replay fixture. CONTRACT.md requires subscribing to all of them.
FIXTURE_INSTRUMENTS: Final = (
    Instrument(source="ctrader", symbol="EURUSD"),
    Instrument(source="ctrader", symbol="GBPJPY"),
    Instrument(source="ctrader", symbol="XAUUSD"),
    Instrument(source="mt5", symbol="EURUSD"),
    Instrument(source="mt5", symbol="US500"),
    Instrument(source="dxtrade", symbol="BTCUSD"),
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True, env_ignore_empty=True)

    simulator_url: str = "ws://localhost:8080/market/v1/ticks/ws/"
    instruments: tuple[Instrument, ...] = FIXTURE_INSTRUMENTS
    kafka_bootstrap: str = "localhost:29092"
    ticks_topic: str = "ticks.raw"
    # postgresql:// for asyncpg; use `sqlalchemy_url` with SQLAlchemy.
    database_url: str = "postgresql://candlekeeper:candlekeeper@localhost:5432/ticks"
    # Port of the Prometheus endpoint for services other than the API.
    metrics_port: int = 9100
    log_level: str = "INFO"

    @property
    def sqlalchemy_url(self) -> str:
        return self.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
