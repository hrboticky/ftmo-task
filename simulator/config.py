"""Part of the homework infrastructure. Don't change; see CONTRACT.md."""

from collections.abc import Mapping
from typing import Annotated, Final, Literal

from enum import StrEnum
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    ValidationInfo,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

type Probability = Annotated[float, Field(ge=0, le=1)]


class Mode(StrEnum):
    LIVE = "live"
    REPLAY = "replay"


class ReplayStart(StrEnum):
    ON_SUBSCRIBE = "on_subscribe"
    MANUAL = "manual"


class FaultPreset(StrEnum):
    NONE = "none"
    REPLAY = "replay"
    LIVE = "live"
    CHAOS = "chaos"


class FaultConfig(BaseModel):
    """A rate or interval of 0 disables that fault.

    Delays are wall-clock time in live mode and tick time in replay mode: in replay, a
    held-back tick is released once the stream has passed its timestamp plus the delay,
    so a delay means the same amount of market time at any replay speed.
    """

    # forbid, so a mistyped FAULT_* variable fails at startup instead of being ignored.
    model_config = ConfigDict(frozen=True, extra="forbid")

    disconnect_every_s: NonNegativeFloat = 0.0
    duplicate_rate: Probability = 0.0
    duplicate_max_delay_ms: NonNegativeInt = 2_000
    out_of_order_rate: Probability = 0.0
    out_of_order_max_delay_ms: NonNegativeInt = 3_000
    out_of_order_flag_rate: Probability = 0.5
    null_price_rate: Probability = 0.0
    malformed_rate: Probability = 0.0
    burst_every_s: NonNegativeFloat = 0.0
    burst_duration_s: NonNegativeFloat = 2.0
    burst_rate: PositiveFloat = 5_000.0


PRESETS: Final[Mapping[FaultPreset, FaultConfig]] = {
    FaultPreset.NONE: FaultConfig(),
    # For comparisons against the fixture: nothing that loses data or depends on timing.
    # Delays are in tick time; same-instrument ticks are minutes apart in the fixture.
    FaultPreset.REPLAY: FaultConfig(
        duplicate_rate=0.01,
        duplicate_max_delay_ms=300_000,
        out_of_order_rate=0.005,
        out_of_order_max_delay_ms=600_000,
        malformed_rate=0.001,
    ),
    FaultPreset.LIVE: FaultConfig(
        disconnect_every_s=40.0,
        duplicate_rate=0.01,
        out_of_order_rate=0.005,
        null_price_rate=0.02,
        malformed_rate=0.001,
        burst_every_s=60.0,
    ),
    FaultPreset.CHAOS: FaultConfig(
        disconnect_every_s=20.0,
        duplicate_rate=0.05,
        out_of_order_rate=0.03,
        null_price_rate=0.05,
        malformed_rate=0.01,
        burst_every_s=15.0,
        burst_duration_s=3.0,
    ),
}


class Config(BaseSettings):
    # FAULT_DUPLICATE_RATE splits once into FAULT + DUPLICATE_RATE, the fault field and its key.
    # env_ignore_empty, because docker-compose passes every variable the user left unset as "".
    model_config = SettingsConfigDict(
        frozen=True,
        env_ignore_empty=True,
        env_nested_delimiter="_",
        env_nested_max_split=1,
        validate_by_name=True,
    )

    mode: Mode = Mode.LIVE
    port: Annotated[int, Field(ge=1, le=65_535)] = 8080
    rate: PositiveFloat = 200.0
    symbol_count: PositiveInt = 20
    fixture_path: Annotated[Path, Field(validation_alias="FIXTURE")] = Path("/fixtures/ticks.jsonl")
    replay_loop: bool = False
    replay_start: ReplayStart = ReplayStart.ON_SUBSCRIBE
    seed: int = 42
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # Declared before faults, which reads it during validation.
    fault_preset: Annotated[FaultPreset, Field(validation_alias="FAULTS")] = FaultPreset.NONE
    # Not "faults": names match case-insensitively, so it would also claim the FAULTS variable.
    fault: Annotated[FaultConfig, Field(default_factory=dict, validate_default=True)]

    @field_validator("fault", mode="before")
    @classmethod
    def layer_over_preset(cls, fault: FaultConfig | dict[str, object], info: ValidationInfo) -> FaultConfig | dict[str, object]:
        # FAULT_* variables override single fields of the preset; a FaultConfig passed in code is complete.
        if isinstance(fault, FaultConfig):
            return fault
        preset = info.data.get("fault_preset", FaultPreset.NONE)
        return PRESETS[preset].model_dump() | fault
