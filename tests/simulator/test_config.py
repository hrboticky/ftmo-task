from pathlib import Path

import pytest

from simulator.config import PRESETS, Config, FaultPreset


def test_individual_variables_override_the_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAULTS", "chaos")
    monkeypatch.setenv("FAULT_DUPLICATE_RATE", "0")
    fault = Config().fault
    assert fault.duplicate_rate == 0
    assert fault.out_of_order_rate == PRESETS[FaultPreset.CHAOS].out_of_order_rate


def test_empty_variables_fall_back_to_the_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    # docker-compose passes every variable the user left unset as an empty string.
    monkeypatch.setenv("FAULTS", "live")
    monkeypatch.setenv("FAULT_BURST_EVERY_S", "")
    monkeypatch.setenv("RATE", "")
    config = Config()
    assert config.fault == PRESETS[FaultPreset.LIVE]
    assert config.rate == 200


def test_fixture_variable_sets_the_fixture_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIXTURE", "/data/ticks.jsonl")
    assert Config().fixture_path == Path("/data/ticks.jsonl")


def test_replay_preset_never_loses_data() -> None:
    replay = PRESETS[FaultPreset.REPLAY]
    assert replay.disconnect_every_s == 0
    assert replay.burst_every_s == 0
    assert replay.null_price_rate == 0
