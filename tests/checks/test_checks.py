from typing import Final

from pathlib import Path
import subprocess

import pytest

from checks import protected, verify
from checks.common import GAUGE, Response, load_decisions

ANSWERING_URL: Final = "http://localhost:9101/metrics"
SILENT_URL: Final = "http://localhost:9102/metrics"


class Clock:
    def __init__(self) -> None:
        self.now_s = 0.0

    def monotonic(self) -> float:
        return self.now_s

    def sleep(self, seconds: float) -> None:
        self.now_s += seconds


def test_files_from_editors_and_file_managers_are_not_protected(tmp_path: Path) -> None:
    (tmp_path / "simulator" / ".idea").mkdir(parents=True)
    (tmp_path / "simulator" / "server.py").write_text("print()\n")
    for junk in (".DS_Store", "server.py.swp", "server.py~", ".idea/workspace.xml"):
        (tmp_path / "simulator" / junk).write_text("junk\n")
    assert list(protected.current(tmp_path)) == ["simulator/server.py"]


def test_invalid_yaml_is_reported_as_a_readable_error(tmp_path: Path) -> None:
    path = tmp_path / "decisions.yaml"
    path.write_text("candles:\n  price_basis: [mid\n")
    with pytest.raises(ValueError, match=r"^decisions\.yaml is not valid YAML"):
        load_decisions(path)


def test_metrics_endpoints_that_never_answer_are_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    def http_get(url: str, params: dict[str, str] | None = None, timeout: float = 10) -> Response:
        if url == SILENT_URL:
            raise ConnectionRefusedError(url)
        return Response(200, f'{GAUGE}{{stage="reader"}} 1.0\n')

    monkeypatch.setattr(verify, "time", Clock())
    monkeypatch.setattr(verify, "http_get", http_get)
    assert verify.wait_until_settled([ANSWERING_URL, SILENT_URL]) == {SILENT_URL}


def test_verify_starts_from_an_empty_system(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str] | None]] = []

    def compose(*args: str, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        calls.append((args, env))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(verify, "compose", compose)
    verify.reset_system()
    [(down, _), (up, env)] = calls
    assert down[:2] == ("down", "-v")
    assert up[:3] == ("up", "-d", "--build")
    assert env is not None
    assert (env["MODE"], env["FAULTS"], env["REPLAY_START"]) == ("replay", "replay", "manual")
