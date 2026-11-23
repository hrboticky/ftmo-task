"""Helpers shared by the checks.

Part of the homework infrastructure. Don't change; see CONTRACT.md.
"""

from collections.abc import Iterator
from typing import Final, Literal

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import urllib.error
import urllib.parse
import urllib.request

import yaml

ROOT: Final = Path(__file__).resolve().parent.parent
# The host ports docker-compose.yml publishes; override them with the same variables.
API_URL: Final = f"http://localhost:{os.environ.get('API_PORT') or 8000}"
SIMULATOR_URL: Final = f"http://localhost:{os.environ.get('SIMULATOR_PORT') or 8080}"
GAUGE: Final = "last_tick_processed_timestamp_seconds"
TIMEOUT_S: Final = 10

CHOICES: Final[dict[str, tuple[str, ...]]] = {
    "price_basis": ("bid", "ask", "mid"),
    "null_price": ("skip", "other_side", "last_value"),
    "out_of_sequence": ("same_as_other_ticks", "drop"),
    "empty_intervals": ("omit", "null", "carry_forward"),
    "late_ticks.policy": ("include", "exclude_after"),
}

type Status = Literal["PASS", "WARN", "FAIL"]


@dataclass
class Report:
    lines: list[tuple[Status, str, str]] = field(default_factory=list)

    def add(self, status: Status, name: str, detail: str = "") -> None:
        self.lines.append((status, name, detail))
        print(f"{status:4}  {name}" + (f": {detail}" if detail else ""), flush=True)  # noqa: T201

    @property
    def failed(self) -> bool:
        return any(status == "FAIL" for status, _, _ in self.lines)

    def summary(self) -> str:
        counts = {status: sum(1 for line in self.lines if line[0] == status) for status in ("PASS", "WARN", "FAIL")}
        return ", ".join(f"{count} {status.lower()}" for status, count in counts.items())


@dataclass(frozen=True)
class Response:
    status: int
    body: str

    def json(self) -> object:
        return json.loads(self.body)


def http_get(url: str, params: dict[str, str] | None = None, timeout: float = TIMEOUT_S) -> Response:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return Response(response.status, response.read().decode())
    except urllib.error.HTTPError as error:
        return Response(error.code, error.read().decode(errors="replace"))


def require(condition: bool, message: str) -> None:
    """Like assert, but not removed by python -O."""
    if not condition:
        raise ValueError(message)


def as_dict(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{what} is not an object: {value!r:.200}")
    return value


def as_list(value: object, what: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{what} is not a list: {value!r:.200}")
    return value


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ms(value: object) -> int:
    if not isinstance(value, str):
        raise TypeError(f"expected an ISO 8601 string, got {value!r}")
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError(f"timestamp without an offset: {value!r}")
    return round(moment.timestamp() * 1000)


def gauge_values(metrics_text: str) -> dict[str, float]:
    """`stage` label -> value of the staleness gauge, from Prometheus text format."""
    values: dict[str, float] = {}
    pattern = re.compile(rf'^{GAUGE}\{{[^}}]*stage="([^"]+)"[^}}]*\}}\s+(\S+)', re.MULTILINE)
    for stage, value in pattern.findall(metrics_text):
        values[stage] = float(value)
    return values


def load_decisions(path: Path = ROOT / "decisions.yaml") -> dict[str, object]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise ValueError(f"decisions.yaml is not valid YAML: {error}") from error
    if not isinstance(data, dict):
        raise TypeError("decisions.yaml must be a mapping")
    return data


def _get(data: object, dotted: str) -> object:
    for key in dotted.split("."):
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def validate_decisions(data: dict[str, object]) -> list[str]:
    """Problems with decisions.yaml, as readable messages; empty when valid."""
    problems: list[str] = []
    for key, choices in CHOICES.items():
        dotted = f"candles.{key}"
        value = _get(data, dotted)
        if value not in choices:
            problems.append(f"{dotted} is {value!r}, expected one of: {', '.join(choices)}")
    lateness = _get(data, "candles.late_ticks.lateness_seconds")
    if not isinstance(lateness, int) or isinstance(lateness, bool) or lateness < 0:
        problems.append("candles.late_ticks.lateness_seconds must be a whole number >= 0")
    elif _get(data, "candles.late_ticks.policy") == "exclude_after" and lateness == 0:
        problems.append("candles.late_ticks.lateness_seconds must be > 0 with policy exclude_after")
    for key in ("services.ws_readers", "services.db_writers"):
        value = _get(data, key)
        if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
            problems.append(f"{key} must be a non-empty list of compose service names")
        elif "TODO" in value:
            problems.append(f"{key} still contains TODO")
    list(metrics_entries(data, problems))  # collects problems with the metrics entries
    return problems


def metrics_entries(data: dict[str, object], problems: list[str] | None = None) -> Iterator[tuple[str, str]]:
    entries = data.get("metrics")
    if not isinstance(entries, list) or not entries:
        if problems is not None:
            problems.append("metrics must be a non-empty list of {stage, url}")
        return
    for index, entry in enumerate(entries):
        stage = entry.get("stage") if isinstance(entry, dict) else None
        url = entry.get("url") if isinstance(entry, dict) else None
        if not isinstance(stage, str) or not isinstance(url, str) or "TODO" in (stage, url) or not url.startswith("http"):
            if problems is not None:
                problems.append(f"metrics[{index}] needs a stage name and an http:// URL reachable from your machine")
            continue
        yield stage, url


def compose(*args: str, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("docker is not installed or not on PATH")
    return subprocess.run(
        [docker, "compose", *args],
        cwd=ROOT,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )
