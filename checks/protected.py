"""Checksums of the files candidates must not change.

Part of the homework infrastructure. Don't change; see CONTRACT.md.

`python -m checks.protected --write` regenerates checks/protected.sha256 (for us, after
changing the infrastructure). `make check` compares against it.
"""

from typing import Final

import argparse
import hashlib
from pathlib import Path

from checks.common import ROOT

PROTECTED: Final = (
    "simulator",
    "infra/kafka/create-topics.sh",
    "checks",
)
CHECKSUM_FILE: Final = ROOT / "checks" / "protected.sha256"
# Created by editors and file managers (.DS_Store, .idea/, vim swap and backup files).
IGNORED_SUFFIXES: Final = ("~", ".swp", ".swo")


def ignored(relative: Path) -> bool:
    return any(part.startswith(".") or part == "__pycache__" for part in relative.parts) or relative.name.endswith(
        IGNORED_SUFFIXES
    )


def protected_files(root: Path = ROOT) -> list[Path]:
    checksum_file = root / CHECKSUM_FILE.relative_to(ROOT)
    files: list[Path] = []
    for name in PROTECTED:
        path = root / name
        candidates = sorted(path.rglob("*")) if path.is_dir() else [path]
        files.extend(
            candidate
            for candidate in candidates
            if candidate.is_file() and candidate != checksum_file and not ignored(candidate.relative_to(root))
        )
    return files


def current(root: Path = ROOT) -> dict[str, str]:
    return {file.relative_to(root).as_posix(): hashlib.sha256(file.read_bytes()).hexdigest() for file in protected_files(root)}


def recorded() -> dict[str, str]:
    checksums: dict[str, str] = {}
    for line in CHECKSUM_FILE.read_text(encoding="utf-8").splitlines():
        if line.strip():
            checksum, name = line.split(maxsplit=1)
            checksums[name] = checksum
    return checksums


def differences() -> list[str]:
    """Protected files that were changed, added or removed."""
    now, then = current(), recorded()
    changed = [name for name in now.keys() & then.keys() if now[name] != then[name]]
    added = now.keys() - then.keys()
    removed = then.keys() - now.keys()
    return sorted([*(f"changed: {n}" for n in changed), *(f"added: {n}" for n in added), *(f"removed: {n}" for n in removed)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Protected file checksums")
    parser.add_argument("--write", action="store_true", help="regenerate checks/protected.sha256")
    arguments = parser.parse_args()
    if arguments.write:
        CHECKSUM_FILE.write_text("".join(f"{checksum}  {name}\n" for name, checksum in sorted(current().items())))
        print(f"wrote {CHECKSUM_FILE.relative_to(ROOT)}")  # noqa: T201
    else:
        for line in differences():
            print(line)  # noqa: T201


if __name__ == "__main__":
    main()
