from typing import Final

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT: Final = Path(__file__).resolve().parent.parent


def config() -> Config:
    return Config(ROOT / "alembic.ini")


def test_revisions_form_a_single_line() -> None:
    assert len(ScriptDirectory.from_config(config()).get_heads()) <= 1


def test_migrations_render_as_sql_without_a_database() -> None:
    # Offline mode runs env.py and every revision's upgrade() without connecting.
    command.upgrade(config(), "head", sql=True)
