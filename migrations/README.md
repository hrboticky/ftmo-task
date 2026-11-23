# Migrations

Your database schema, as [Alembic](https://alembic.sqlalchemy.org/) revisions in `versions/`. The `migrate` compose service runs `alembic upgrade head` before your services start.

```bash
uv run alembic revision -m "create ticks"     # new file in migrations/versions/
uv run alembic upgrade head                   # apply to the database on localhost
uv run alembic upgrade head --sql             # print the SQL without running it
docker compose run --rm migrate               # apply inside compose
```

- Write each revision's `upgrade()` with `op.create_table(...)`, `op.create_index(...)` and so on, or `op.execute("...")` for TimescaleDB features such as `create_hypertable`, compression and retention policies. Write `downgrade()` too, or raise `NotImplementedError` if you decide not to support it.
- `alembic.ini`, `env.py` and `script.py.mako` are set up for you and are yours to change. `env.py` reads `DATABASE_URL`; set `target_metadata` there if you want `--autogenerate`.
- New revision files are formatted with Ruff automatically. Remove the `op` or `sa` import if a revision doesn't use it.
- The TimescaleDB extension is already enabled.
- Applied revisions are recorded in the `alembic_version` table. Don't edit a revision once it's applied; add a new one, or run `make down` to start from an empty database.
- The database session's time zone is UTC. Don't rely on it for candle alignment; name `Europe/Prague` explicitly where it matters.
