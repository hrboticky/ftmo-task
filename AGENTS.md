# CandleKeeper agent instructions

- Follow CONTRACT.md. Keep simulator/, checks/, infra/kafka/create-topics.sh and the supplied infrastructure configuration intact.
- Use uv and Python 3.14. Use async SQLAlchemy Core and apply schema changes through Alembic.
- Keep code comment-free, explicitly typed, and compliant with strict MyPy and Ruff. Prefer small changes and existing interfaces.
- Run make lint, make test, make test-integration, make check and make verify. Use disposable Compose data for make verify.
- Keep secrets and PLAN.md out of Git and the submission. Commit and push only when explicitly requested. Use the configured human identity without attribution trailers.
