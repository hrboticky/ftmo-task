from alembic import op
import sqlalchemy as sa

revision = "0001_ticks"
down_revision = None


def upgrade() -> None:
    op.create_table(
        "ticks",
        sa.Column("source", sa.Text(), primary_key=True),
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("message_id", sa.Text(), primary_key=True),
        sa.Column("bid", sa.Double()),
        sa.Column("ask", sa.Double()),
        sa.Column("may_be_out_of_sequence", sa.Boolean(), nullable=False),
        sa.Column("prior_event_time", sa.DateTime(timezone=True)),
    )
    op.execute("SELECT create_hypertable('ticks', by_range('timestamp', INTERVAL '1 day'), create_default_indexes => FALSE)")
    op.create_index("ticks_time_order", "ticks", ["timestamp", "source", "symbol", "message_id"])
    op.create_table(
        "tick_checkpoints",
        sa.Column("topic", sa.Text(), primary_key=True),
        sa.Column("partition", sa.Integer(), primary_key=True),
        sa.Column("last_offset", sa.BigInteger(), nullable=False),
        sa.Column("max_event_time", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    op.drop_table("tick_checkpoints")
    op.drop_table("ticks")
