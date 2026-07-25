"""create processed_events table

Revision ID: 202607250001
Revises:
Create Date: 2026-07-25 00:01:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "202607250001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Required for gen_random_uuid() to be available on the target database.
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')

    op.create_table(
        "processed_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        op.f("ix_processed_events_source"), "processed_events", ["source"], unique=False
    )
    op.create_index(
        op.f("ix_processed_events_status"), "processed_events", ["status"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_processed_events_status"), table_name="processed_events")
    op.drop_index(op.f("ix_processed_events_source"), table_name="processed_events")
    op.drop_table("processed_events")
