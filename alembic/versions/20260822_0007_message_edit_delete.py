"""Add edited_at/deleted_at to messages for edit/soft-delete support.

Revision ID: 20260822_0007
Revises: 20260819_0006
Create Date: 2026-08-22

Both columns are nullable and default to NULL (never edited/deleted). A
soft-delete stamps deleted_at and clears body server-side (see
app.messages.service.delete_message) rather than removing the row, so
sequence_number stays contiguous - see the README's message editing &
deletion section for the full state model.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260822_0007"
down_revision: str | Sequence[str] | None = "20260819_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("messages", sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("messages", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("messages", "deleted_at")
    op.drop_column("messages", "edited_at")
