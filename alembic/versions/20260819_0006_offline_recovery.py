"""Add replay cursor to conversation_members for offline recovery.

Revision ID: 20260819_0006
Revises: 20260818_0005
Create Date: 2026-08-19

last_acknowledged_sequence tracks, per member, the highest
messages.sequence_number they've acknowledged (sent themselves, or been
marked DELIVERED for). Reuses the existing conversation_members row and the
existing sequence_number column rather than a parallel tracking table -
missing-message lookups on reconnect are a plain indexed range scan on
(conversation_id, sequence_number) > cursor.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260819_0006"
down_revision: str | Sequence[str] | None = "20260818_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "conversation_members",
        sa.Column(
            "last_acknowledged_sequence",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("conversation_members", "last_acknowledged_sequence")
