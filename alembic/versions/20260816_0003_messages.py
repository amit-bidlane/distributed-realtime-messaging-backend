"""Create messages table.

Revision ID: 20260816_0003
Revises: 20260814_0002
Create Date: 2026-08-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260816_0003"
down_revision: str | Sequence[str] | None = "20260814_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("sender_id", sa.Uuid(), nullable=False),
        sa.Column("client_message_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["sender_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_messages_conversation_sequence", "messages", ["conversation_id", "sequence_number"]
    )
    op.create_index(
        "ix_messages_conversation_sender_client_id",
        "messages",
        ["conversation_id", "sender_id", "client_message_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_messages_conversation_sender_client_id", table_name="messages")
    op.drop_index("ix_messages_conversation_sequence", table_name="messages")
    op.drop_table("messages")
