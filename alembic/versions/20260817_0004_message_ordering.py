"""Add per-conversation sequence counters and message uniqueness.

Revision ID: 20260817_0004
Revises: 20260816_0003
Create Date: 2026-08-17

Replaces an earlier placeholder idempotency/ordering approach (check-then-insert,
MAX(sequence_number)+1) with real guarantees:

- conversation_sequence_counters: one row per conversation, incremented by a
  single atomic UPDATE ... RETURNING at message-send time so sequence
  numbers can never race, duplicate, or skip under concurrent writers.
- A unique constraint on messages(conversation_id, sender_id,
  client_message_id) so duplicate sends are rejected by the database itself,
  not just by an application-level check.

Existing conversations (if any) are backfilled with a counter row seeded
from their current max sequence_number, so a deploy of this migration never
resets or collides with already-issued sequence numbers.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260817_0004"
down_revision: str | Sequence[str] | None = "20260816_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "conversation_sequence_counters",
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("last_sequence", sa.Integer(), server_default="0", nullable=False),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("conversation_id"),
    )
    op.execute(
        """
        INSERT INTO conversation_sequence_counters (conversation_id, last_sequence)
        SELECT c.id, COALESCE(MAX(m.sequence_number), 0)
        FROM conversations c
        LEFT JOIN messages m ON m.conversation_id = c.id
        GROUP BY c.id
        """
    )

    op.drop_index("ix_messages_conversation_sender_client_id", table_name="messages")
    op.create_unique_constraint(
        "uq_messages_conversation_sender_client",
        "messages",
        ["conversation_id", "sender_id", "client_message_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_messages_conversation_sender_client", "messages", type_="unique")
    op.create_index(
        "ix_messages_conversation_sender_client_id",
        "messages",
        ["conversation_id", "sender_id", "client_message_id"],
    )
    op.drop_table("conversation_sequence_counters")
