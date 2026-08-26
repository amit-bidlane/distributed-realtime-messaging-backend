"""Create conversation tables.

Revision ID: 20260814_0002
Revises: 20260814_0001
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260814_0002"
down_revision: str | Sequence[str] | None = "20260814_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    conversation_type = postgresql.ENUM(
        "DIRECT", "GROUP", name="conversation_type", create_type=False
    )
    membership_role = postgresql.ENUM(
        "OWNER", "MEMBER", name="membership_role", create_type=False
    )
    conversation_type.create(op.get_bind(), checkfirst=True)
    membership_role.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "conversations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("kind", conversation_type, nullable=False),
        sa.Column("title", sa.String(length=128), nullable=True),
        sa.Column("direct_key", sa.String(length=73), nullable=True),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("direct_key"),
    )
    op.create_index("ix_conversations_created_at_id", "conversations", ["created_at", "id"])
    op.create_table(
        "conversation_members",
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("role", membership_role, nullable=False),
        sa.Column(
            "joined_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("conversation_id", "user_id"),
    )
    op.create_index(
        "ix_conversation_members_user_conversation",
        "conversation_members",
        ["user_id", "conversation_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_conversation_members_user_conversation", table_name="conversation_members")
    op.drop_table("conversation_members")
    op.drop_index("ix_conversations_created_at_id", table_name="conversations")
    op.drop_table("conversations")
    postgresql.ENUM(name="membership_role").drop(op.get_bind(), checkfirst=True)
    postgresql.ENUM(name="conversation_type").drop(op.get_bind(), checkfirst=True)
