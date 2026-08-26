import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class ConversationType(StrEnum):
    DIRECT = "direct"
    GROUP = "group"


class MembershipRole(StrEnum):
    OWNER = "owner"
    MEMBER = "member"


class Conversation(Base):
    __tablename__ = "conversations"
    __table_args__ = (Index("ix_conversations_created_at_id", "created_at", "id"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    kind: Mapped[ConversationType] = mapped_column(Enum(ConversationType, name="conversation_type"))
    title: Mapped[str | None] = mapped_column(String(128))
    direct_key: Mapped[str | None] = mapped_column(String(73), unique=True)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        nullable=False,
    )

    members: Mapped[list["ConversationMember"]] = relationship(
        back_populates="conversation", cascade="all, delete-orphan"
    )


class ConversationMember(Base):
    __tablename__ = "conversation_members"
    __table_args__ = (
        Index("ix_conversation_members_user_conversation", "user_id", "conversation_id"),
    )

    conversation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    role: Mapped[MembershipRole] = mapped_column(
        Enum(MembershipRole, name="membership_role"), default=MembershipRole.MEMBER
    )
    joined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # Offline-recovery replay cursor: the highest messages.sequence_number in
    # this conversation this member has acknowledged (sent themselves, or
    # been marked DELIVERED for - live or via replay). See
    # app.messages.service.advance_last_acknowledged_sequence /
    # get_missing_messages. Reuses this membership row rather than a
    # separate per-member tracking table.
    last_acknowledged_sequence: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )

    conversation: Mapped[Conversation] = relationship(back_populates="members")
