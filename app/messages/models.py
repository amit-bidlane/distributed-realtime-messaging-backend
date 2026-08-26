import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy import func as sql_func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        Index("ix_messages_conversation_sequence", "conversation_id", "sequence_number"),
        UniqueConstraint(
            "conversation_id",
            "sender_id",
            "client_message_id",
            name="uq_messages_conversation_sender_client",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE")
    )
    sender_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    client_message_id: Mapped[uuid.UUID] = mapped_column()
    sequence_number: Mapped[int] = mapped_column(Integer)
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=sql_func.now(),
        nullable=False,
    )
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MessageReceipt(Base):
    """Per-recipient delivery/read state for a message.

    Row absent: not yet delivered to this recipient. ``delivered_at`` set,
    ``read_at`` null: DELIVERED. ``read_at`` set (which always implies
    ``delivered_at`` is also set): READ. See app.messages.service.mark_delivered
    / mark_read for the transition rules; there is no separate SENT marker
    here since that state belongs to the message itself (a persisted row in
    ``messages`` *is* "sent" - see app.messages.service.persist_message).
    """

    __tablename__ = "message_receipts"

    message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("messages.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ConversationSequenceCounter(Base):
    """One row per conversation; source of truth for the next sequence_number.

    Created alongside its conversation (see app.conversations.service) so a
    row always exists before any client could send a message. Incremented via
    a single atomic ``UPDATE ... RETURNING`` (see app.messages.service), which
    lets the database's own row-level write lock serialize concurrent senders
    on the same conversation without an explicit SELECT ... FOR UPDATE step.
    """

    __tablename__ = "conversation_sequence_counters"

    conversation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), primary_key=True
    )
    last_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
