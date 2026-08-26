import base64
import binascii
import json
import uuid
from datetime import datetime

from pydantic import BaseModel

from app.messages.models import Message


class MessageOut(BaseModel):
    """A message's *current* state - the single representation shared by
    every read path (WebSocket message:replay and this REST history
    endpoint), so a client never sees stale content through one path that
    was already corrected through another. See from_message.
    """

    id: uuid.UUID
    conversation_id: uuid.UUID
    sender_id: uuid.UUID
    sequence_number: int
    created_at: datetime
    body: str | None = None
    edited_at: datetime | None = None
    deleted_at: datetime | None = None

    @classmethod
    def from_message(cls, message: Message) -> "MessageOut":
        """Deleted rows omit body/edited_at and carry deleted_at instead;
        everything else carries its current (possibly edited) body. Same
        branching as message:edited/message:deleted's live broadcast shape -
        see app.websocket.router._replay_item, which delegates here.
        """
        if message.deleted_at is not None:
            return cls(
                id=message.id,
                conversation_id=message.conversation_id,
                sender_id=message.sender_id,
                sequence_number=message.sequence_number,
                created_at=message.created_at,
                deleted_at=message.deleted_at,
            )
        return cls(
            id=message.id,
            conversation_id=message.conversation_id,
            sender_id=message.sender_id,
            sequence_number=message.sequence_number,
            created_at=message.created_at,
            body=message.body,
            edited_at=message.edited_at,
        )


class MessagePage(BaseModel):
    items: list[MessageOut]
    next_cursor: str | None


class MessageCursor(BaseModel):
    """Opaque pagination cursor keyed on sequence_number, the same field
    GET /conversations/{id}/messages orders by (see the router) - kept
    single-field since sequence_number is already unique per conversation,
    unlike ConversationCursor which needs a tiebreaker across conversations.
    """

    sequence_number: int

    @classmethod
    def decode(cls, cursor: str) -> "MessageCursor":
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
            value = cls.model_validate(payload)
        except (ValueError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError):
            raise ValueError("invalid cursor") from None
        return value

    def encode(self) -> str:
        payload = {"sequence_number": self.sequence_number}
        encoded = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode()
        ).decode()
        return encoded.rstrip("=")
