import base64
import binascii
import json
import uuid
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from app.conversations.models import ConversationType, MembershipRole


class ConversationCreate(BaseModel):
    kind: ConversationType
    member_ids: list[uuid.UUID] = Field(min_length=1, max_length=100)
    title: str | None = Field(default=None, max_length=128)

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must not be blank")
        return normalized

    @model_validator(mode="after")
    def validate_conversation_shape(self) -> "ConversationCreate":
        if len(set(self.member_ids)) != len(self.member_ids):
            raise ValueError("member_ids must be unique")
        if self.kind is ConversationType.DIRECT:
            if len(self.member_ids) != 1 or self.title is not None:
                raise ValueError("direct conversations require one member and no title")
        elif self.title is None:
            raise ValueError("group conversations require a title")
        return self


class MembershipCreate(BaseModel):
    user_id: uuid.UUID


class MembershipResponse(BaseModel):
    user_id: uuid.UUID
    role: MembershipRole
    joined_at: datetime


class ConversationSummary(BaseModel):
    id: uuid.UUID
    kind: ConversationType
    title: str | None
    created_at: datetime
    member_count: int


class ConversationDetail(ConversationSummary):
    members: list[MembershipResponse]


class ConversationPage(BaseModel):
    items: list[ConversationSummary]
    next_cursor: str | None


class ConversationCursor(BaseModel):
    created_at: datetime
    conversation_id: uuid.UUID

    @classmethod
    def decode(cls, cursor: str) -> "ConversationCursor":
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
            value = cls.model_validate(payload)
        except (ValueError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError):
            raise ValueError("invalid cursor") from None
        return value

    def encode(self) -> str:
        payload = {
            "created_at": self.created_at.isoformat(),
            "conversation_id": str(self.conversation_id),
        }
        encoded = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode()
        ).decode()
        return encoded.rstrip("=")
