import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter


class ConversationJoinEvent(BaseModel):
    type: Literal["conversation:join"]
    conversation_id: uuid.UUID


class ConversationLeaveEvent(BaseModel):
    type: Literal["conversation:leave"]
    conversation_id: uuid.UUID


class MessageSendEvent(BaseModel):
    type: Literal["message:send"]
    conversation_id: uuid.UUID
    client_message_id: uuid.UUID
    body: str = Field(min_length=1, max_length=4000)


class MessageReadEvent(BaseModel):
    type: Literal["message:read"]
    conversation_id: uuid.UUID
    message_id: uuid.UUID


class MessageEditEvent(BaseModel):
    type: Literal["message:edit"]
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    body: str = Field(min_length=1, max_length=4000)


class MessageDeleteEvent(BaseModel):
    type: Literal["message:delete"]
    conversation_id: uuid.UUID
    message_id: uuid.UUID


class TypingStartEvent(BaseModel):
    type: Literal["typing:start"]
    conversation_id: uuid.UUID


class TypingStopEvent(BaseModel):
    type: Literal["typing:stop"]
    conversation_id: uuid.UUID


class PresenceHeartbeatEvent(BaseModel):
    type: Literal["presence:heartbeat"]


InboundEvent = Annotated[
    ConversationJoinEvent
    | ConversationLeaveEvent
    | MessageSendEvent
    | MessageReadEvent
    | MessageEditEvent
    | MessageDeleteEvent
    | TypingStartEvent
    | TypingStopEvent
    | PresenceHeartbeatEvent,
    Field(discriminator="type"),
]

inbound_event_adapter: TypeAdapter[InboundEvent] = TypeAdapter(InboundEvent)
