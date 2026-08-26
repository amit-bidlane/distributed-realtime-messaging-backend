import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.conversations.models import ConversationMember
from app.messages.models import ConversationSequenceCounter, Message, MessageReceipt
from app.messages.schemas import MessageCursor


class MissingSequenceCounterError(Exception):
    """Raised when a conversation has no counter row.

    Every conversation gets one at creation time (see
    app.conversations.service.create_conversation), so this indicates a data
    integrity bug rather than a normal, expected condition.
    """


class ReceiptError(Exception):
    """Raised when a delivery/read receipt transition is not permitted."""


class MessageMutationError(Exception):
    """Raised when an edit/delete is not permitted: the caller isn't the
    original sender, or the message can't be mutated further (e.g. editing
    one that's already deleted).
    """


@dataclass(frozen=True)
class PersistResult:
    message: Message
    is_duplicate: bool


async def persist_message(
    session: AsyncSession,
    *,
    conversation_id: uuid.UUID,
    sender_id: uuid.UUID,
    client_message_id: uuid.UUID,
    body: str,
) -> PersistResult:
    """Persist a message with a gap-free, race-safe per-conversation sequence.

    Ordering guarantee: sequence_number is assigned by a single atomic
    ``UPDATE conversation_sequence_counters ... RETURNING`` statement. The
    database's row-level write lock on that counter row serializes any two
    concurrent senders (same process or different instances) targeting the
    same conversation, so sequence numbers are always contiguous starting at
    1, with no duplicates and no skips.

    Idempotency guarantee: a unique constraint on
    (conversation_id, sender_id, client_message_id) is the source of truth
    for "have we seen this message before", enforced by PostgreSQL itself.
    The upfront SELECT below is only a fast-path optimization for the common
    case (a client retrying after a lost ACK, well after the original
    commit) that avoids burning a sequence number on an already-known
    duplicate. Under a genuine concurrent race - two sends with the same
    client_message_id whose pre-checks both miss - the constraint still
    rejects the second INSERT; that failure is caught below, the losing
    transaction is rolled back (which also reverts its counter increment, so
    no sequence number is permanently lost), and the winning row is returned
    instead.
    """
    existing = await _find_existing(session, conversation_id, sender_id, client_message_id)
    if existing is not None:
        return PersistResult(message=existing, is_duplicate=True)

    next_sequence = await session.scalar(
        update(ConversationSequenceCounter)
        .where(ConversationSequenceCounter.conversation_id == conversation_id)
        .values(last_sequence=ConversationSequenceCounter.last_sequence + 1)
        .returning(ConversationSequenceCounter.last_sequence)
    )
    if next_sequence is None:
        raise MissingSequenceCounterError(conversation_id)

    message = Message(
        conversation_id=conversation_id,
        sender_id=sender_id,
        client_message_id=client_message_id,
        sequence_number=next_sequence,
        body=body,
    )
    session.add(message)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        existing = await _find_existing(session, conversation_id, sender_id, client_message_id)
        if existing is None:
            raise
        return PersistResult(message=existing, is_duplicate=True)

    await session.commit()
    await session.refresh(message)
    return PersistResult(message=message, is_duplicate=False)


async def _find_existing(
    session: AsyncSession,
    conversation_id: uuid.UUID,
    sender_id: uuid.UUID,
    client_message_id: uuid.UUID,
) -> Message | None:
    existing: Message | None = await session.scalar(
        select(Message).where(
            Message.conversation_id == conversation_id,
            Message.sender_id == sender_id,
            Message.client_message_id == client_message_id,
        )
    )
    return existing


@dataclass(frozen=True)
class EditResult:
    message: Message
    changed: bool


async def edit_message(
    session: AsyncSession, *, message: Message, user_id: uuid.UUID, body: str
) -> EditResult:
    """Update a message's body in place. Only the original sender may edit -
    identity comes from the JWT-authenticated user_id, never anything
    client-supplied (a project design rule). A deleted message can't be resurrected
    via edit, since deletion already cleared its body server-side.

    Idempotent: re-submitting an edit that doesn't actually change the
    current body (e.g. a client retry after a lost ack) is a no-op -
    changed=False, edited_at left untouched, nothing for the caller to
    broadcast.
    """
    if message.sender_id != user_id:
        raise MessageMutationError("only the sender may edit this message")
    if message.deleted_at is not None:
        raise MessageMutationError("cannot edit a deleted message")
    if message.body == body:
        return EditResult(message=message, changed=False)

    message.body = body
    message.edited_at = datetime.now(UTC)
    await session.commit()
    await session.refresh(message)
    return EditResult(message=message, changed=True)


@dataclass(frozen=True)
class DeleteResult:
    message: Message
    changed: bool


async def delete_message(
    session: AsyncSession, *, message: Message, user_id: uuid.UUID
) -> DeleteResult:
    """Soft-delete: deleted_at is stamped and body is replaced server-side so
    the plaintext is no longer retained (project design rule: never retain plaintext
    beyond what's needed), but the row - and its sequence_number - stays, so
    no gap opens up in the conversation's ordering. Deletion is for every
    member of the conversation; there is no per-recipient "delete for me
    only" (see README's Future improvements). Only the original sender may
    delete.

    Idempotent: deleting an already-deleted message is a no-op - changed=False,
    the original deleted_at is left untouched, nothing to re-broadcast.
    """
    if message.sender_id != user_id:
        raise MessageMutationError("only the sender may delete this message")
    if message.deleted_at is not None:
        return DeleteResult(message=message, changed=False)

    message.deleted_at = datetime.now(UTC)
    message.body = ""
    await session.commit()
    await session.refresh(message)
    return DeleteResult(message=message, changed=True)


@dataclass(frozen=True)
class ReceiptResult:
    receipt: MessageReceipt
    changed: bool


async def mark_delivered(
    session: AsyncSession, *, message: Message, user_id: uuid.UUID
) -> ReceiptResult:
    """Transition (message, user_id) from no-receipt to DELIVERED.

    Idempotent: if a receipt already exists (DELIVERED or READ), this is a
    no-op that returns changed=False and leaves existing timestamps alone -
    delivery is never re-recorded once observed, and it never regresses a
    READ receipt.
    """
    if message.sender_id == user_id:
        raise ReceiptError("sender cannot receive a delivery receipt for their own message")

    receipt = await _get_or_create_receipt(session, message.id, user_id)
    if receipt.delivered_at is not None:
        return ReceiptResult(receipt=receipt, changed=False)

    receipt.delivered_at = datetime.now(UTC)
    await session.commit()
    await session.refresh(receipt)
    return ReceiptResult(receipt=receipt, changed=True)


async def mark_read(
    session: AsyncSession, *, message: Message, user_id: uuid.UUID
) -> ReceiptResult:
    """Transition (message, user_id) to READ.

    Reachable from no-receipt (skips straight to READ, also stamping
    delivered_at, since reading a message implies it was delivered even if
    the live automatic delivery step was missed - e.g. the recipient wasn't
    connected at send time) or from DELIVERED (stamps read_at, leaves the
    original delivered_at untouched). Idempotent: calling this again on an
    already-READ receipt is a no-op that returns changed=False.
    """
    if message.sender_id == user_id:
        raise ReceiptError("sender cannot mark their own message as read")

    receipt = await _get_or_create_receipt(session, message.id, user_id)
    if receipt.read_at is not None:
        return ReceiptResult(receipt=receipt, changed=False)

    now = datetime.now(UTC)
    receipt.read_at = now
    if receipt.delivered_at is None:
        receipt.delivered_at = now
    await session.commit()
    await session.refresh(receipt)
    return ReceiptResult(receipt=receipt, changed=True)


@dataclass(frozen=True)
class ReplayBatch:
    messages: list[Message]
    has_more: bool


async def get_missing_messages(
    session: AsyncSession,
    *,
    conversation_id: uuid.UUID,
    after_sequence: int,
    limit: int,
) -> ReplayBatch:
    """Messages in a conversation a member hasn't acknowledged yet, oldest first.

    Powers offline recovery: the caller replays everything with
    sequence_number > the member's last_acknowledged_sequence cursor (see
    advance_last_acknowledged_sequence), reusing the same
    (conversation_id, sequence_number) index that already guarantees
    ordering (app.messages.models.Message.__table_args__). Capped at
    `limit`; `has_more` tells the caller whether a backlog remains so it can
    request another batch (e.g. by rejoining once caught up on this one)
    rather than the caller silently receiving a truncated view.
    """
    rows = await session.scalars(
        select(Message)
        .where(
            Message.conversation_id == conversation_id,
            Message.sequence_number > after_sequence,
        )
        .order_by(Message.sequence_number)
        .limit(limit + 1)
    )
    messages = list(rows.all())
    has_more = len(messages) > limit
    return ReplayBatch(messages=messages[:limit], has_more=has_more)


@dataclass(frozen=True)
class MessageHistoryPage:
    messages: list[Message]
    next_cursor: MessageCursor | None


async def get_conversation_history(
    session: AsyncSession,
    *,
    conversation_id: uuid.UUID,
    limit: int,
    cursor: MessageCursor | None,
) -> MessageHistoryPage:
    """Cursor-paginated history for GET /conversations/{id}/messages, newest
    first (sequence_number descending) - the REST counterpart to
    get_missing_messages's oldest-first replay batch. Different order is
    fine: the two serve different purposes (paging arbitrarily far back vs.
    "what have I missed"), and each documents its own order rather than
    being forced to match the other. Membership is checked by the caller
    (see app.conversations.router) before this runs, the same
    get-then-authorize split used throughout this codebase.
    """
    statement = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.sequence_number.desc())
        .limit(limit + 1)
    )
    if cursor is not None:
        statement = statement.where(Message.sequence_number < cursor.sequence_number)

    rows = list((await session.scalars(statement)).all())
    has_next_page = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = None
    if has_next_page:
        next_cursor = MessageCursor(sequence_number=page_rows[-1].sequence_number)
    return MessageHistoryPage(messages=page_rows, next_cursor=next_cursor)


async def advance_last_acknowledged_sequence(
    session: AsyncSession,
    *,
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
    sequence_number: int,
) -> None:
    """Move a member's replay cursor forward; never backward.

    Reuses the conversation_members row created at membership time (see
    app.conversations.service.create_conversation / add_member) instead of a
    parallel per-member tracking table. A single atomic, guarded UPDATE -
    safe under concurrent callers (e.g. two devices for the same user
    acknowledging out of order, or a live delivery race with a replay on
    reconnect) since the WHERE clause makes a would-be regression a no-op
    rather than a read-modify-write race.
    """
    await session.execute(
        update(ConversationMember)
        .where(
            ConversationMember.conversation_id == conversation_id,
            ConversationMember.user_id == user_id,
            ConversationMember.last_acknowledged_sequence < sequence_number,
        )
        .values(last_acknowledged_sequence=sequence_number)
    )
    await session.commit()


async def _get_or_create_receipt(
    session: AsyncSession, message_id: uuid.UUID, user_id: uuid.UUID
) -> MessageReceipt:
    receipt = await session.get(MessageReceipt, {"message_id": message_id, "user_id": user_id})
    if receipt is not None:
        return receipt

    receipt = MessageReceipt(message_id=message_id, user_id=user_id)
    try:
        async with session.begin_nested():
            session.add(receipt)
            await session.flush()
    except IntegrityError:
        # Lost a race with another delivery/read write for the same recipient
        # (e.g. automatic delivery marking racing a fast client read). Only the
        # SAVEPOINT is rolled back, not the whole session, so objects the
        # caller already loaded stay usable. The winner's row is used instead.
        receipt = await session.get(MessageReceipt, {"message_id": message_id, "user_id": user_id})
        if receipt is None:
            raise
    return receipt
