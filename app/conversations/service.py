import uuid
from dataclasses import dataclass

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.models import User
from app.conversations.models import (
    Conversation,
    ConversationMember,
    ConversationType,
    MembershipRole,
)
from app.conversations.schemas import ConversationCreate, ConversationCursor
from app.messages.models import ConversationSequenceCounter


class ConversationAccessError(Exception):
    """Raised when a conversation is unavailable to the current user."""


class MembershipManagementError(Exception):
    """Raised when a member operation is invalid or unauthorized."""


class MemberUnavailableError(Exception):
    """Raised when a requested member does not have an account."""


@dataclass(frozen=True)
class ConversationPageResult:
    rows: list[tuple[Conversation, int]]
    next_cursor: ConversationCursor | None


def _direct_key(user_ids: set[uuid.UUID]) -> str:
    return ":".join(sorted(str(user_id) for user_id in user_ids))


async def create_conversation(
    session: AsyncSession,
    creator_id: uuid.UUID,
    payload: ConversationCreate,
) -> tuple[Conversation, bool]:
    requested_members = set(payload.member_ids)
    if creator_id in requested_members:
        raise MembershipManagementError
    member_ids = requested_members | {creator_id}
    await _validate_users_exist(session, member_ids)

    direct_key = _direct_key(member_ids) if payload.kind is ConversationType.DIRECT else None
    if direct_key is not None:
        existing = await session.scalar(
            select(Conversation).where(Conversation.direct_key == direct_key)
        )
        if existing is not None:
            return existing, False

    conversation = Conversation(
        kind=payload.kind,
        title=payload.title,
        direct_key=direct_key,
        created_by=creator_id,
    )
    session.add(conversation)
    await session.flush()
    session.add_all(
        ConversationMember(
            conversation_id=conversation.id,
            user_id=user_id,
            role=MembershipRole.OWNER if user_id == creator_id else MembershipRole.MEMBER,
        )
        for user_id in member_ids
    )
    # Created in the same transaction as the conversation, so a row always
    # exists before any client could learn the conversation_id and send a
    # message into it. See app.messages.service.persist_message.
    session.add(ConversationSequenceCounter(conversation_id=conversation.id, last_sequence=0))
    await session.commit()
    await session.refresh(conversation)
    return conversation, True


async def get_conversation_for_member(
    session: AsyncSession, conversation_id: uuid.UUID, user_id: uuid.UUID
) -> Conversation:
    conversation = await session.scalar(
        select(Conversation)
        .join(ConversationMember)
        .where(
            Conversation.id == conversation_id,
            ConversationMember.user_id == user_id,
        )
        .options(selectinload(Conversation.members))
    )
    if conversation is None:
        raise ConversationAccessError
    return conversation


async def list_conversations(
    session: AsyncSession,
    user_id: uuid.UUID,
    limit: int,
    cursor: ConversationCursor | None,
) -> ConversationPageResult:
    member_count = (
        select(func.count(ConversationMember.user_id))
        .where(ConversationMember.conversation_id == Conversation.id)
        .correlate(Conversation)
        .scalar_subquery()
    )
    statement = (
        select(Conversation, member_count)
        .join(ConversationMember)
        .where(ConversationMember.user_id == user_id)
        .order_by(Conversation.created_at.desc(), Conversation.id.desc())
        .limit(limit + 1)
    )
    if cursor is not None:
        statement = statement.where(
            or_(
                Conversation.created_at < cursor.created_at,
                and_(
                    Conversation.created_at == cursor.created_at,
                    Conversation.id < cursor.conversation_id,
                ),
            )
        )
    rows = list((await session.execute(statement)).tuples())
    has_next_page = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = None
    if has_next_page:
        last_conversation, _ = page_rows[-1]
        next_cursor = ConversationCursor(
            created_at=last_conversation.created_at,
            conversation_id=last_conversation.id,
        )
    return ConversationPageResult(rows=page_rows, next_cursor=next_cursor)


async def add_member(
    session: AsyncSession,
    conversation_id: uuid.UUID,
    actor_id: uuid.UUID,
    member_id: uuid.UUID,
) -> ConversationMember:
    conversation = await get_conversation_for_member(session, conversation_id, actor_id)
    await _require_group_owner(session, conversation, actor_id)
    await _validate_users_exist(session, {member_id})
    existing_membership = await session.get(
        ConversationMember, {"conversation_id": conversation_id, "user_id": member_id}
    )
    if existing_membership is not None:
        raise MembershipManagementError
    membership = ConversationMember(
        conversation_id=conversation_id,
        user_id=member_id,
        role=MembershipRole.MEMBER,
    )
    session.add(membership)
    await session.commit()
    await session.refresh(membership)
    return membership


async def remove_member(
    session: AsyncSession,
    conversation_id: uuid.UUID,
    actor_id: uuid.UUID,
    member_id: uuid.UUID,
) -> None:
    conversation = await get_conversation_for_member(session, conversation_id, actor_id)
    await _require_group_owner(session, conversation, actor_id)
    membership = await session.get(
        ConversationMember, {"conversation_id": conversation_id, "user_id": member_id}
    )
    if membership is None or membership.role is MembershipRole.OWNER:
        raise MembershipManagementError
    await session.delete(membership)
    await session.commit()


async def _validate_users_exist(session: AsyncSession, user_ids: set[uuid.UUID]) -> None:
    existing_ids = set((await session.scalars(select(User.id).where(User.id.in_(user_ids)))).all())
    if existing_ids != user_ids:
        raise MemberUnavailableError


async def _require_group_owner(
    session: AsyncSession, conversation: Conversation, actor_id: uuid.UUID
) -> None:
    if conversation.kind is not ConversationType.GROUP:
        raise MembershipManagementError
    membership = await session.get(
        ConversationMember,
        {"conversation_id": conversation.id, "user_id": actor_id},
    )
    if membership is None or membership.role is not MembershipRole.OWNER:
        raise MembershipManagementError
