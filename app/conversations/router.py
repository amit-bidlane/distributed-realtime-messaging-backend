import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import AuthenticatedUser, get_current_user, get_db_session
from app.conversations.models import Conversation, ConversationMember
from app.conversations.schemas import (
    ConversationCreate,
    ConversationCursor,
    ConversationDetail,
    ConversationPage,
    ConversationSummary,
    MembershipCreate,
    MembershipResponse,
)
from app.conversations.service import (
    ConversationAccessError,
    MembershipManagementError,
    MemberUnavailableError,
    add_member,
    create_conversation,
    get_conversation_for_member,
    list_conversations,
    remove_member,
)
from app.messages.schemas import MessageCursor, MessageOut, MessagePage
from app.messages.service import get_conversation_history

router = APIRouter(prefix="/conversations", tags=["conversations"])


def _summary(conversation: Conversation, member_count: int) -> ConversationSummary:
    return ConversationSummary(
        id=conversation.id,
        kind=conversation.kind,
        title=conversation.title,
        created_at=conversation.created_at,
        member_count=member_count,
    )


def _membership_response(membership: ConversationMember) -> MembershipResponse:
    return MembershipResponse(
        user_id=membership.user_id,
        role=membership.role,
        joined_at=membership.joined_at,
    )


@router.post("", response_model=ConversationSummary, status_code=status.HTTP_201_CREATED)
async def create_new_conversation(
    payload: ConversationCreate,
    response: Response,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ConversationSummary:
    try:
        conversation, created = await create_conversation(
            session, uuid.UUID(current_user.user_id), payload
        )
    except MemberUnavailableError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="member unavailable"
        ) from None
    except MembershipManagementError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="invalid members"
        ) from None
    if not created:
        response.status_code = status.HTTP_200_OK
    return _summary(conversation, len(payload.member_ids) + 1)


@router.get("", response_model=ConversationPage)
async def get_conversations(
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
) -> ConversationPage:
    try:
        parsed_cursor = ConversationCursor.decode(cursor) if cursor else None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="invalid cursor"
        ) from None
    page = await list_conversations(session, uuid.UUID(current_user.user_id), limit, parsed_cursor)
    return ConversationPage(
        items=[_summary(conversation, count) for conversation, count in page.rows],
        next_cursor=page.next_cursor.encode() if page.next_cursor else None,
    )


@router.get("/{conversation_id}", response_model=ConversationDetail)
async def get_conversation(
    conversation_id: uuid.UUID,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ConversationDetail:
    try:
        conversation = await get_conversation_for_member(
            session, conversation_id, uuid.UUID(current_user.user_id)
        )
    except ConversationAccessError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="conversation not found"
        ) from None
    return ConversationDetail(
        **_summary(conversation, len(conversation.members)).model_dump(),
        members=[_membership_response(member) for member in conversation.members],
    )


@router.get("/{conversation_id}/messages", response_model=MessagePage)
async def get_conversation_messages(
    conversation_id: uuid.UUID,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
) -> MessagePage:
    """History paging for a client without a live WebSocket connection (e.g.
    loading a web view before connecting), independent of and complementary
    to message:replay - see README's Offline recovery section for how the
    two mechanisms differ. Newest first (sequence_number descending); see
    get_conversation_history for why that differs from replay's oldest-first
    order.
    """
    try:
        await get_conversation_for_member(session, conversation_id, uuid.UUID(current_user.user_id))
    except ConversationAccessError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="conversation not found"
        ) from None
    try:
        parsed_cursor = MessageCursor.decode(cursor) if cursor else None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="invalid cursor"
        ) from None
    page = await get_conversation_history(
        session, conversation_id=conversation_id, limit=limit, cursor=parsed_cursor
    )
    return MessagePage(
        items=[MessageOut.from_message(message) for message in page.messages],
        next_cursor=page.next_cursor.encode() if page.next_cursor else None,
    )


@router.post(
    "/{conversation_id}/members",
    response_model=MembershipResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_conversation_member(
    conversation_id: uuid.UUID,
    payload: MembershipCreate,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MembershipResponse:
    try:
        membership = await add_member(
            session, conversation_id, uuid.UUID(current_user.user_id), payload.user_id
        )
    except ConversationAccessError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="conversation not found"
        ) from None
    except MemberUnavailableError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="member unavailable"
        ) from None
    except MembershipManagementError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="membership change denied"
        ) from None
    return _membership_response(membership)


@router.delete("/{conversation_id}/members/{member_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_conversation_member(
    conversation_id: uuid.UUID,
    member_id: uuid.UUID,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    try:
        await remove_member(session, conversation_id, uuid.UUID(current_user.user_id), member_id)
    except ConversationAccessError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="conversation not found"
        ) from None
    except MembershipManagementError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="membership change denied"
        ) from None
