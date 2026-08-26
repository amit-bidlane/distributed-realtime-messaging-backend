import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import AuthenticatedUser, get_current_user, get_db_session
from app.contacts.schemas import ContactMatch, ContactSyncRequest, ContactSyncResponse
from app.contacts.service import match_contacts
from app.core.rate_limit import contacts_rate_limit

router = APIRouter(prefix="/contacts", tags=["contacts"])


@router.post(
    "/sync",
    response_model=ContactSyncResponse,
    dependencies=[Depends(contacts_rate_limit)],
)
async def sync_contacts(
    payload: ContactSyncRequest,
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ContactSyncResponse:
    matches = await match_contacts(
        session, uuid.UUID(current_user.user_id), payload.identifiers
    )
    return ContactSyncResponse(
        matches=[
            ContactMatch(identifier=match.identifier, user_id=match.user_id) for match in matches
        ]
    )
