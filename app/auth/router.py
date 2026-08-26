import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import AuthenticatedUser, get_current_user, get_db_session
from app.auth.schemas import CredentialsRequest, RefreshRequest, TokenResponse, UserResponse
from app.auth.service import (
    AuthenticationError,
    EmailAlreadyRegisteredError,
    login,
    refresh,
    register,
    revoke,
)
from app.core.config import Settings
from app.core.rate_limit import login_rate_limit, register_rate_limit

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(register_rate_limit)],
)
async def register_user(
    credentials: CredentialsRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TokenResponse:
    settings: Settings = request.app.state.settings
    try:
        return await register(session, credentials, settings)
    except EmailAlreadyRegisteredError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="email already registered"
        ) from None


@router.post("/login", response_model=TokenResponse, dependencies=[Depends(login_rate_limit)])
async def login_user(
    credentials: CredentialsRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TokenResponse:
    settings: Settings = request.app.state.settings
    try:
        return await login(session, credentials, settings)
    except AuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials"
        ) from None


@router.post("/refresh", response_model=TokenResponse)
async def refresh_tokens(
    payload: RefreshRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TokenResponse:
    settings: Settings = request.app.state.settings
    try:
        return await refresh(session, payload.refresh_token, settings)
    except AuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid refresh token"
        ) from None


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    from app.auth.tokens import TokenClaims

    try:
        await revoke(
            session,
            TokenClaims(
                user_id=uuid.UUID(current_user.user_id),
                session_id=uuid.UUID(current_user.session_id),
                token_type="access",
            ),
        )
    except AuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials"
        ) from None


@router.get("/me", response_model=UserResponse)
async def current_user_profile(
    current_user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> UserResponse:
    return UserResponse(id=current_user.user_id)
