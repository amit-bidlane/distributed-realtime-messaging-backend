import hmac
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import Device, User, UserSession
from app.auth.passwords import hash_password, verify_password
from app.auth.schemas import CredentialsRequest, TokenResponse
from app.auth.tokens import (
    TokenClaims,
    TokenError,
    create_access_token,
    create_refresh_token,
    decode_token,
    digest_refresh_token,
)
from app.core.config import Settings


class AuthenticationError(Exception):
    """Raised when credentials or session state cannot authenticate a request."""


class EmailAlreadyRegisteredError(Exception):
    """Raised when registration attempts to reuse an email address."""


async def register(
    session: AsyncSession, credentials: CredentialsRequest, settings: Settings
) -> TokenResponse:
    existing_user = await session.scalar(select(User).where(User.email == credentials.email))
    if existing_user is not None:
        raise EmailAlreadyRegisteredError

    user = User(email=credentials.email, password_hash=hash_password(credentials.password))
    session.add(user)
    await session.flush()
    return await _create_session_tokens(session, user, credentials.device_label, settings)


async def login(
    session: AsyncSession, credentials: CredentialsRequest, settings: Settings
) -> TokenResponse:
    user = await session.scalar(select(User).where(User.email == credentials.email))
    if user is None or not verify_password(credentials.password, user.password_hash):
        raise AuthenticationError
    return await _create_session_tokens(session, user, credentials.device_label, settings)


async def refresh(session: AsyncSession, refresh_token: str, settings: Settings) -> TokenResponse:
    try:
        claims = decode_token(refresh_token, "refresh", settings)
    except TokenError:
        raise AuthenticationError from None
    user_session = await _get_active_session(session, claims)
    if user_session is None:
        raise AuthenticationError

    expected_digest = digest_refresh_token(refresh_token, settings)
    if not hmac.compare_digest(user_session.refresh_token_digest, expected_digest):
        raise AuthenticationError

    new_refresh_token = create_refresh_token(user_session.user_id, user_session.id, settings)
    user_session.refresh_token_digest = digest_refresh_token(new_refresh_token, settings)
    user_session.last_used_at = datetime.now(UTC)
    await session.commit()
    return TokenResponse(
        access_token=create_access_token(user_session.user_id, user_session.id, settings),
        refresh_token=new_refresh_token,
    )


async def revoke(session: AsyncSession, claims: TokenClaims) -> None:
    user_session = await _get_active_session(session, claims)
    if user_session is None:
        raise AuthenticationError
    user_session.revoked_at = datetime.now(UTC)
    await session.commit()


async def get_authenticated_session(
    session: AsyncSession, token: str, settings: Settings
) -> TokenClaims:
    try:
        claims = decode_token(token, "access", settings)
    except TokenError:
        raise AuthenticationError from None
    if await _get_active_session(session, claims) is None:
        raise AuthenticationError
    return claims


async def _create_session_tokens(
    session: AsyncSession, user: User, device_label: str, settings: Settings
) -> TokenResponse:
    device = Device(user_id=user.id, label=device_label)
    session.add(device)
    await session.flush()
    user_session = UserSession(
        user_id=user.id,
        device_id=device.id,
        refresh_token_digest="pending",
        expires_at=datetime.now(UTC) + timedelta(days=settings.refresh_token_ttl_days),
    )
    session.add(user_session)
    await session.flush()
    refresh_token = create_refresh_token(user.id, user_session.id, settings)
    user_session.refresh_token_digest = digest_refresh_token(refresh_token, settings)
    await session.commit()
    return TokenResponse(
        access_token=create_access_token(user.id, user_session.id, settings),
        refresh_token=refresh_token,
    )


async def _get_active_session(session: AsyncSession, claims: TokenClaims) -> UserSession | None:
    user_session = await session.scalar(
        select(UserSession).where(
            UserSession.id == claims.session_id,
            UserSession.user_id == claims.user_id,
            UserSession.revoked_at.is_(None),
        )
    )
    if user_session is None:
        return None
    expires_at = user_session.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    if expires_at <= datetime.now(UTC):
        return None
    return user_session
