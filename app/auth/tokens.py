import hashlib
import hmac
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import jwt
from jwt import ExpiredSignatureError, InvalidTokenError

from app.core.config import Settings

ALGORITHM = "HS256"


class TokenError(Exception):
    """Raised when a JWT is invalid, expired, or has an unexpected shape."""


@dataclass(frozen=True)
class TokenClaims:
    user_id: uuid.UUID
    session_id: uuid.UUID
    token_type: str


def _create_token(
    *,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
    token_type: str,
    lifetime: timedelta,
    settings: Settings,
) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "sid": str(session_id),
        "typ": token_type,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + lifetime,
    }
    return jwt.encode(payload, settings.jwt_secret.get_secret_value(), algorithm=ALGORITHM)


def create_access_token(user_id: uuid.UUID, session_id: uuid.UUID, settings: Settings) -> str:
    return _create_token(
        user_id=user_id,
        session_id=session_id,
        token_type="access",
        lifetime=timedelta(minutes=settings.access_token_ttl_minutes),
        settings=settings,
    )


def create_refresh_token(user_id: uuid.UUID, session_id: uuid.UUID, settings: Settings) -> str:
    return _create_token(
        user_id=user_id,
        session_id=session_id,
        token_type="refresh",
        lifetime=timedelta(days=settings.refresh_token_ttl_days),
        settings=settings,
    )


def decode_token(token: str, expected_type: str, settings: Settings) -> TokenClaims:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[ALGORITHM],
            options={"require": ["sub", "sid", "typ", "exp", "jti"]},
        )
        if payload["typ"] != expected_type:
            raise TokenError
        return TokenClaims(
            user_id=uuid.UUID(payload["sub"]),
            session_id=uuid.UUID(payload["sid"]),
            token_type=payload["typ"],
        )
    except (ExpiredSignatureError, InvalidTokenError, KeyError, ValueError) as error:
        raise TokenError from error


def digest_refresh_token(token: str, settings: Settings) -> str:
    pepper = settings.refresh_token_pepper.get_secret_value().encode()
    return hmac.new(pepper, token.encode(), hashlib.sha256).hexdigest()
