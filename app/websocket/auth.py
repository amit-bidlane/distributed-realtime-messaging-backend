from fastapi import WebSocket, WebSocketException, status
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth.service import AuthenticationError, get_authenticated_session
from app.auth.tokens import TokenClaims
from app.core.config import Settings


async def authenticate_websocket(
    websocket: WebSocket,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> TokenClaims:
    authorization = websocket.headers.get("authorization")
    if authorization is None:
        raise _websocket_unauthorized()
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or token.strip() != token:
        raise _websocket_unauthorized()
    async with session_factory() as session:
        try:
            return await get_authenticated_session(session, token, settings)
        except AuthenticationError:
            raise _websocket_unauthorized() from None


def _websocket_unauthorized() -> WebSocketException:
    return WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="unauthorized")
