from typing import Protocol

from fastapi import APIRouter, HTTPException, Request, status
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class ReadinessChecker(Protocol):
    async def check(self) -> bool: ...


class InfrastructureReadiness:
    """Checks that durable and ephemeral dependencies are reachable."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis_client: Redis,
    ) -> None:
        self._session_factory = session_factory
        self._redis_client = redis_client

    async def check(self) -> bool:
        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            await self._redis_client.ping()
        except Exception:
            return False
        return True


router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    checker = request.app.state.readiness
    if not await checker.check():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="not ready")
    return {"status": "ready"}
