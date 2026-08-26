from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    engine_kwargs: dict[str, Any] = {
        "pool_pre_ping": True,
        # See app.core.logging.JsonFormatter - hide_parameters stops bound
        # SQL parameters (e.g. message plaintext) from leaking into
        # exception tracebacks that logger.exception(...) captures.
        "hide_parameters": True,
    }

    if settings.database_url.startswith("sqlite"):
        # aiosqlite connections aren't safe to share across the separate
        # anyio blocking-portal threads that TestClient.websocket_connect()
        # creates per connection (each test's websocket tests run under
        # their own portal thread). Pooling can hand a connection checked
        # out under one portal's event loop to another, racing its
        # asyncio.shield()-wrapped close against a new checkout and
        # deadlocking - this is what caused the CI hang in
        # test_offline_recovery.py. NullPool opens a fresh connection per
        # checkout instead, which sidesteps the race entirely. NullPool is
        # not the only hang cause; see the session shield in
        # tests/websocket/conftest.py. pool_size/max_overflow only apply to
        # the production Postgres pool, so they don't belong here
        # regardless.
        engine_kwargs["poolclass"] = NullPool
    else:
        engine_kwargs["pool_size"] = settings.db_pool_size
        engine_kwargs["max_overflow"] = settings.db_max_overflow

    return create_async_engine(settings.database_url, **engine_kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)