"""Test-only mitigation for a starlette.testclient hazard: leaving a
`with client.websocket_connect(...)` block cancels the handler's task through
an anyio CancelScope. If that lands while a DB query is in flight, SQLAlchemy
invalidates the connection and runs aiosqlite's asyncio.shield()-wrapped
terminate path; the handler's task was then observed never to finish, hanging
the run. The exact wedge point was not isolated.

This fixture shields every AsyncSession for its whole lifetime, so such a
cancel is deferred until the session closes. It patches the base class (not one
session_factory) because some fixtures, e.g. test_presence.py's app_null_pool,
build their own async_sessionmaker. No app/ changes."""

import sys
from typing import Any

import anyio
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

_real_aenter = AsyncSession.__aenter__
_real_aexit = AsyncSession.__aexit__


async def _shielded_aenter(self: AsyncSession) -> AsyncSession:
    scope = anyio.CancelScope(shield=True)
    scope.__enter__()
    try:
        session = await _real_aenter(self)
    except BaseException:
        scope.__exit__(*sys.exc_info())
        raise
    self._test_shield_scope = scope  # type: ignore[attr-defined]
    return session


async def _shielded_aexit(self: AsyncSession, *exc_info: Any) -> None:
    scope = getattr(self, "_test_shield_scope", None)
    self._test_shield_scope = None  # type: ignore[attr-defined]
    try:
        await _real_aexit(self, *exc_info)
    finally:
        if scope is not None:
            scope.__exit__(*exc_info)


@pytest.fixture(autouse=True)
def _shield_test_db_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AsyncSession, "__aenter__", _shielded_aenter)
    monkeypatch.setattr(AsyncSession, "__aexit__", _shielded_aexit)
