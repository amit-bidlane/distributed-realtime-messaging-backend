import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.presence.service import PresenceSnapshot, get_presence

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"
HEARTBEAT_TTL_SECONDS = 30


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'presence.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        presence_heartbeat_ttl_seconds=HEARTBEAT_TTL_SECONDS,
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())
    yield application
    asyncio.run(application.state.db_engine.dispose())


@pytest.fixture
def app_null_pool(tmp_path: Path) -> Iterator[FastAPI]:
    """Same as `app`, but the pooled sqlite engine is swapped for NullPool.

    Only test_activity_refreshes_heartbeat needs this - see the comment on
    that test for why. NullPool never reuses a connection across checkouts,
    which avoids the pool ever needing to invalidate/terminate one, so it's
    a targeted, test-only workaround rather than a change to how the app
    itself configures its engine (see app.db.session.create_engine, which
    is unaffected by this fixture).
    """
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'presence.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        presence_heartbeat_ttl_seconds=HEARTBEAT_TTL_SECONDS,
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))
    null_pool_engine = create_async_engine(
        settings.database_url, poolclass=NullPool, hide_parameters=True
    )
    application.state.db_engine = null_pool_engine
    application.state.session_factory = async_sessionmaker(null_pool_engine, expire_on_commit=False)

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())
    yield application
    asyncio.run(application.state.db_engine.dispose())


def register_user(client: TestClient, email: str) -> tuple[dict[str, str], uuid.UUID]:
    response = client.post(
        "/auth/register",
        json={"email": email, "password": PASSWORD, "device_label": "Test browser"},
    )
    assert response.status_code == 201
    tokens = response.json()
    profile = client.get("/auth/me", headers={"Authorization": f"Bearer {tokens['access_token']}"})
    assert profile.status_code == 200
    return tokens, uuid.UUID(profile.json()["id"])


def auth_header(tokens: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _presence(app: FastAPI, user_id: uuid.UUID) -> PresenceSnapshot:
    return await get_presence(
        app.state.redis, user_id, heartbeat_ttl_seconds=HEARTBEAT_TTL_SECONDS
    )


def test_connect_marks_user_online(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens, user_id = register_user(client, "owner@example.com")
        with client.websocket_connect("/ws", headers=auth_header(tokens)) as ws:
            ws.receive_json()
            snapshot = client.portal.call(_presence, app, user_id)

    assert snapshot.online is True
    assert snapshot.active_connection_count == 1


def test_disconnect_marks_user_offline_and_stamps_last_seen(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens, user_id = register_user(client, "owner@example.com")
        with client.websocket_connect("/ws", headers=auth_header(tokens)) as ws:
            ws.receive_json()

        snapshot = client.portal.call(_presence, app, user_id)

    assert snapshot.online is False
    assert snapshot.active_connection_count == 0
    assert snapshot.last_seen is not None


def test_multi_device_stays_online_until_every_connection_closes(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens, user_id = register_user(client, "owner@example.com")

        with client.websocket_connect("/ws", headers=auth_header(tokens)) as tab_a:
            tab_a.receive_json()
            with client.websocket_connect("/ws", headers=auth_header(tokens)) as tab_b:
                tab_b.receive_json()
                both_open = client.portal.call(_presence, app, user_id)
                assert both_open.online is True
                assert both_open.active_connection_count == 2

            # tab_b closed; tab_a is still open.
            one_open = client.portal.call(_presence, app, user_id)
            assert one_open.online is True
            assert one_open.active_connection_count == 1

        # Both tabs closed now.
        none_open = client.portal.call(_presence, app, user_id)
        assert none_open.online is False
        assert none_open.active_connection_count == 0


def test_activity_refreshes_heartbeat(app_null_pool: FastAPI) -> None:
    """Known flake note: this specific test (connect, then immediately
    conversation:join and tear down) can intermittently hang under the
    sqlite+aiosqlite test engine. The hang goes through a
    SQLAlchemy-internal cleanup coroutine - _terminate_graceful_close in
    sqlalchemy/dialects/sqlite/aiosqlite.py - which SQLAlchemy wraps in
    asyncio.shield() (deliberately uncancellable) and which can wedge when
    a cancel lands mid-query. The exact wedge point was not isolated.
    Confirmed via cross-platform testing (native Windows and a
    Linux container) that the hang reproduces identically on both - so it
    is not a Windows/ProactorEventLoop quirk - and, before the session
    shield was added, via 16 clean runs against real PostgreSQL/asyncpg
    (the actual production driver, using the docker-compose postgres
    service) that it did not reproduce there (a small sample, and a
    cancel mid-query was not forced). Giving this one test a
    NullPool engine via the app_null_pool fixture did not fully prevent
    the hang: it recurred in this test even with NullPool in place. The
    actual mitigation is the autouse session shield in
    tests/websocket/conftest.py, which defers a cancel until the session
    closes.
    """
    with TestClient(app_null_pool) as client:
        owner_tokens, owner_id = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation = client.post(
            "/conversations",
            headers=auth_header(owner_tokens),
            json={"kind": "direct", "member_ids": [str(member_id)]},
        )
        conversation_id = conversation.json()["id"]

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as ws:
            ws.receive_json()
            after_connect = client.portal.call(_presence, app_null_pool, owner_id)

            ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            ws.receive_json()
            after_activity = client.portal.call(_presence, app_null_pool, owner_id)

    assert after_connect.last_seen is not None
    assert after_activity.last_seen is not None
    assert after_activity.last_seen >= after_connect.last_seen


def test_presence_heartbeat_event_is_accepted_silently(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens, _ = register_user(client, "owner@example.com")
        with client.websocket_connect("/ws", headers=auth_header(tokens)) as ws:
            ws.receive_json()
            ws.send_json({"type": "presence:heartbeat"})

            # No reply to the heartbeat itself: the next thing on the wire
            # is the response to whatever we send next.
            ws.send_json({"type": "conversation:leave", "conversation_id": str(uuid.uuid4())})
            reply = ws.receive_json()

    assert reply["type"] == "error"
    assert reply["detail"] == "not joined"
