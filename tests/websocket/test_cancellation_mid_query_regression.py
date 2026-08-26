"""Verifies the conftest.py shield defers a cancel landing mid-INSERT:
with the shield the messages INSERT completes and commits
(message_count == 1); without it the INSERT is abandoned
(message_count == 0).

A variant that cancels a real statement already running in aiosqlite's
worker thread is not committed here - a thread-method timeout kill on
that variant would abort the whole pytest run, not just one test."""

import asyncio
import threading
import uuid
from pathlib import Path

import aiosqlite
import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.messages.models import Message

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"
INSERT_DELAY_SECONDS = 1.0


def _patch_cursor_execute_to_stall_on_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> threading.Event:
    """Makes the aiosqlite cursor's execute() for the messages INSERT await
    asyncio.sleep() before running the real statement, signalling
    `insert_started` right before doing so, so a cancel can land while the
    INSERT is in flight."""
    insert_started = threading.Event()
    real_execute = aiosqlite.Cursor.execute
    triggered = False

    async def patched_execute(self: aiosqlite.Cursor, sql: str, parameters=None):
        nonlocal triggered
        if not triggered and "INSERT INTO messages" in sql:
            triggered = True
            insert_started.set()
            await asyncio.sleep(INSERT_DELAY_SECONDS)
        return await real_execute(self, sql, parameters)

    monkeypatch.setattr(aiosqlite.Cursor, "execute", patched_execute)
    return insert_started


def _build_app(tmp_path: Path, name: str) -> FastAPI:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / name}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())
    return application


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


def create_direct_conversation(
    client: TestClient, owner_tokens: dict[str, str], member_id: uuid.UUID
) -> str:
    response = client.post(
        "/conversations",
        headers=auth_header(owner_tokens),
        json={"kind": "direct", "member_ids": [str(member_id)]},
    )
    assert response.status_code == 201
    return str(response.json()["id"])


def test_cancel_mid_insert_on_websocket_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exiting a websocket `with` block while its handler is mid-INSERT
    must be safe when the conftest shield is in place."""
    insert_started = _patch_cursor_execute_to_stall_on_insert(monkeypatch)
    application = _build_app(tmp_path, "cancel_mid_insert.db")

    with TestClient(application) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()  # auth:authenticated
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()  # conversation:joined
            owner_ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": str(uuid.uuid4()),
                    "body": "hello",
                }
            )
            assert insert_started.wait(timeout=5), "INSERT never reached the cursor"
            # The handler's task is now genuinely awaiting the slow INSERT
            # on its session's connection. Exit immediately, without
            # reading message:new or message:ack: WebSocketTestSession
            # cancels the handler's task right now, deterministically
            # mid-query.

        # Positive check: the shield let the INSERT actually finish rather
        # than just avoiding an exception/hang.
        async def count_messages() -> int:
            async with application.state.session_factory() as session:
                rows = (await session.scalars(select(Message))).all()
                return len(rows)

        message_count = asyncio.run(count_messages())

    asyncio.run(application.state.db_engine.dispose())
    assert message_count == 1
