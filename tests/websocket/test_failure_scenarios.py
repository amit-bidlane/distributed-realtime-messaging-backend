import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis.exceptions
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


class _ToggleableRedis:
    """Wraps a real (fake) Redis client; once `.broken` is True, every
    command this codebase actually calls raises the same exception class
    redis-py raises for a genuine connection failure - standing in for
    Redis becoming unreachable mid-session, so the tests below characterize
    real behavior against a real exception type rather than a mock that
    only proves the code path was hit.
    """

    def __init__(self, real: Any) -> None:
        self._real = real
        self.broken = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def pipeline(self, *args: Any, **kwargs: Any) -> Any:
        if self.broken:
            raise redis.exceptions.ConnectionError("connection refused")
        return self._real.pipeline(*args, **kwargs)

    async def set(self, *args: Any, **kwargs: Any) -> Any:
        if self.broken:
            raise redis.exceptions.ConnectionError("connection refused")
        return await self._real.set(*args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        if self.broken:
            raise redis.exceptions.ConnectionError("connection refused")
        return await self._real.delete(*args, **kwargs)


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'failure_scenarios.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))

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


def send_message(conversation_id: str, body: str) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(uuid.uuid4()),
        "body": body,
    }


def test_message_send_returns_graceful_error_when_database_fails(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PostgreSQL failure during message:send. app.websocket.router already
    wraps persist_message in a try/except (see _handle_message_send) - this
    test confirms that path actually works end-to-end: the client gets an
    explicit error frame instead of a crash or a silently dropped message,
    and the connection stays fully usable afterward for the next send.
    """
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            async def _boom(*args: Any, **kwargs: Any) -> Any:
                raise OperationalError(
                    "INSERT INTO messages ...", {}, Exception("connection refused")
                )

            monkeypatch.setattr("app.websocket.router.persist_message", _boom)

            owner_ws.send_json(send_message(conversation_id, "hello"))
            error = owner_ws.receive_json()
            assert error == {
                "type": "error",
                "detail": "failed to send message",
                "conversation_id": conversation_id,
            }

            # Database recovers (or was never really down elsewhere): the
            # same connection keeps working, proving this was a per-request
            # failure, not something that poisoned the connection.
            monkeypatch.undo()
            owner_ws.send_json(send_message(conversation_id, "hello again"))
            recovered = owner_ws.receive_json()
            assert recovered["type"] == "message:new"
            assert recovered["body"] == "hello again"
            owner_ws.receive_json()  # message:ack


def test_websocket_survives_a_full_redis_outage_via_fail_open_degradation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis failure mid-connection. Presence heartbeats, rate-limit checks,
    and typing-indicator state are all ephemeral, best-effort Redis state
    (project design rule: Redis is for ephemeral/distributed state only) -
    none of them are allowed to take the connection down or block a
    send/typing event when Redis is unreachable. Every one of these call
    sites (app.websocket.router._refresh_presence_heartbeat,
    _enforce_rate_limit, _handle_typing_start/_stop,
    _clear_presence_connection) fails open and logs a warning rather than
    raising. This test was written *after* discovering, empirically, that an
    earlier version of this code let a Redis outage raise straight out of
    the WebSocket handler - killing the connection and then failing a
    second time in its own disconnect cleanup - which is what motivated
    wrapping each of these call sites individually rather than assuming
    graceful degradation.
    """
    warnings: list[str] = []
    monkeypatch.setattr(
        "app.websocket.router.logger.warning",
        lambda message, *args, **kwargs: warnings.append(message % args if args else message),
    )

    real_redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    toggleable = _ToggleableRedis(real_redis)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'redis_outage.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
    )
    application = create_app(settings, redis_client=toggleable)  # type: ignore[arg-type]

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())

    with TestClient(application) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            assert owner_ws.receive_json() == {"type": "auth:authenticated"}
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()  # conversation:joined

            # Redis goes down from here on - the connection is already
            # established and joined, matching an outage that starts
            # mid-session rather than at connect time.
            toggleable.broken = True

            # A bare heartbeat: no reply expected either way: the point is
            # only that this doesn't crash the connection.
            owner_ws.send_json({"type": "presence:heartbeat"})

            # message:send depends on a rate-limit check (Redis) but not on
            # anything else Redis-backed: it must still fully succeed.
            owner_ws.send_json(send_message(conversation_id, "hello despite redis outage"))
            new_event = owner_ws.receive_json()
            assert new_event["type"] == "message:new"
            assert new_event["body"] == "hello despite redis outage"
            ack = owner_ws.receive_json()
            assert ack["type"] == "message:ack"
            assert ack["id"] == new_event["id"]
            assert ack["duplicate"] is False

            # typing:start/stop also depend on Redis (rate limit + the
            # typing flag itself) - best-effort, so no broadcast is
            # expected (there's no other socket in the room anyway), but
            # neither call may crash the connection.
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})

            # The connection is still fully alive: prove it with one more
            # ordinary send/ack round-trip.
            owner_ws.send_json(send_message(conversation_id, "still alive"))
            final_new = owner_ws.receive_json()
            assert final_new["type"] == "message:new"
            assert final_new["body"] == "still alive"
            final_ack = owner_ws.receive_json()
            assert final_ack["type"] == "message:ack"

        # Exiting the `with` block runs the disconnect path, including the
        # Redis-backed presence cleanup - this must not raise either.

    asyncio.run(application.state.db_engine.dispose())

    # Every degradation above must be visible, not silent.
    assert any("presence_heartbeat_failed" in message for message in warnings)
    assert any(
        "rate_limit_check_failed" in message and "scope=message_send" in message
        for message in warnings
    )
    assert any(
        "rate_limit_check_failed" in message and "scope=typing" in message
        for message in warnings
    )
    assert any("presence_disconnect_cleanup_failed" in message for message in warnings)


def test_http_rate_limit_fails_open_when_redis_is_unreachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis failure on the HTTP rate-limited path (login/register/contact
    sync all share app.core.rate_limit.build_rate_limiter). A Redis outage
    must not take login down: requests keep succeeding, well past what the
    configured limit would ever allow, proving the limit is genuinely not
    being enforced rather than merely still within budget - and every
    occurrence is logged.
    """
    warnings: list[str] = []
    monkeypatch.setattr(
        "app.core.rate_limit.logger.warning",
        lambda message, *args, **kwargs: warnings.append(message % args if args else message),
    )

    real_redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    toggleable = _ToggleableRedis(real_redis)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'http_rate_limit_outage.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        rate_limit_login_max_attempts=2,
        rate_limit_login_window_seconds=60,
    )
    application = create_app(settings, redis_client=toggleable)  # type: ignore[arg-type]

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())

    with TestClient(application) as client:
        client.post(
            "/auth/register",
            json={"email": "owner@example.com", "password": PASSWORD, "device_label": "Test"},
        )

        toggleable.broken = True

        responses = [
            client.post(
                "/auth/login",
                json={"email": "owner@example.com", "password": PASSWORD, "device_label": "Test"},
            )
            for _ in range(5)  # well past rate_limit_login_max_attempts=2
        ]

    asyncio.run(application.state.db_engine.dispose())

    assert all(response.status_code == 200 for response in responses)
    assert any(
        "rate_limit_check_failed" in message and "scope=login" in message
        for message in warnings
    )
