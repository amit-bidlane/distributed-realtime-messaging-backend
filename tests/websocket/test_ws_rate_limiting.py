import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'ws_rate_limiting.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        rate_limit_message_send_max_events=2,
        rate_limit_message_send_window_seconds=60,
        rate_limit_typing_max_events=2,
        rate_limit_typing_window_seconds=60,
        rate_limit_conversation_action_max_events=2,
        rate_limit_conversation_action_window_seconds=60,
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


def send_message(conversation_id: str) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(uuid.uuid4()),
        "body": "hi",
    }


def read_message(conversation_id: str, message_id: str) -> dict[str, str]:
    return {"type": "message:read", "conversation_id": conversation_id, "message_id": message_id}


def test_message_send_allows_requests_within_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id))
            first_new = owner_ws.receive_json()
            first_ack = owner_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id))
            second_new = owner_ws.receive_json()
            second_ack = owner_ws.receive_json()

    assert first_new["type"] == "message:new"
    assert first_ack["type"] == "message:ack"
    assert second_new["type"] == "message:new"
    assert second_ack["type"] == "message:ack"


def test_message_send_rejects_requests_over_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id))
            owner_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(send_message(conversation_id))
            owner_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:ack

            # Third send within the window exceeds the configured limit of 2.
            owner_ws.send_json(send_message(conversation_id))
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "rate limited",
            "conversation_id": conversation_id,
        }


def test_message_send_rate_limit_is_per_user(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with (
            client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            # Owner exhausts their own budget (2 sends).
            owner_ws.send_json(send_message(conversation_id))
            owner_ws.receive_json()  # message:new
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(send_message(conversation_id))
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id))
            owner_error = owner_ws.receive_json()
            assert owner_error["detail"] == "rate limited"

            # The member has their own, untouched budget.
            member_ws.send_json(send_message(conversation_id))
            member_new = member_ws.receive_json()

    assert member_new["type"] == "message:new"


def test_conversation_join_rejects_events_over_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()

            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()  # conversation:joined

            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()  # conversation:joined

            # Third join within the window exceeds the configured limit of 2.
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "rate limited",
            "conversation_id": conversation_id,
        }


def test_message_read_rejects_events_over_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with (
            client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id))
            new_event = owner_ws.receive_json()  # message:new
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack
            message_id = new_event["id"]

            # The member's conversation_action budget (max 2) already spent
            # one unit on the conversation:join above, so only one more
            # message:read is allowed before the third is rejected.
            member_ws.send_json(read_message(conversation_id, message_id))
            owner_ws.receive_json()  # message:read
            member_ws.receive_json()  # message:read

            member_ws.send_json(read_message(conversation_id, message_id))
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "rate limited",
            "conversation_id": conversation_id,
        }


def test_typing_rejects_events_over_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, owner_id = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with (
            client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            # First two typing events (start, stop) consume the configured
            # budget of 2, shared across typing:start and typing:stop.
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            member_ws.receive_json()  # typing:start
            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})
            member_ws.receive_json()  # typing:stop

            # Third typing event within the window is rejected before it is
            # even evaluated for broadcast.
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "rate limited",
            "conversation_id": conversation_id,
        }
