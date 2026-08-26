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
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'typing.db'}",
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


def create_group_conversation(
    client: TestClient, owner_tokens: dict[str, str], member_ids: list[uuid.UUID], title: str
) -> str:
    response = client.post(
        "/conversations",
        headers=auth_header(owner_tokens),
        json={"kind": "group", "title": title, "member_ids": [str(m) for m in member_ids]},
    )
    assert response.status_code == 201
    return str(response.json()["id"])


def test_typing_start_broadcasts_to_others_but_not_self(app: FastAPI) -> None:
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

            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            typing_event = member_ws.receive_json()

            # Prove the owner never got their own typing:start echoed back:
            # the very next frame the owner sees is this distinct message.
            owner_ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": str(uuid.uuid4()),
                    "body": "hi",
                }
            )
            owner_next = owner_ws.receive_json()  # message:new
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

    assert typing_event == {
        "type": "typing:start",
        "conversation_id": conversation_id,
        "user_id": str(owner_id),
    }
    assert owner_next["type"] == "message:new"


def test_typing_stop_broadcasts_to_others(app: FastAPI) -> None:
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

            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            member_ws.receive_json()

            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})
            stop_event = member_ws.receive_json()

    assert stop_event == {
        "type": "typing:stop",
        "conversation_id": conversation_id,
        "user_id": str(owner_id),
    }


def test_typing_stop_without_active_typing_is_not_broadcast(app: FastAPI) -> None:
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

            # Stop without ever starting is a no-op: nothing broadcast for it.
            # Prove it by immediately starting for real and checking that the
            # member's very next frame is that start, not a stray stop.
            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            member_next = member_ws.receive_json()

    assert member_next == {
        "type": "typing:start",
        "conversation_id": conversation_id,
        "user_id": str(owner_id),
    }


def test_typing_requires_prior_join(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "join conversation before typing",
            "conversation_id": conversation_id,
        }


def test_typing_rejected_after_membership_revoked(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_group_conversation(
            client, owner_tokens, [member_id], "Team room"
        )

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            removal = client.delete(
                f"/conversations/{conversation_id}/members/{member_id}",
                headers=auth_header(owner_tokens),
            )
            assert removal.status_code == 204

            member_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "conversation not found",
            "conversation_id": conversation_id,
        }


def test_typing_start_is_rate_limited(app: FastAPI) -> None:
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

            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            first_start = member_ws.receive_json()
            assert first_start["type"] == "typing:start"

            # Rapid repeat: should be coalesced, not rebroadcast. Prove it by
            # sending a distinct follow-up and checking it's the very next
            # frame the member sees.
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})
            member_next = member_ws.receive_json()

    assert member_next["type"] == "typing:stop"
    assert member_next["user_id"] == str(owner_id)


def test_typing_stop_clears_rate_limit_for_immediate_restart(app: FastAPI) -> None:
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

            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            member_ws.receive_json()
            owner_ws.send_json({"type": "typing:stop", "conversation_id": conversation_id})
            member_ws.receive_json()

            # Immediately after an explicit stop, a new start is real signal,
            # not spam, and must be broadcast even though little time has
            # passed since the original start's rate-limit window began.
            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})
            second_start = member_ws.receive_json()

    assert second_start == {
        "type": "typing:start",
        "conversation_id": conversation_id,
        "user_id": str(owner_id),
    }
