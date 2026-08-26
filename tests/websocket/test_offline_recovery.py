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


def _build_app(database_url: str, *, message_replay_batch_limit: int = 200) -> FastAPI:
    settings = Settings(
        database_url=database_url,
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        message_replay_batch_limit=message_replay_batch_limit,
    )
    return create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))


def _create_schema(application: FastAPI) -> None:
    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    application = _build_app(f"sqlite+aiosqlite:///{tmp_path / 'offline_recovery.db'}")
    _create_schema(application)
    yield application
    asyncio.run(application.state.db_engine.dispose())


@pytest.fixture
def small_batch_app(tmp_path: Path) -> Iterator[FastAPI]:
    application = _build_app(
        f"sqlite+aiosqlite:///{tmp_path / 'small_batch.db'}", message_replay_batch_limit=2
    )
    _create_schema(application)
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


def send_message(
    conversation_id: str, body: str, client_message_id: uuid.UUID | None = None
) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(client_message_id or uuid.uuid4()),
        "body": body,
    }


def test_reconnect_replays_missing_messages_after_disconnect(app: FastAPI) -> None:
    """B disconnects, A sends messages, B reconnects, B receives the missing ones."""
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
        # B is now disconnected.

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            sent_ids = []
            for body in ("one", "two", "three"):
                owner_ws.send_json(send_message(conversation_id, body))
                new_event = owner_ws.receive_json()  # message:new (B offline: A only)
                sent_ids.append(new_event["id"])
                owner_ws.receive_json()  # message:ack (no message:delivered: B is offline)

        # B reconnects and rejoins.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()  # auth:authenticated
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            replay = member_ws.receive_json()
            for _ in replay["messages"]:
                member_ws.receive_json()  # message:delivered, one per replayed message

        assert replay["type"] == "message:replay"
        assert replay["conversation_id"] == conversation_id
        assert replay["has_more"] is False
        assert [m["id"] for m in replay["messages"]] == sent_ids
        assert [m["body"] for m in replay["messages"]] == ["one", "two", "three"]
        assert [m["sequence_number"] for m in replay["messages"]] == [1, 2, 3]


def test_reconnect_replay_marks_messages_delivered(app: FastAPI) -> None:
    """Replaying a message on reconnect also marks it DELIVERED (reuses the
    delivery-receipt marking)."""
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            replay = member_ws.receive_json()
            assert replay["type"] == "message:replay"
            delivered = member_ws.receive_json()

        assert delivered == {
            "type": "message:delivered",
            "conversation_id": conversation_id,
            "message_id": new_event["id"],
            "user_id": str(member_id),
            "delivered_at": delivered["delivered_at"],
        }


def test_sender_does_not_replay_own_previously_sent_messages(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "hello"))
            owner_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:ack

        # Owner reconnects and rejoins: their own cursor already advanced at
        # send time (app.websocket.router._handle_message_send), so no
        # replay of "hello" - the next frame for a fresh send is message:new.
        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()  # auth:authenticated
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            assert owner_ws.receive_json() == {
                "type": "conversation:joined",
                "conversation_id": conversation_id,
            }
            owner_ws.send_json(send_message(conversation_id, "second"))
            next_frame = owner_ws.receive_json()
            assert next_frame["type"] == "message:new"
            assert next_frame["body"] == "second"


def test_second_reconnect_only_replays_newly_missed_messages(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            for body in ("one", "two"):
                owner_ws.send_json(send_message(conversation_id, body))
                owner_ws.receive_json()  # message:new
                owner_ws.receive_json()  # message:ack

        # First reconnect: catches up on "one" and "two". Fully drain the
        # resulting message:delivered frames (and thus let the server-side
        # handler run to completion, committing the cursor advance) before
        # the connection closes - TestClient cancels the in-flight handler
        # task on __exit__ if frames are left unread, which would abort the
        # cursor update along with it.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            first_replay = member_ws.receive_json()
            assert [m["body"] for m in first_replay["messages"]] == ["one", "two"]
            member_ws.receive_json()  # message:delivered (one)
            member_ws.receive_json()  # message:delivered (two)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "three"))
            owner_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:ack

        # Second reconnect: only "three" is missing now.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            second_replay = member_ws.receive_json()
            member_ws.receive_json()  # message:delivered (three)

        assert second_replay["type"] == "message:replay"
        assert [m["body"] for m in second_replay["messages"]] == ["three"]


def test_replay_batch_respects_limit_and_reports_has_more(small_batch_app: FastAPI) -> None:
    with TestClient(small_batch_app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            for body in ("one", "two", "three"):
                owner_ws.send_json(send_message(conversation_id, body))
                owner_ws.receive_json()  # message:new
                owner_ws.receive_json()  # message:ack

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            first_batch = member_ws.receive_json()
            assert first_batch["type"] == "message:replay"
            assert [m["body"] for m in first_batch["messages"]] == ["one", "two"]
            assert first_batch["has_more"] is True
            member_ws.receive_json()  # message:delivered (one)
            member_ws.receive_json()  # message:delivered (two)

            # Rejoin to continue: the cursor already advanced past the
            # first batch, so this pulls the remainder.
            member_ws.send_json(
                {"type": "conversation:leave", "conversation_id": conversation_id}
            )
            member_ws.receive_json()  # conversation:left
            member_ws.send_json(
                {"type": "conversation:join", "conversation_id": conversation_id}
            )
            member_ws.receive_json()  # conversation:joined
            second_batch = member_ws.receive_json()
            assert second_batch["type"] == "message:replay"
            assert [m["body"] for m in second_batch["messages"]] == ["three"]
            assert second_batch["has_more"] is False
            member_ws.receive_json()  # message:delivered (three)


def test_server_restart_preserves_offline_recovery_state(tmp_path: Path) -> None:
    """Missing-message recovery must survive a process restart: the cursor
    and the message history both live in PostgreSQL (sqlite here, standing
    in for it), not in the in-memory ConnectionManager/Redis state that a
    restart wipes."""
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'restart.db'}"

    app1 = _build_app(database_url)
    _create_schema(app1)
    with TestClient(app1) as client1:
        owner_tokens, _ = register_user(client1, "owner@example.com")
        member_tokens, member_id = register_user(client1, "member@example.com")
        conversation_id = create_direct_conversation(client1, owner_tokens, member_id)

        with client1.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
        # B goes offline before the server "restarts".

        with client1.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            for body in ("one", "two", "three"):
                owner_ws.send_json(send_message(conversation_id, body))
                owner_ws.receive_json()  # message:new
                owner_ws.receive_json()  # message:ack
    # Exiting the TestClient context runs the app's lifespan shutdown,
    # disposing its DB engine - simulating a server restart.

    app2 = _build_app(database_url)  # same file, schema already created by app1
    with TestClient(app2) as client2:
        with client2.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()  # auth:authenticated (session row survived the restart)
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            replay = member_ws.receive_json()
            for _ in replay["messages"]:
                member_ws.receive_json()  # message:delivered, one per replayed message

    assert replay["type"] == "message:replay"
    assert [m["body"] for m in replay["messages"]] == ["one", "two", "three"]
    assert [m["sequence_number"] for m in replay["messages"]] == [1, 2, 3]
