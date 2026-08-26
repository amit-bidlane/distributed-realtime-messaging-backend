import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.websockets import WebSocketDisconnect

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.messages.models import Message

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'websocket.db'}",
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


def send_message(
    conversation_id: str, body: str, client_message_id: uuid.UUID | None = None
) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(client_message_id or uuid.uuid4()),
        "body": body,
    }


async def _count_messages(
    session_factory: async_sessionmaker[AsyncSession], conversation_id: uuid.UUID
) -> int:
    async with session_factory() as session:
        rows = await session.scalars(
            select(Message).where(Message.conversation_id == conversation_id)
        )
        return len(rows.all())


def test_websocket_rejects_missing_authorization(app: FastAPI) -> None:
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws"):
            pass


def test_websocket_rejects_invalid_token(app: FastAPI) -> None:
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws", headers={"Authorization": "Bearer garbage"}):
            pass


def test_websocket_authenticates_and_join_requires_membership(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, owner_id = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        outsider_tokens, _ = register_user(client, "outsider@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            assert owner_ws.receive_json() == {"type": "auth:authenticated"}

            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            assert owner_ws.receive_json() == {
                "type": "conversation:joined",
                "conversation_id": conversation_id,
            }

        with client.websocket_connect("/ws", headers=auth_header(outsider_tokens)) as outsider_ws:
            outsider_ws.receive_json()
            outsider_ws.send_json(
                {"type": "conversation:join", "conversation_id": conversation_id}
            )
            error = outsider_ws.receive_json()
            assert error["type"] == "error"
            assert error["conversation_id"] == conversation_id

        assert owner_id and member_tokens


def test_message_send_broadcasts_to_joined_members_only(app: FastAPI) -> None:
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

            # Member has not joined yet: message:send from the owner should
            # broadcast only to sockets currently in the room (just the owner).
            owner_ws.send_json(send_message(conversation_id, "are you there?"))
            assert owner_ws.receive_json()["body"] == "are you there?"
            owner_ws.receive_json()  # message:ack

            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            assert member_ws.receive_json() == {
                "type": "conversation:joined",
                "conversation_id": conversation_id,
            }
            # The member just joined for the first time, and "are you
            # there?" (sequence 1) predates that join: offline-recovery
            # replay delivers it now, which also marks it DELIVERED to both
            # sockets currently in the room.
            replay = member_ws.receive_json()
            assert replay["type"] == "message:replay"
            assert [m["body"] for m in replay["messages"]] == ["are you there?"]
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered

            owner_ws.send_json(send_message(conversation_id, "hello"))
            owner_received = owner_ws.receive_json()  # message:new
            member_received = member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

        assert owner_received["type"] == "message:new"
        assert owner_received["body"] == "hello"
        assert owner_received["conversation_id"] == conversation_id
        assert owner_received["sequence_number"] == 2
        assert member_received == owner_received


def test_message_send_persists_survives_disconnect_and_acks(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            client_message_id = uuid.uuid4()
            owner_ws.send_json(send_message(conversation_id, "hello", client_message_id))
            new_event = owner_ws.receive_json()
            ack = owner_ws.receive_json()

        assert new_event["type"] == "message:new"
        assert new_event["sequence_number"] == 1
        assert new_event["body"] == "hello"
        assert ack == {
            "type": "message:ack",
            "client_message_id": str(client_message_id),
            "id": new_event["id"],
            "sequence_number": 1,
            "duplicate": False,
        }

        # The WebSocket above is fully closed now; the row must still be there.
        count = client.portal.call(
            _count_messages, app.state.session_factory, uuid.UUID(conversation_id)
        )
        assert count == 1


def test_message_send_duplicate_client_message_id_is_not_rebroadcast(app: FastAPI) -> None:
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

            client_message_id = uuid.uuid4()
            owner_ws.send_json(send_message(conversation_id, "hello", client_message_id))
            first_new = owner_ws.receive_json()  # message:new
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            first_ack = owner_ws.receive_json()  # message:ack
            assert first_ack["duplicate"] is False
            assert first_new["sequence_number"] == 1

            # Simulate a client retry after a lost ACK: same client_message_id.
            owner_ws.send_json(send_message(conversation_id, "hello", client_message_id))
            retry_ack = owner_ws.receive_json()
            assert retry_ack == {
                "type": "message:ack",
                "client_message_id": str(client_message_id),
                "id": first_new["id"],
                "sequence_number": 1,
                "duplicate": True,
            }

            # No second message:new was broadcast for the retry: the member's
            # next frame is the following distinct message, at sequence 2.
            owner_ws.send_json(send_message(conversation_id, "second", uuid.uuid4()))
            member_next = member_ws.receive_json()  # message:new
            assert member_next["body"] == "second"
            assert member_next["sequence_number"] == 2
            owner_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

        count = client.portal.call(
            _count_messages, app.state.session_factory, uuid.UUID(conversation_id)
        )
        assert count == 2


def test_message_send_requires_join_first(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "hi"))
            error = owner_ws.receive_json()
            assert error == {
                "type": "error",
                "detail": "join conversation before sending",
                "conversation_id": conversation_id,
            }


def test_leave_then_message_send_is_rejected(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            owner_ws.send_json({"type": "conversation:leave", "conversation_id": conversation_id})
            assert owner_ws.receive_json() == {
                "type": "conversation:left",
                "conversation_id": conversation_id,
            }

            owner_ws.send_json({"type": "conversation:leave", "conversation_id": conversation_id})
            leave_again = owner_ws.receive_json()
            assert leave_again == {
                "type": "error",
                "detail": "not joined",
                "conversation_id": conversation_id,
            }

            owner_ws.send_json(send_message(conversation_id, "hi"))
            send_after_leave = owner_ws.receive_json()
            assert send_after_leave["detail"] == "join conversation before sending"


def test_invalid_event_returns_error_without_closing(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()

            owner_ws.send_json({"type": "not:a:real:event"})
            assert owner_ws.receive_json() == {"type": "error", "detail": "invalid event"}

            owner_ws.send_json({"type": "conversation:join"})
            assert owner_ws.receive_json() == {"type": "error", "detail": "invalid event"}

            owner_ws.send_text("not json")
            assert owner_ws.receive_json() == {"type": "error", "detail": "invalid json"}


def test_disconnect_cleans_up_connection_manager(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

        manager = app.state.connection_manager
        assert manager._connections == {}
        assert manager._rooms == {}

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
            member_ws.send_json(send_message(conversation_id, "hi"))
            assert member_ws.receive_json()["type"] == "message:new"


def test_graceful_shutdown_notifies_and_closes_clients(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()

            client.portal.call(app.state.connection_manager.begin_shutdown)

            assert owner_ws.receive_json() == {"type": "server:shutting_down"}
            with pytest.raises(WebSocketDisconnect):
                owner_ws.receive_json()

        assert app.state.connection_manager.accepting_connections is False


def test_new_connections_rejected_while_shutting_down(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        client.portal.call(app.state.connection_manager.begin_shutdown)

        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws", headers=auth_header(owner_tokens)):
                pass
