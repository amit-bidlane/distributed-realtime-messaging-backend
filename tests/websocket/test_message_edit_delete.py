import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.messages.models import Message

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'edit_delete.db'}",
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


@pytest.fixture
def two_instances(tmp_path: Path) -> Iterator[tuple[FastAPI, FastAPI]]:
    """Same construction as tests/websocket/test_multi_instance.py's fixture
    of the same name: two independent app instances sharing one fake Redis
    server and one SQLite file, standing in for fastapi-1/fastapi-2."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'edit_delete_multi_instance.db'}"
    shared_redis_server = fakeredis.FakeServer()

    def build() -> FastAPI:
        settings = Settings(
            database_url=db_url,
            jwt_secret=TEST_JWT_SECRET,
            refresh_token_pepper="test-refresh-pepper",
        )
        redis_client = fakeredis.FakeAsyncRedis(server=shared_redis_server, decode_responses=True)
        application = create_app(settings, redis_client=redis_client)
        engine = create_async_engine(db_url, poolclass=NullPool, hide_parameters=True)
        application.state.db_engine = engine
        application.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return application

    app1 = build()
    app2 = build()

    async def create_schema() -> None:
        async with app1.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())
    yield app1, app2
    asyncio.run(app1.state.db_engine.dispose())
    asyncio.run(app2.state.db_engine.dispose())


def _await_fanout_ready(client: TestClient, app: FastAPI) -> None:
    client.portal.call(app.state.fanout.ready.wait)


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


def send_message(
    conversation_id: str, body: str, client_message_id: uuid.UUID | None = None
) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(client_message_id or uuid.uuid4()),
        "body": body,
    }


def edit_message(conversation_id: str, message_id: str, body: str) -> dict[str, str]:
    return {
        "type": "message:edit",
        "conversation_id": conversation_id,
        "message_id": message_id,
        "body": body,
    }


def delete_message(conversation_id: str, message_id: str) -> dict[str, str]:
    return {
        "type": "message:delete",
        "conversation_id": conversation_id,
        "message_id": message_id,
    }


async def _get_message(
    session_factory: async_sessionmaker[AsyncSession], message_id: uuid.UUID
) -> Message | None:
    async with session_factory() as session:
        return await session.scalar(select(Message).where(Message.id == message_id))


def test_sender_can_edit_own_message_and_broadcast_reaches_other_members(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(edit_message(conversation_id, new_event["id"], "hello, edited"))
            edited_owner = owner_ws.receive_json()
            edited_member = member_ws.receive_json()

        assert edited_owner == edited_member
        assert edited_owner["type"] == "message:edited"
        assert edited_owner["id"] == new_event["id"]
        assert edited_owner["body"] == "hello, edited"
        assert edited_owner["edited_at"] is not None

        message = client.portal.call(
            _get_message, app.state.session_factory, uuid.UUID(new_event["id"])
        )
        assert message is not None
        assert message.body == "hello, edited"
        assert message.edited_at is not None


def test_edit_broadcasts_cross_instance(two_instances: tuple[FastAPI, FastAPI]) -> None:
    app1, app2 = two_instances
    with TestClient(app1) as client1, TestClient(app2) as client2:
        _await_fanout_ready(client1, app1)
        _await_fanout_ready(client2, app2)

        owner_tokens, _ = register_user(client1, "owner@example.com")
        member_tokens, member_id = register_user(client2, "member@example.com")
        conversation_id = create_direct_conversation(client1, owner_tokens, member_id)

        with (
            client1.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client2.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()  # message:new (instance 1, local)
            member_ws.receive_json()  # message:new (relayed via Redis)
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(
                edit_message(conversation_id, new_event["id"], "edited cross-instance")
            )
            owner_edited = owner_ws.receive_json()
            # Only reachable via Redis Pub/Sub relay - member's socket only
            # ever connected to instance 2, which never handled this edit.
            member_edited = member_ws.receive_json()

        assert owner_edited["type"] == "message:edited"
        assert member_edited == owner_edited
        assert member_edited["body"] == "edited cross-instance"


def test_sender_can_delete_own_message_and_broadcast_reaches_other_members(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "secret plans"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(delete_message(conversation_id, new_event["id"]))
            deleted_owner = owner_ws.receive_json()
            deleted_member = member_ws.receive_json()

        assert deleted_owner == deleted_member
        assert deleted_owner == {
            "type": "message:deleted",
            "conversation_id": conversation_id,
            "message_id": new_event["id"],
            "deleted_at": deleted_owner["deleted_at"],
        }

        message = client.portal.call(
            _get_message, app.state.session_factory, uuid.UUID(new_event["id"])
        )
        assert message is not None
        assert message.deleted_at is not None
        assert message.body == ""  # plaintext cleared server-side
        assert message.sequence_number == new_event["sequence_number"]  # row/ordering kept


def test_non_sender_cannot_edit_another_members_message(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            member_ws.send_json(edit_message(conversation_id, new_event["id"], "tampered"))
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "only the sender may edit this message",
            "conversation_id": conversation_id,
        }

        message = client.portal.call(
            _get_message, app.state.session_factory, uuid.UUID(new_event["id"])
        )
        assert message is not None
        assert message.body == "hello"  # untouched


def test_non_sender_cannot_delete_another_members_message(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            member_ws.send_json(delete_message(conversation_id, new_event["id"]))
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "only the sender may delete this message",
            "conversation_id": conversation_id,
        }

        message = client.portal.call(
            _get_message, app.state.session_factory, uuid.UUID(new_event["id"])
        )
        assert message is not None
        assert message.deleted_at is None  # untouched


def test_deleted_message_replayed_never_shows_original_plaintext(app: FastAPI) -> None:
    """A message deleted mid-conversation, replayed later to a reconnecting
    client, must show the deleted_at marker - never the original body."""
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "one"))
            first = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack (member offline)
            owner_ws.send_json(send_message(conversation_id, "sensitive - delete me"))
            second = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(delete_message(conversation_id, second["id"]))
            deleted = owner_ws.receive_json()
            assert deleted["type"] == "message:deleted"

        # Member was never connected: reconnecting now replays both, the
        # second one reflecting its current (deleted) state.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            replay = member_ws.receive_json()
            for _ in replay["messages"]:
                member_ws.receive_json()  # message:delivered, one per replayed message

        assert replay["type"] == "message:replay"
        messages = replay["messages"]
        assert [m["id"] for m in messages] == [first["id"], second["id"]]

        first_item, second_item = messages
        assert first_item["body"] == "one"
        assert "deleted_at" not in first_item

        assert "body" not in second_item
        assert second_item["deleted_at"] == deleted["deleted_at"]
        assert second_item["sequence_number"] == second["sequence_number"]


def test_replay_shows_current_edited_body_not_original(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "original body"))
            new_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(edit_message(conversation_id, new_event["id"], "corrected body"))
            edited = owner_ws.receive_json()

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
            replay = member_ws.receive_json()
            member_ws.receive_json()  # message:delivered

        [item] = replay["messages"]
        assert item["body"] == "corrected body"
        assert item["edited_at"] == edited["edited_at"]


def test_edit_and_delete_reject_unknown_or_cross_conversation_message_id_cleanly(
    app: FastAPI,
) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_a = create_direct_conversation(client, owner_tokens, member_id)
        conversation_b = create_group_conversation(
            client, owner_tokens, [member_id], "Other room"
        )

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            for conversation_id in (conversation_a, conversation_b):
                owner_ws.send_json(
                    {"type": "conversation:join", "conversation_id": conversation_id}
                )
                owner_ws.receive_json()

            owner_ws.send_json(send_message(conversation_a, "hello"))
            new_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            # Real message, but from conversation_a, quoted against conversation_b.
            owner_ws.send_json(edit_message(conversation_b, new_event["id"], "nope"))
            cross_conversation_edit_error = owner_ws.receive_json()

            # A message_id that was never persisted at all.
            owner_ws.send_json(edit_message(conversation_a, str(uuid.uuid4()), "nope"))
            unknown_edit_error = owner_ws.receive_json()

            owner_ws.send_json(delete_message(conversation_b, new_event["id"]))
            cross_conversation_delete_error = owner_ws.receive_json()

            owner_ws.send_json(delete_message(conversation_a, str(uuid.uuid4())))
            unknown_delete_error = owner_ws.receive_json()

        for error, conversation_id in (
            (cross_conversation_edit_error, conversation_b),
            (unknown_edit_error, conversation_a),
            (cross_conversation_delete_error, conversation_b),
            (unknown_delete_error, conversation_a),
        ):
            assert error == {
                "type": "error",
                "detail": "message not found",
                "conversation_id": conversation_id,
            }

        message = client.portal.call(
            _get_message, app.state.session_factory, uuid.UUID(new_event["id"])
        )
        assert message is not None
        assert message.body == "hello"  # untouched by any of the rejected attempts
        assert message.deleted_at is None


def test_repeated_edit_with_same_body_is_idempotent_and_not_rebroadcast(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            owner_ws.send_json(edit_message(conversation_id, new_event["id"], "edited"))
            owner_ws.receive_json()
            member_ws.receive_json()

            # Second edit with the identical body: no error, no second
            # broadcast anywhere. Proved the same way test_receipts.py proves
            # it for message:read - the next frame each side sees is a
            # distinct, unrelated message, not a stray repeated
            # message:edited.
            owner_ws.send_json(edit_message(conversation_id, new_event["id"], "edited"))
            owner_ws.send_json(send_message(conversation_id, "second"))
            next_owner = owner_ws.receive_json()  # message:new
            next_member = member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

        assert next_owner["type"] == "message:new"
        assert next_owner["body"] == "second"
        assert next_member["type"] == "message:new"
        assert next_member["body"] == "second"


def test_repeated_delete_is_idempotent_and_not_rebroadcast(app: FastAPI) -> None:
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

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            owner_ws.send_json(delete_message(conversation_id, new_event["id"]))
            first_deleted_owner = owner_ws.receive_json()
            member_ws.receive_json()

            owner_ws.send_json(delete_message(conversation_id, new_event["id"]))
            owner_ws.send_json(send_message(conversation_id, "second"))
            next_owner = owner_ws.receive_json()  # message:new
            next_member = member_ws.receive_json()  # message:new
            owner_ws.receive_json()  # message:delivered
            member_ws.receive_json()  # message:delivered
            owner_ws.receive_json()  # message:ack

        assert first_deleted_owner["type"] == "message:deleted"
        assert next_owner["type"] == "message:new"
        assert next_owner["body"] == "second"
        assert next_member["type"] == "message:new"
        assert next_member["body"] == "second"


def test_editing_a_deleted_message_is_rejected(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(delete_message(conversation_id, new_event["id"]))
            owner_ws.receive_json()  # message:deleted

            owner_ws.send_json(edit_message(conversation_id, new_event["id"], "resurrected"))
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "cannot edit a deleted message",
            "conversation_id": conversation_id,
        }


def test_edit_and_delete_require_prior_join(app: FastAPI) -> None:
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
            owner_ws.receive_json()

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as fresh_ws:
            fresh_ws.receive_json()
            # No conversation:join this time.
            fresh_ws.send_json(edit_message(conversation_id, new_event["id"], "nope"))
            edit_error = fresh_ws.receive_json()
            fresh_ws.send_json(delete_message(conversation_id, new_event["id"]))
            delete_error = fresh_ws.receive_json()

        assert edit_error == {
            "type": "error",
            "detail": "join conversation before editing",
            "conversation_id": conversation_id,
        }
        assert delete_error == {
            "type": "error",
            "detail": "join conversation before deleting",
            "conversation_id": conversation_id,
        }
