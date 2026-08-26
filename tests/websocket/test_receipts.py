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

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.messages.models import MessageReceipt

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}",
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


def send_message(
    conversation_id: str, body: str, client_message_id: uuid.UUID | None = None
) -> dict[str, str]:
    return {
        "type": "message:send",
        "conversation_id": conversation_id,
        "client_message_id": str(client_message_id or uuid.uuid4()),
        "body": body,
    }


def read_message(conversation_id: str, message_id: str) -> dict[str, str]:
    return {"type": "message:read", "conversation_id": conversation_id, "message_id": message_id}


async def _get_receipt(
    session_factory: async_sessionmaker[AsyncSession], message_id: uuid.UUID, user_id: uuid.UUID
) -> MessageReceipt | None:
    async with session_factory() as session:
        return await session.scalar(
            select(MessageReceipt).where(
                MessageReceipt.message_id == message_id, MessageReceipt.user_id == user_id
            )
        )


def test_message_delivered_automatically_to_joined_recipient(app: FastAPI) -> None:
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

            delivered_owner = owner_ws.receive_json()
            delivered_member = member_ws.receive_json()
            owner_ws.receive_json()  # message:ack

        assert delivered_owner == delivered_member
        assert delivered_owner["type"] == "message:delivered"
        assert delivered_owner["message_id"] == new_event["id"]
        assert delivered_owner["user_id"] == str(member_id)

        receipt = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member_id
        )
        assert receipt is not None
        assert receipt.delivered_at is not None
        assert receipt.read_at is None


def test_message_not_delivered_when_recipient_not_joined(app: FastAPI) -> None:
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
            # member never joins.

            owner_ws.send_json(send_message(conversation_id, "hello"))
            new_event = owner_ws.receive_json()
            assert new_event["type"] == "message:new"
            ack = owner_ws.receive_json()
            assert ack["type"] == "message:ack"  # no message:delivered frame in between

        receipt = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member_id
        )
        assert receipt is None  # SENT (persisted) but never DELIVERED


def test_message_read_after_delivered_preserves_delivered_at(app: FastAPI) -> None:
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

            member_ws.send_json(read_message(conversation_id, new_event["id"]))
            read_owner = owner_ws.receive_json()
            read_member = member_ws.receive_json()

        assert read_owner == read_member
        assert read_owner["type"] == "message:read"
        assert read_owner["user_id"] == str(member_id)

        receipt = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member_id
        )
        assert receipt is not None
        assert receipt.delivered_at is not None
        assert receipt.read_at is not None
        assert receipt.delivered_at != receipt.read_at


def test_reconnect_replay_delivers_message_before_read(app: FastAPI) -> None:
    """A message sent while the recipient was fully offline is marked
    DELIVERED automatically by offline-recovery replay on reconnect (see
    app.websocket.router._replay_missing_messages), before the recipient
    gets a chance to explicitly mark it read - superseding the old
    "read without prior delivery" path now that delivery is marked automatically on replay."""
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
            owner_ws.receive_json()  # message:ack (no delivered: member was never connected)

        # Member connects later and joins: replay delivers "hello" before
        # the member ever sends message:read.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()  # conversation:joined
            replay = member_ws.receive_json()
            assert replay["type"] == "message:replay"
            assert [m["id"] for m in replay["messages"]] == [new_event["id"]]
            delivered_event = member_ws.receive_json()
            assert delivered_event["type"] == "message:delivered"

            member_ws.send_json(read_message(conversation_id, new_event["id"]))
            read_event = member_ws.receive_json()
            assert read_event["type"] == "message:read"

        receipt = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member_id
        )
        assert receipt is not None
        assert receipt.delivered_at is not None
        assert receipt.read_at is not None
        assert receipt.delivered_at != receipt.read_at


def test_repeated_read_is_idempotent_and_not_rebroadcast(app: FastAPI) -> None:
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

            member_ws.send_json(read_message(conversation_id, new_event["id"]))
            owner_ws.receive_json()
            member_ws.receive_json()

            # Second read of the same message: no second broadcast anywhere.
            # Prove it by sending a second, distinct message right after and
            # confirming the very next frame each side sees is that message,
            # not a stray repeated message:read.
            member_ws.send_json(read_message(conversation_id, new_event["id"]))
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


def test_sender_cannot_mark_own_message_read(app: FastAPI) -> None:
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

            owner_ws.send_json(read_message(conversation_id, new_event["id"]))
            error = owner_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "cannot mark own message as read",
            "conversation_id": conversation_id,
        }


def test_message_read_requires_prior_join(app: FastAPI) -> None:
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

        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            # member never joins this time.
            member_ws.send_json(read_message(conversation_id, new_event["id"]))
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "join conversation before marking read",
            "conversation_id": conversation_id,
        }


def test_message_read_rejected_after_membership_revoked(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_group_conversation(
            client, owner_tokens, [member_id], "Team room"
        )

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

            # The member is removed from the group mid-session; their
            # WebSocket connection stays open and still thinks it's joined.
            removal = client.delete(
                f"/conversations/{conversation_id}/members/{member_id}",
                headers=auth_header(owner_tokens),
            )
            assert removal.status_code == 204

            member_ws.send_json(read_message(conversation_id, new_event["id"]))
            error = member_ws.receive_json()

        assert error == {
            "type": "error",
            "detail": "conversation not found",
            "conversation_id": conversation_id,
        }


def test_message_read_rejects_unknown_or_cross_conversation_message_id(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_a = create_direct_conversation(client, owner_tokens, member_id)
        conversation_b = create_group_conversation(
            client, owner_tokens, [member_id], "Other room"
        )

        with (
            client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            owner_ws.receive_json()
            member_ws.receive_json()
            for conversation_id in (conversation_a, conversation_b):
                owner_ws.send_json(
                    {"type": "conversation:join", "conversation_id": conversation_id}
                )
                owner_ws.receive_json()
                member_ws.send_json(
                    {"type": "conversation:join", "conversation_id": conversation_id}
                )
                member_ws.receive_json()

            owner_ws.send_json(send_message(conversation_a, "hello"))
            new_event = owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()
            member_ws.receive_json()
            owner_ws.receive_json()

            # Real message, but from conversation_a, quoted against conversation_b.
            member_ws.send_json(read_message(conversation_b, new_event["id"]))
            cross_conversation_error = member_ws.receive_json()

            # A message_id that was never persisted at all.
            member_ws.send_json(read_message(conversation_a, str(uuid.uuid4())))
            unknown_error = member_ws.receive_json()

        assert cross_conversation_error == {
            "type": "error",
            "detail": "message not found",
            "conversation_id": conversation_b,
        }
        assert unknown_error == {
            "type": "error",
            "detail": "message not found",
            "conversation_id": conversation_a,
        }


def test_group_conversation_receipts_are_independent_per_recipient(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, owner_id = register_user(client, "owner@example.com")
        member1_tokens, member1_id = register_user(client, "member1@example.com")
        member2_tokens, member2_id = register_user(client, "member2@example.com")
        conversation_id = create_group_conversation(
            client, owner_tokens, [member1_id, member2_id], "Team room"
        )

        with (
            client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client.websocket_connect("/ws", headers=auth_header(member1_tokens)) as member1_ws,
            client.websocket_connect("/ws", headers=auth_header(member2_tokens)) as member2_ws,
        ):
            for ws in (owner_ws, member1_ws, member2_ws):
                ws.receive_json()
                ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
                ws.receive_json()

            owner_ws.send_json(send_message(conversation_id, "hello team"))
            new_event = owner_ws.receive_json()
            member1_ws.receive_json()
            member2_ws.receive_json()

            delivered_ids = set()
            for _ in range(2):
                delivered_ids.add(owner_ws.receive_json()["user_id"])
                member1_ws.receive_json()
                member2_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            assert delivered_ids == {str(member1_id), str(member2_id)}

            member1_ws.send_json(read_message(conversation_id, new_event["id"]))
            owner_ws.receive_json()
            member1_ws.receive_json()
            member2_ws.receive_json()

        receipt1 = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member1_id
        )
        receipt2 = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), member2_id
        )
        sender_receipt = client.portal.call(
            _get_receipt, app.state.session_factory, uuid.UUID(new_event["id"]), owner_id
        )

        assert receipt1 is not None and receipt1.read_at is not None
        assert receipt2 is not None and receipt2.delivered_at is not None
        assert receipt2.read_at is None
        assert sender_receipt is None  # no self-receipt for the sender
