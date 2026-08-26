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
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'message_history.db'}",
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


def send_via_websocket(
    client: TestClient, tokens: dict[str, str], conversation_id: str, bodies: list[str]
) -> list[dict[str, object]]:
    """Send messages over the WebSocket API (the only write path for
    messages) and return each resulting message:new event.
    """
    sent: list[dict[str, object]] = []
    with client.websocket_connect("/ws", headers=auth_header(tokens)) as ws:
        ws.receive_json()  # auth:authenticated
        ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
        ws.receive_json()  # conversation:joined
        for body in bodies:
            ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": str(uuid.uuid4()),
                    "body": body,
                }
            )
            sent.append(ws.receive_json())  # message:new
            ws.receive_json()  # message:ack
    return sent


def test_non_member_gets_404_not_the_message_list(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        outsider_tokens, _ = register_user(client, "outsider@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)
        send_via_websocket(client, owner_tokens, conversation_id, ["hello"])

        outsider_response = client.get(
            f"/conversations/{conversation_id}/messages",
            headers=auth_header(outsider_tokens),
        )
        member_response = client.get(
            f"/conversations/{conversation_id}/messages",
            headers=auth_header(owner_tokens),
        )

    assert outsider_response.status_code == 404
    assert member_response.status_code == 200
    assert len(member_response.json()["items"]) == 1


def test_pagination_correctness_across_cursor_boundaries(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)
        bodies = [f"message {i}" for i in range(1, 6)]
        send_via_websocket(client, owner_tokens, conversation_id, bodies)

        first_page = client.get(
            f"/conversations/{conversation_id}/messages?limit=2",
            headers=auth_header(owner_tokens),
        )
        second_page = client.get(
            f"/conversations/{conversation_id}/messages"
            f"?limit=2&cursor={first_page.json()['next_cursor']}",
            headers=auth_header(owner_tokens),
        )
        third_page = client.get(
            f"/conversations/{conversation_id}/messages"
            f"?limit=2&cursor={second_page.json()['next_cursor']}",
            headers=auth_header(owner_tokens),
        )

    assert first_page.status_code == 200
    assert second_page.status_code == 200
    assert third_page.status_code == 200
    assert first_page.json()["next_cursor"] is not None
    assert second_page.json()["next_cursor"] is not None
    assert third_page.json()["next_cursor"] is None

    all_items = (
        first_page.json()["items"] + second_page.json()["items"] + third_page.json()["items"]
    )
    ids = [item["id"] for item in all_items]
    assert len(ids) == 5
    assert len(set(ids)) == 5
    # Newest first: sequence_number 5..1.
    assert [item["sequence_number"] for item in all_items] == [5, 4, 3, 2, 1]
    assert [item["body"] for item in all_items] == list(reversed(bodies))


def test_large_history_pages_without_loading_everything_at_once(app: FastAPI) -> None:
    # Stays under rate_limit_message_send_max_events (20 per window; see
    # app/core/config.py) so every send succeeds - this test is about
    # pagination, not rate limiting.
    total_messages = 15
    page_size = 5
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)
        bodies = [f"message {i}" for i in range(1, total_messages + 1)]
        send_via_websocket(client, owner_tokens, conversation_id, bodies)

        pages: list[dict[str, object]] = []
        cursor: str | None = None
        for _ in range(10):
            url = f"/conversations/{conversation_id}/messages?limit={page_size}"
            if cursor:
                url += f"&cursor={cursor}"
            response = client.get(url, headers=auth_header(owner_tokens))
            assert response.status_code == 200
            payload = response.json()
            assert len(payload["items"]) <= page_size
            pages.append(payload)
            cursor = payload["next_cursor"]
            if cursor is None:
                break

    assert cursor is None
    assert len(pages) == total_messages // page_size
    all_items = [item for page in pages for item in page["items"]]
    assert len(all_items) == total_messages
    assert len({item["id"] for item in all_items}) == total_messages
    assert [item["sequence_number"] for item in all_items] == list(range(total_messages, 0, -1))


def test_edited_and_deleted_messages_match_replay_current_state(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()

            owner_ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": str(uuid.uuid4()),
                    "body": "original",
                }
            )
            edited_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(
                {
                    "type": "message:edit",
                    "conversation_id": conversation_id,
                    "message_id": edited_event["id"],
                    "body": "corrected",
                }
            )
            edited = owner_ws.receive_json()

            owner_ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": str(uuid.uuid4()),
                    "body": "sensitive - delete me",
                }
            )
            deleted_event = owner_ws.receive_json()
            owner_ws.receive_json()  # message:ack

            owner_ws.send_json(
                {
                    "type": "message:delete",
                    "conversation_id": conversation_id,
                    "message_id": deleted_event["id"],
                }
            )
            deleted = owner_ws.receive_json()

        # Replay via reconnect, for comparison against the REST history view.
        with client.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
            replay = member_ws.receive_json()
            for _ in replay["messages"]:
                member_ws.receive_json()  # message:delivered

        history_response = client.get(
            f"/conversations/{conversation_id}/messages",
            headers=auth_header(owner_tokens),
        )

    assert history_response.status_code == 200
    history_by_id = {item["id"]: item for item in history_response.json()["items"]}
    replay_by_id = {item["id"]: item for item in replay["messages"]}

    # The REST response always includes every field (body/edited_at/deleted_at
    # explicitly null where inapplicable, matching this app's other list
    # endpoints - e.g. ConversationSummary.title), while message:replay omits
    # inapplicable keys entirely for a terser wire format. Same underlying
    # current-state values either way (both built from MessageOut.from_message).
    edited_history_item = history_by_id[edited_event["id"]]
    edited_replay_item = replay_by_id[edited_event["id"]]
    assert edited_history_item["body"] == "corrected" == edited_replay_item["body"]
    assert edited_history_item["edited_at"] == edited["edited_at"]
    assert edited_history_item["edited_at"] == edited_replay_item["edited_at"]
    assert edited_history_item["deleted_at"] is None
    assert "deleted_at" not in edited_replay_item

    deleted_history_item = history_by_id[deleted_event["id"]]
    deleted_replay_item = replay_by_id[deleted_event["id"]]
    assert deleted_history_item["body"] is None
    assert "body" not in deleted_replay_item
    assert deleted_history_item["deleted_at"] == deleted["deleted_at"]
    assert deleted_history_item["deleted_at"] == deleted_replay_item["deleted_at"]


def test_invalid_cursor_is_rejected(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, member_id = register_user(client, "member@example.com")
        conversation_id = create_direct_conversation(client, owner_tokens, member_id)

        response = client.get(
            f"/conversations/{conversation_id}/messages?cursor=not-a-real-cursor",
            headers=auth_header(owner_tokens),
        )

    assert response.status_code == 422
