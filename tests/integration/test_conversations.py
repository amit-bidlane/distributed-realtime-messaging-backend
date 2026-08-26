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
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'conversations.db'}",
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


def authorization_header(tokens: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def test_creates_direct_and_group_conversations(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        _, participant_id = register_user(client, "participant@example.com")
        headers = authorization_header(owner_tokens)

        direct = client.post(
            "/conversations",
            headers=headers,
            json={"kind": "direct", "member_ids": [str(participant_id)]},
        )
        repeated_direct = client.post(
            "/conversations",
            headers=headers,
            json={"kind": "direct", "member_ids": [str(participant_id)]},
        )
        group = client.post(
            "/conversations",
            headers=headers,
            json={
                "kind": "group",
                "title": "Project room",
                "member_ids": [str(participant_id)],
            },
        )

    assert direct.status_code == 201
    assert direct.json()["kind"] == "direct"
    assert direct.json()["title"] is None
    assert direct.json()["member_count"] == 2
    assert repeated_direct.status_code == 200
    assert repeated_direct.json()["id"] == direct.json()["id"]
    assert group.status_code == 201
    assert group.json()["kind"] == "group"
    assert group.json()["title"] == "Project room"


def test_listing_is_private_and_cursor_paginated(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        participant_tokens, participant_id = register_user(client, "participant@example.com")
        _, outsider_id = register_user(client, "outsider@example.com")
        owner_headers = authorization_header(owner_tokens)

        for title in ("One", "Two", "Three"):
            response = client.post(
                "/conversations",
                headers=owner_headers,
                json={"kind": "group", "title": title, "member_ids": [str(participant_id)]},
            )
            assert response.status_code == 201

        first_page = client.get("/conversations?limit=2", headers=owner_headers)
        second_page = client.get(
            f"/conversations?limit=2&cursor={first_page.json()['next_cursor']}",
            headers=owner_headers,
        )
        participant_page = client.get(
            "/conversations", headers=authorization_header(participant_tokens)
        )
        outsider_tokens, _ = register_user(client, "another-outsider@example.com")
        outsider_page = client.get("/conversations", headers=authorization_header(outsider_tokens))

    assert first_page.status_code == 200
    assert first_page.json()["next_cursor"] is not None
    assert second_page.status_code == 200
    ids = [item["id"] for item in first_page.json()["items"] + second_page.json()["items"]]
    assert len(ids) == 3
    assert len(set(ids)) == 3
    assert set(first_page.json()["items"][0]) == {
        "id",
        "kind",
        "title",
        "created_at",
        "member_count",
    }
    assert len(participant_page.json()["items"]) == 3
    assert outsider_id
    assert outsider_page.json() == {"items": [], "next_cursor": None}


def test_membership_authorization_and_group_management(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")
        member_tokens, member_id = register_user(client, "member@example.com")
        outsider_tokens, outsider_id = register_user(client, "outsider@example.com")
        owner_headers = authorization_header(owner_tokens)
        conversation = client.post(
            "/conversations",
            headers=owner_headers,
            json={"kind": "group", "title": "Private room", "member_ids": [str(member_id)]},
        )
        conversation_id = conversation.json()["id"]

        outsider_read = client.get(
            f"/conversations/{conversation_id}", headers=authorization_header(outsider_tokens)
        )
        outsider_add = client.post(
            f"/conversations/{conversation_id}/members",
            headers=authorization_header(outsider_tokens),
            json={"user_id": str(outsider_id)},
        )
        member_add = client.post(
            f"/conversations/{conversation_id}/members",
            headers=authorization_header(member_tokens),
            json={"user_id": str(outsider_id)},
        )
        owner_add = client.post(
            f"/conversations/{conversation_id}/members",
            headers=owner_headers,
            json={"user_id": str(outsider_id)},
        )
        outsider_read_after_add = client.get(
            f"/conversations/{conversation_id}", headers=authorization_header(outsider_tokens)
        )
        owner_remove = client.delete(
            f"/conversations/{conversation_id}/members/{outsider_id}", headers=owner_headers
        )
        outsider_read_after_removal = client.get(
            f"/conversations/{conversation_id}", headers=authorization_header(outsider_tokens)
        )

    assert conversation.status_code == 201
    assert outsider_read.status_code == 404
    assert outsider_add.status_code == 404
    assert member_add.status_code == 403
    assert owner_add.status_code == 201
    assert owner_add.json()["user_id"] == str(outsider_id)
    assert outsider_read_after_add.status_code == 200
    assert owner_remove.status_code == 204
    assert outsider_read_after_removal.status_code == 404
