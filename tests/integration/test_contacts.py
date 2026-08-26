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
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'contacts.db'}",
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


def test_sync_matches_registered_users_by_normalized_email(app: FastAPI) -> None:
    with TestClient(app) as client:
        requester_tokens, _ = register_user(client, "requester@example.com")
        _, friend_id = register_user(client, "friend@example.com")

        response = client.post(
            "/contacts/sync",
            headers=auth_header(requester_tokens),
            json={
                "identifiers": [
                    "  Friend@Example.com  ",  # different case/whitespace, same account
                    "stranger@example.com",  # never registered
                    "not-an-email",  # malformed, must not error the whole request
                ]
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["matches"] == [
        {"identifier": "  Friend@Example.com  ", "user_id": str(friend_id)}
    ]


def test_sync_excludes_requesters_own_email(app: FastAPI) -> None:
    with TestClient(app) as client:
        requester_tokens, requester_id = register_user(client, "self@example.com")

        response = client.post(
            "/contacts/sync",
            headers=auth_header(requester_tokens),
            json={"identifiers": ["self@example.com"]},
        )

    assert response.status_code == 200
    assert response.json()["matches"] == []
    assert str(requester_id)  # sanity: id was actually issued


def test_sync_requires_authentication(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.post("/contacts/sync", json={"identifiers": ["someone@example.com"]})

    assert response.status_code == 401


def test_sync_rejects_empty_identifier_list(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens, _ = register_user(client, "requester@example.com")
        response = client.post(
            "/contacts/sync", headers=auth_header(tokens), json={"identifiers": []}
        )

    assert response.status_code == 422


def test_sync_duplicate_identifiers_for_the_same_match_are_not_duplicated(app: FastAPI) -> None:
    with TestClient(app) as client:
        requester_tokens, _ = register_user(client, "requester@example.com")
        _, friend_id = register_user(client, "friend@example.com")

        response = client.post(
            "/contacts/sync",
            headers=auth_header(requester_tokens),
            json={"identifiers": ["friend@example.com", "FRIEND@EXAMPLE.COM"]},
        )

    assert response.status_code == 200
    assert response.json()["matches"] == [
        {"identifier": "friend@example.com", "user_id": str(friend_id)}
    ]


def test_sync_is_rate_limited(tmp_path: Path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'contacts_rate_limit.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        rate_limit_contacts_max_requests=2,
        rate_limit_contacts_window_seconds=60,
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())

    with TestClient(application) as client:
        tokens, _ = register_user(client, "requester@example.com")
        headers = auth_header(tokens)
        payload = {"identifiers": ["someone@example.com"]}

        first = client.post("/contacts/sync", headers=headers, json=payload)
        second = client.post("/contacts/sync", headers=headers, json=payload)
        third = client.post("/contacts/sync", headers=headers, json=payload)

    asyncio.run(application.state.db_engine.dispose())

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429
    assert "Retry-After" in third.headers
