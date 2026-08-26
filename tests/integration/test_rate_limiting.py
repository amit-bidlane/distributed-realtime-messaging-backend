import asyncio
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
WRONG_PASSWORD = "wrong-password-value"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'rate_limiting.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
        rate_limit_login_max_attempts=2,
        rate_limit_login_window_seconds=60,
        rate_limit_register_max_attempts=2,
        rate_limit_register_window_seconds=60,
    )
    application = create_app(settings, redis_client=fakeredis.FakeAsyncRedis(decode_responses=True))

    async def create_schema() -> None:
        async with application.state.db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(create_schema())
    yield application
    asyncio.run(application.state.db_engine.dispose())


def test_registration_allows_requests_within_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        first = client.post(
            "/auth/register",
            json={"email": "one@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        second = client.post(
            "/auth/register",
            json={"email": "two@example.com", "password": PASSWORD, "device_label": "Test"},
        )

    assert first.status_code == 201
    assert second.status_code == 201


def test_registration_rejects_requests_over_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        client.post(
            "/auth/register",
            json={"email": "one@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        client.post(
            "/auth/register",
            json={"email": "two@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        third = client.post(
            "/auth/register",
            json={"email": "three@example.com", "password": PASSWORD, "device_label": "Test"},
        )

    assert third.status_code == 429
    assert third.json() == {"detail": "too many requests"}
    assert "Retry-After" in third.headers


def test_login_allows_requests_within_the_limit(app: FastAPI) -> None:
    with TestClient(app) as client:
        client.post(
            "/auth/register",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        first = client.post(
            "/auth/login",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        second = client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": WRONG_PASSWORD,
                "device_label": "Test",
            },
        )

    assert first.status_code == 200
    assert second.status_code == 401


def test_login_rejects_requests_over_the_limit_even_with_valid_credentials(app: FastAPI) -> None:
    with TestClient(app) as client:
        client.post(
            "/auth/register",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": WRONG_PASSWORD,
                "device_label": "Test",
            },
        )
        client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": WRONG_PASSWORD,
                "device_label": "Test",
            },
        )
        third = client.post(
            "/auth/login",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test"},
        )

    assert third.status_code == 429
    assert third.json() == {"detail": "too many requests"}


def test_login_and_registration_limits_are_independent(app: FastAPI) -> None:
    with TestClient(app) as client:
        client.post(
            "/auth/register",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test"},
        )
        # Two failed logins exhaust the login limit...
        client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": WRONG_PASSWORD,
                "device_label": "Test",
            },
        )
        client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": WRONG_PASSWORD,
                "device_label": "Test",
            },
        )
        # ...but registration still has budget left, since the two scopes
        # are tracked under separate Redis keys for the same client.
        second_registration = client.post(
            "/auth/register",
            json={"email": "someone-else@example.com", "password": PASSWORD, "device_label": "T"},
        )

    assert second_registration.status_code == 201
