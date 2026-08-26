import asyncio
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import fakeredis
import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.auth.tokens import ALGORITHM
from app.core.config import Settings
from app.db.base import Base
from app.main import create_app

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'authentication.db'}",
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


def register(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/auth/register",
        json={"email": "person@example.com", "password": PASSWORD, "device_label": "Test browser"},
    )
    assert response.status_code == 201
    return response.json()


def test_valid_login_and_authenticated_request(app: FastAPI) -> None:
    with TestClient(app) as client:
        register(client)
        login_response = client.post(
            "/auth/login",
            json={"email": "person@example.com", "password": PASSWORD, "device_label": "Laptop"},
        )

        assert login_response.status_code == 200
        tokens = login_response.json()
        profile_response = client.get(
            "/auth/me", headers={"Authorization": f"Bearer {tokens['access_token']}"}
        )

    assert profile_response.status_code == 200
    assert uuid.UUID(profile_response.json()["id"])


def test_invalid_login_is_rejected(app: FastAPI) -> None:
    with TestClient(app) as client:
        register(client)
        response = client.post(
            "/auth/login",
            json={
                "email": "person@example.com",
                "password": "wrong-password-value",
                "device_label": "Laptop",
            },
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "invalid credentials"}


def test_refresh_token_rotation_rejects_reuse(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens = register(client)
        rotation = client.post("/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
        reused_token = client.post("/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
        refreshed_token = client.post(
            "/auth/refresh", json={"refresh_token": rotation.json()["refresh_token"]}
        )

    assert rotation.status_code == 200
    assert rotation.json()["refresh_token"] != tokens["refresh_token"]
    assert reused_token.status_code == 401
    assert refreshed_token.status_code == 200


def test_revoked_session_invalidates_access_token(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens = register(client)
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        logout_response = client.post("/auth/logout", headers=headers)
        profile_response = client.get("/auth/me", headers=headers)

    assert logout_response.status_code == 204
    assert profile_response.status_code == 401


def test_expired_and_invalid_tokens_are_rejected(app: FastAPI) -> None:
    expired_token = jwt.encode(
        {
            "sub": str(uuid.uuid4()),
            "sid": str(uuid.uuid4()),
            "typ": "access",
            "jti": str(uuid.uuid4()),
            "exp": datetime.now(UTC) - timedelta(seconds=1),
        },
        TEST_JWT_SECRET,
        algorithm=ALGORITHM,
    )
    with TestClient(app) as client:
        expired_response = client.get(
            "/auth/me", headers={"Authorization": f"Bearer {expired_token}"}
        )
        invalid_response = client.get("/auth/me", headers={"Authorization": "Bearer malformed"})

    assert expired_response.status_code == 401
    assert invalid_response.status_code == 401


def test_websocket_requires_handshake_authorization_header(app: FastAPI) -> None:
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("/ws"):
                pass

    assert error.value.code == 1008


def test_websocket_accepts_a_valid_handshake_authorization_header(app: FastAPI) -> None:
    with TestClient(app) as client:
        tokens = register(client)
        with client.websocket_connect(
            "/ws", headers={"Authorization": f"Bearer {tokens['access_token']}"}
        ) as websocket:
            assert websocket.receive_json() == {"type": "auth:authenticated"}
