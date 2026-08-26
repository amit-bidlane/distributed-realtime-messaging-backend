import asyncio
import io
import json
import logging
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from app.auth.models import User
from app.auth.passwords import hash_password
from app.conversations.models import ConversationType
from app.conversations.schemas import ConversationCreate
from app.conversations.service import create_conversation
from app.core.config import Settings
from app.core.logging import JsonFormatter, RequestIdFilter, bind_request_id
from app.db.base import Base
from app.db.session import create_engine, create_session_factory
from app.main import create_app
from app.messages.models import Message

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"
SENSITIVE_BODY = "the launch codes are 8675309, do not repeat this anywhere"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'observability.db'}",
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


def _read_gauge(client: TestClient, name: str) -> float:
    body = client.get("/metrics").text
    for line in body.splitlines():
        if line.startswith(f"{name} "):
            return float(line.split()[-1])
    raise AssertionError(f"metric {name} not found in /metrics output")


def test_http_requests_get_a_correlation_id(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.headers.get("X-Request-ID")


def test_http_request_id_from_client_is_echoed_back(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.get("/health", headers={"X-Request-ID": "caller-supplied-id"})

    assert response.headers["X-Request-ID"] == "caller-supplied-id"


def test_metrics_endpoint_exposes_plan_metrics(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    body = response.text
    for metric in (
        "active_connections",
        "messages_sent_total",
        "messages_failed_total",
        "message_send_latency_seconds",
        "redis_errors_total",
        "database_errors_total",
    ):
        assert metric in body


def test_active_connections_gauge_tracks_connect_and_disconnect(app: FastAPI) -> None:
    with TestClient(app) as client:
        owner_tokens, _ = register_user(client, "owner@example.com")

        before = _read_gauge(client, "active_connections")
        with client.websocket_connect("/ws", headers=auth_header(owner_tokens)) as ws:
            ws.receive_json()  # auth:authenticated
            during = _read_gauge(client, "active_connections")
        after = _read_gauge(client, "active_connections")

    assert during == before + 1
    assert after == before


def _make_capturing_logger(stream: io.StringIO) -> logging.Logger:
    logger = logging.getLogger("test.observability")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    logger.addHandler(handler)
    return logger


def test_exception_logging_includes_traceback_and_error_type() -> None:
    """Regression test for a prior JsonFormatter finding: it used to build
    its payload without ever looking at record.exc_info, so
    logger.exception(...)/logger.warning(..., exc_info=True) silently
    dropped the traceback - the log line looked identical to one with no
    error at all.
    """
    stream = io.StringIO()
    logger = _make_capturing_logger(stream)

    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("something_failed")

    payload = json.loads(stream.getvalue())
    assert payload["message"] == "something_failed"
    assert payload["error_type"] == "ValueError"
    assert "boom" in payload["exc_info"]
    assert "Traceback (most recent call last)" in payload["exc_info"]


def test_non_exception_logs_omit_exc_info_fields() -> None:
    stream = io.StringIO()
    logger = _make_capturing_logger(stream)

    logger.info("just_fine")

    payload = json.loads(stream.getvalue())
    assert "exc_info" not in payload
    assert "error_type" not in payload


def test_request_id_is_attached_only_while_bound() -> None:
    stream = io.StringIO()
    logger = _make_capturing_logger(stream)

    logger.info("before_bind")
    with bind_request_id("abc-123"):
        logger.info("during_bind")
    logger.info("after_bind")

    before, during, after = (json.loads(line) for line in stream.getvalue().splitlines())
    assert "request_id" not in before
    assert during["request_id"] == "abc-123"
    assert "request_id" not in after


def test_engine_hides_bound_parameters_so_message_bodies_never_leak_via_exceptions(
    tmp_path: Path,
) -> None:
    """Project design rule: never log plaintext message content.

    app.websocket.router's message_persist_failed handler wraps arbitrary
    persistence failures with exc_info=True, and JsonFormatter now actually
    renders that traceback (see the fix above - previously it was silently
    dropped, which is also why this specific leak was never observed in
    practice despite the logging call site looking correct). SQLAlchemy's
    own StatementError.__str__ embeds bound statement parameters by
    default, so without create_engine(..., hide_parameters=True) (see
    app.db.session), a genuine INSERT failure on the messages table would
    put the sender's plaintext message body into the exception's own string
    representation - and therefore into the structured log - even though no
    logger.* call site anywhere references `body` directly.
    """
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'sensitive.db'}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    async def scenario() -> IntegrityError:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        async with session_factory() as session:
            owner = User(email="owner@example.com", password_hash=hash_password("x"))
            member = User(email="member@example.com", password_hash=hash_password("x"))
            session.add_all([owner, member])
            await session.flush()
            conversation, _ = await create_conversation(
                session,
                owner.id,
                ConversationCreate(kind=ConversationType.DIRECT, member_ids=[member.id]),
            )
            conversation_id, sender_id = conversation.id, owner.id

        client_message_id = uuid.uuid4()
        async with session_factory() as session:
            session.add(
                Message(
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    client_message_id=client_message_id,
                    sequence_number=1,
                    body=SENSITIVE_BODY,
                )
            )
            await session.commit()

        # Raw duplicate insert, bypassing persist_message's own pre-check
        # entirely (same technique as test_messages.py's
        # test_database_rejects_duplicate_client_message_id_even_bypassing_the_app_check)
        # to force a genuine, engine-raised IntegrityError carrying the
        # sensitive body as a bound parameter.
        async with session_factory() as session:
            session.add(
                Message(
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    client_message_id=client_message_id,
                    sequence_number=2,
                    body=SENSITIVE_BODY,
                )
            )
            try:
                await session.flush()
            except IntegrityError as exc:
                return exc
        raise AssertionError("expected IntegrityError")

    try:
        exc = asyncio.run(scenario())
    finally:
        asyncio.run(engine.dispose())

    stream = io.StringIO()
    logger = _make_capturing_logger(stream)
    try:
        raise exc
    except IntegrityError:
        logger.exception("message_persist_failed conversation_id=%s", uuid.uuid4())

    raw_output = stream.getvalue()
    payload = json.loads(raw_output)
    assert payload["error_type"] == "IntegrityError"
    assert SENSITIVE_BODY not in raw_output
