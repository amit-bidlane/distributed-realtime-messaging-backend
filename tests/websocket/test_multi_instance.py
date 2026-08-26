import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import Settings
from app.db.base import Base
from app.main import create_app
from app.presence.service import PresenceSnapshot, get_presence

PASSWORD = "correct-horse-battery-staple"
TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


@pytest.fixture
def two_instances(tmp_path: Path) -> Iterator[tuple[FastAPI, FastAPI]]:
    """Two independently-built FastAPI apps standing in for fastapi-1/fastapi-2
    in docker-compose.yml.

    Each gets its own ConnectionManager and its own EventFanout instance_id
    (so neither ever treats the other's publishes as "self" and discards
    them - see app.redis.pubsub.EventFanout), but they share one fake Redis
    server and one SQLite database file, mirroring how the real instances
    share one Redis and one PostgreSQL. NullPool avoids the pooled-aiosqlite
    connection-reuse hang documented on
    tests/websocket/test_presence.py::test_activity_refreshes_heartbeat,
    which is more likely to surface here since two instances' engines touch
    the same file concurrently.
    """
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'multi_instance.db'}"
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


def _create_direct_conversation(
    client: TestClient, owner_tokens: dict[str, str], member_id: uuid.UUID
) -> str:
    response = client.post(
        "/conversations",
        headers=auth_header(owner_tokens),
        json={"kind": "direct", "member_ids": [str(member_id)]},
    )
    assert response.status_code == 201
    return str(response.json()["id"])


def _await_fanout_ready(client: TestClient, app: FastAPI) -> None:
    """Block until this app's Redis Pub/Sub subscription is confirmed active.

    Production startup deliberately does *not* wait for this (see
    app.main's lifespan - it would make every instance hard-fail to start
    whenever Redis is unreachable, which most of this test suite's own
    fixtures rely on not happening). These tests are specifically about the
    fanout mechanism itself, so unlike production code they need the
    subscription guaranteed active before publishing, or a publish could
    race the other instance's subscribe and simply never be delivered - the
    same fire-and-forget behavior documented on EventFanout, just not what
    this particular test is trying to exercise.
    """
    client.portal.call(app.state.fanout.ready.wait)


def test_message_fans_out_from_one_instance_to_a_recipient_on_another(
    two_instances: tuple[FastAPI, FastAPI],
) -> None:
    """A -> FastAPI #1 -> Redis -> FastAPI #2 -> B: the cross-instance fanout scenario
    this test exists to prove.

    Owner connects only to app1, member connects only to app2. Neither
    TestClient/app ever talks to the other directly - the only channel
    between them is the shared fake Redis, exactly like the real
    docker-compose services only share Redis and PostgreSQL.
    """
    app1, app2 = two_instances
    with TestClient(app1) as client1, TestClient(app2) as client2:
        _await_fanout_ready(client1, app1)
        _await_fanout_ready(client2, app2)

        owner_tokens, _ = register_user(client1, "owner@example.com")
        member_tokens, member_id = register_user(client2, "member@example.com")
        conversation_id = _create_direct_conversation(client1, owner_tokens, member_id)

        with (
            client1.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws,
            client2.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws,
        ):
            assert owner_ws.receive_json() == {"type": "auth:authenticated"}
            assert member_ws.receive_json() == {"type": "auth:authenticated"}

            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()

            client_message_id = str(uuid.uuid4())
            owner_ws.send_json(
                {
                    "type": "message:send",
                    "conversation_id": conversation_id,
                    "client_message_id": client_message_id,
                    "body": "hello from instance 1",
                }
            )

            # Instance 1's own local delivery to the sender.
            owner_new = owner_ws.receive_json()
            assert owner_new["type"] == "message:new"
            assert owner_ws.receive_json() == {
                "type": "message:ack",
                "client_message_id": client_message_id,
                "id": owner_new["id"],
                "sequence_number": owner_new["sequence_number"],
                "duplicate": False,
            }

            # Member's socket only exists on instance 2, which never saw
            # this send - this frame can only have arrived via Redis Pub/Sub
            # (app.redis.pubsub.EventFanout / app.websocket.router.handle_remote_event).
            member_new = member_ws.receive_json()
            assert member_new["type"] == "message:new"
            assert member_new["id"] == owner_new["id"]
            assert member_new["body"] == "hello from instance 1"


def test_typing_indicator_fans_out_across_instances(
    two_instances: tuple[FastAPI, FastAPI],
) -> None:
    app1, app2 = two_instances
    with TestClient(app1) as client1, TestClient(app2) as client2:
        _await_fanout_ready(client1, app1)
        _await_fanout_ready(client2, app2)

        owner_tokens, owner_id = register_user(client1, "owner@example.com")
        member_tokens, member_id = register_user(client2, "member@example.com")
        conversation_id = _create_direct_conversation(client1, owner_tokens, member_id)

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

            owner_ws.send_json({"type": "typing:start", "conversation_id": conversation_id})

            event = member_ws.receive_json()
            assert event == {
                "type": "typing:start",
                "conversation_id": conversation_id,
                "user_id": str(owner_id),
            }


async def _presence_from(app: FastAPI, user_id: uuid.UUID) -> PresenceSnapshot:
    ttl = app.state.settings.presence_heartbeat_ttl_seconds
    return await get_presence(app.state.redis, user_id, heartbeat_ttl_seconds=ttl)


def test_client_recovers_through_a_surviving_instance_after_its_own_instance_fails(
    two_instances: tuple[FastAPI, FastAPI],
) -> None:
    """Instance failure. Everything a client needs to pick back up lives in
    PostgreSQL/Redis, not in any one instance's in-memory ConnectionManager
    (app.websocket.manager), so a client whose instance disappears entirely
    can reconnect through *any* surviving instance and fully recover -
    missed messages replay, presence still resolves correctly - with no
    special-cased failover logic anywhere. This is the same recovery
    mechanism already proven for an ordinary reconnect in
    tests/websocket/test_offline_recovery.py; the only new claim here is
    that it still works when the *original* instance is the one that's
    gone, not just when the client's own socket happens to drop.
    """
    app1, app2 = two_instances
    with TestClient(app1) as client1, TestClient(app2) as client2:
        _await_fanout_ready(client1, app1)
        _await_fanout_ready(client2, app2)

        owner_tokens, _ = register_user(client1, "owner@example.com")
        member_tokens, member_id = register_user(client2, "member@example.com")
        conversation_id = _create_direct_conversation(client1, owner_tokens, member_id)

        with client1.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws:
            owner_ws.receive_json()
            owner_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws.receive_json()
        # The owner's connection to instance 1 ends here - from the owner's
        # point of view this is indistinguishable from instance 1 crashing:
        # either way, they simply stop receiving frames and must reconnect
        # somewhere. Instance 1 (app1) is never used again in this test.

        # The member, still connected to instance 2 the whole time, sends
        # messages while the owner has no live connection anywhere.
        with client2.websocket_connect("/ws", headers=auth_header(member_tokens)) as member_ws:
            member_ws.receive_json()
            member_ws.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            member_ws.receive_json()
            for body in ("one", "two"):
                member_ws.send_json(
                    {
                        "type": "message:send",
                        "conversation_id": conversation_id,
                        "client_message_id": str(uuid.uuid4()),
                        "body": body,
                    }
                )
                member_ws.receive_json()  # message:new
                member_ws.receive_json()  # message:ack (owner offline: no message:delivered)

        # The owner reconnects through instance 2 instead of instance 1 -
        # standing in for Nginx routing them to a surviving backend once the
        # original one is gone (nginx/nginx.conf's `least_conn` upstream).
        with client2.websocket_connect("/ws", headers=auth_header(owner_tokens)) as owner_ws2:
            assert owner_ws2.receive_json() == {"type": "auth:authenticated"}
            owner_ws2.send_json({"type": "conversation:join", "conversation_id": conversation_id})
            owner_ws2.receive_json()  # conversation:joined
            replay = owner_ws2.receive_json()

    assert replay["type"] == "message:replay"
    assert [m["body"] for m in replay["messages"]] == ["one", "two"]
    assert [m["sequence_number"] for m in replay["messages"]] == [1, 2]


def test_presence_is_visible_from_a_different_instance(
    two_instances: tuple[FastAPI, FastAPI],
) -> None:
    """Presence needs no Pub/Sub relay at all: it is written directly to Redis
    (see app.presence.service), which both instances already share, so this
    only has to prove that shared state is actually visible - not that
    anything was fanned out.
    """
    app1, app2 = two_instances
    with TestClient(app1) as client1, TestClient(app2) as client2:
        tokens, user_id = register_user(client1, "owner@example.com")

        with client1.websocket_connect("/ws", headers=auth_header(tokens)) as ws:
            ws.receive_json()

            # Queried through app2's own portal/event loop and app2's own
            # Redis client - this is instance 2 looking up a user it has
            # never seen a connection from.
            snapshot: PresenceSnapshot = client2.portal.call(_presence_from, app2, user_id)

    assert snapshot.online is True
    assert snapshot.active_connection_count == 1
