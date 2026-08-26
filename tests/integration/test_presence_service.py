import asyncio
import time
import uuid

import fakeredis

from app.presence.service import get_presence, record_heartbeat, remove_connection

TTL_SECONDS = 30


def test_user_is_offline_before_any_heartbeat() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        snapshot = await get_presence(redis, uuid.uuid4(), heartbeat_ttl_seconds=TTL_SECONDS)
        assert snapshot.online is False
        assert snapshot.active_connection_count == 0
        assert snapshot.last_seen is None

    asyncio.run(scenario())


def test_heartbeat_marks_user_online() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        user_id = uuid.uuid4()
        await record_heartbeat(
            redis, user_id, uuid.uuid4(), heartbeat_ttl_seconds=TTL_SECONDS
        )
        snapshot = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert snapshot.online is True
        assert snapshot.active_connection_count == 1
        assert snapshot.last_seen is not None

    asyncio.run(scenario())


def test_user_stays_online_until_every_connection_is_gone() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        user_id = uuid.uuid4()
        connection_a, connection_b = uuid.uuid4(), uuid.uuid4()
        await record_heartbeat(redis, user_id, connection_a, heartbeat_ttl_seconds=TTL_SECONDS)
        await record_heartbeat(redis, user_id, connection_b, heartbeat_ttl_seconds=TTL_SECONDS)

        both_connected = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert both_connected.online is True
        assert both_connected.active_connection_count == 2

        await remove_connection(redis, user_id, connection_a)
        one_left = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert one_left.online is True
        assert one_left.active_connection_count == 1

        await remove_connection(redis, user_id, connection_b)
        none_left = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert none_left.online is False
        assert none_left.active_connection_count == 0

    asyncio.run(scenario())


def test_last_seen_persists_after_going_offline() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        user_id = uuid.uuid4()
        connection_id = uuid.uuid4()
        await record_heartbeat(redis, user_id, connection_id, heartbeat_ttl_seconds=TTL_SECONDS)
        await remove_connection(redis, user_id, connection_id)

        snapshot = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert snapshot.online is False
        assert snapshot.last_seen is not None

    asyncio.run(scenario())


def test_stale_heartbeat_is_offline_and_pruned() -> None:
    """A connection that vanished without a graceful disconnect (crash, dead
    network) is still treated as offline once its heartbeat is older than
    the TTL - and the stale hash entry is cleaned up as a side effect of
    reading it, rather than lingering forever."""

    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        user_id = uuid.uuid4()
        connection_id = uuid.uuid4()
        stale_timestamp = time.time() - (TTL_SECONDS + 60)
        await redis.hset(
            f"presence:connections:{user_id}", str(connection_id), str(stale_timestamp)
        )

        snapshot = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert snapshot.online is False
        assert snapshot.active_connection_count == 0

        remaining = await redis.hgetall(f"presence:connections:{user_id}")
        assert remaining == {}

    asyncio.run(scenario())


def test_multi_device_one_stale_one_fresh() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        user_id = uuid.uuid4()
        stale_connection, fresh_connection = uuid.uuid4(), uuid.uuid4()
        stale_timestamp = time.time() - (TTL_SECONDS + 60)
        await redis.hset(
            f"presence:connections:{user_id}", str(stale_connection), str(stale_timestamp)
        )
        await record_heartbeat(
            redis, user_id, fresh_connection, heartbeat_ttl_seconds=TTL_SECONDS
        )

        snapshot = await get_presence(redis, user_id, heartbeat_ttl_seconds=TTL_SECONDS)
        assert snapshot.online is True
        assert snapshot.active_connection_count == 1

    asyncio.run(scenario())
