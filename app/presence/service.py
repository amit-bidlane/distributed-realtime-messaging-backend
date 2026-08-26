import time
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis


def _connections_key(user_id: uuid.UUID) -> str:
    return f"presence:connections:{user_id}"


def _last_seen_key(user_id: uuid.UUID) -> str:
    return f"presence:last_seen:{user_id}"


@dataclass(frozen=True)
class PresenceSnapshot:
    online: bool
    active_connection_count: int
    last_seen: float | None


async def record_heartbeat(
    redis: Redis,
    user_id: uuid.UUID,
    connection_id: uuid.UUID,
    *,
    heartbeat_ttl_seconds: int,
) -> None:
    """Record that connection_id belonging to user_id is alive right now.

    Called on connect and again on every subsequent inbound event (see
    app.websocket.router), so a connection's entry only goes stale if the
    client falls silent for longer than heartbeat_ttl_seconds. This is a
    stronger liveness signal than relying solely on the ASGI/TCP disconnect
    event, which may never fire promptly for a silent network partition.

    Multi-device: connection_id is unique per WebSocket connection, not per
    user, so two devices for the same user hold independent fields in the
    same hash and neither can clobber the other's liveness state.
    """
    now = time.time()
    connections_key = _connections_key(user_id)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(connections_key, str(connection_id), str(now))
        # Outer TTL is only a safety net against unbounded growth for a user
        # who never comes back; sized well past the per-connection staleness
        # window so it never fires while any device is still heartbeating.
        pipe.expire(connections_key, heartbeat_ttl_seconds * 3)
        pipe.set(_last_seen_key(user_id), now)
        await pipe.execute()


async def remove_connection(redis: Redis, user_id: uuid.UUID, connection_id: uuid.UUID) -> None:
    """Record that connection_id is gone, stamping last_seen at this moment.

    A user is offline only once every connection has been removed here (a
    graceful disconnect) or has aged out of heartbeat_ttl_seconds without a
    refresh (an ungraceful one) - see get_presence.
    """
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hdel(_connections_key(user_id), str(connection_id))
        pipe.set(_last_seen_key(user_id), time.time())
        await pipe.execute()


async def get_presence(
    redis: Redis, user_id: uuid.UUID, *, heartbeat_ttl_seconds: int
) -> PresenceSnapshot:
    """Read a user's current presence.

    online is true iff at least one connection's last heartbeat is within
    heartbeat_ttl_seconds of now - i.e. the user is offline only when every
    active connection is gone or expired. Stale entries found during the
    read (a device that vanished without a graceful disconnect) are pruned
    as a side effect; this is the only cleanup path for those, since Redis
    key TTLs don't cascade into hash field removal.
    """
    connections_key = _connections_key(user_id)
    # redis-py's stubs type hash/set commands as `Awaitable[T] | T` (shared
    # between the sync and async clients), which mypy can't narrow from a
    # `Redis` (async-only) instance - these awaits are correct at runtime.
    connections = await redis.hgetall(connections_key)  # type: ignore[misc]
    last_seen_raw = await redis.get(_last_seen_key(user_id))

    now = time.time()
    stale_connection_ids = [
        connection_id
        for connection_id, heartbeat_at in connections.items()
        if now - float(heartbeat_at) > heartbeat_ttl_seconds
    ]
    if stale_connection_ids:
        await redis.hdel(connections_key, *stale_connection_ids)  # type: ignore[misc]

    active_count = len(connections) - len(stale_connection_ids)
    return PresenceSnapshot(
        online=active_count > 0,
        active_connection_count=active_count,
        last_seen=float(last_seen_raw) if last_seen_raw is not None else None,
    )
