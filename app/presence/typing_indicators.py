import uuid

from redis.asyncio import Redis


def _typing_key(conversation_id: uuid.UUID, user_id: uuid.UUID) -> str:
    return f"typing:{conversation_id}:{user_id}"


def _rate_limit_key(conversation_id: uuid.UUID, user_id: uuid.UUID) -> str:
    return f"typing:ratelimit:{conversation_id}:{user_id}"


async def start_typing(
    redis: Redis,
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    ttl_seconds: int,
    rate_limit_seconds: int,
) -> bool:
    """Record that user_id is typing in conversation_id.

    The typing flag itself is always (re)stamped with a fresh TTL, so a
    continuously-typing client keeps the indicator alive for onlookers.
    Returns whether *this call* should be broadcast: at most one broadcast
    per rate_limit_seconds per (user, conversation), enforced with a
    SET ... NX EX cooldown key - a client sending typing:start on every
    keystroke doesn't turn into a flood of WebSocket frames.
    """
    await redis.set(_typing_key(conversation_id, user_id), "1", ex=ttl_seconds)
    acquired = await redis.set(
        _rate_limit_key(conversation_id, user_id), "1", ex=rate_limit_seconds, nx=True
    )
    return bool(acquired)


async def stop_typing(redis: Redis, conversation_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    """Clear typing state for (conversation_id, user_id).

    Also clears the rate-limit cooldown, so a genuine stop-then-restart is
    never mistaken for spam. Returns whether the typing flag was actually
    set (i.e. whether this is worth broadcasting) - stopping typing that was
    never started, or already expired, is a no-op.
    """
    deleted = await redis.delete(_typing_key(conversation_id, user_id))
    await redis.delete(_rate_limit_key(conversation_id, user_id))
    return bool(deleted)
