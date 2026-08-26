from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int


async def check_rate_limit(
    redis: Redis,
    key: str,
    *,
    max_events: int,
    window_seconds: int,
) -> RateLimitDecision:
    """Fixed-window counter: at most `max_events` per `window_seconds` per key.

    `INCR` and `EXPIRE ... NX` run in one pipeline so the count and its TTL
    are set atomically from the caller's point of view - a process crashing
    between two separate round-trips can never leave the key's count
    incremented with no expiry attached (which would otherwise let one bad
    interleaving permanently lock a key out). `NX` on the expire means only
    the request that actually created the key (count == 1) sets its TTL;
    every later request in the same window extends nothing, so the window
    is a fixed wall-clock slice from first use, not a rolling one.

    A fixed window trades a known edge case (roughly 2x `max_events` can
    land across a window boundary, e.g. `max_events` right at the end of one
    window followed by `max_events` right at the start of the next) for an
    implementation simple enough to explain and test in one call - a
    sliding-window log or token bucket removes that edge case at the cost of
    materially more Redis state and complexity, which isn't justified here.

    Generic by design: `key` is fully caller-constructed (see
    app.core.rate_limit for the HTTP wiring and app.websocket.router for the
    WebSocket wiring), so this same function covers login/registration
    throttling today and can cover contact sync without changes.
    """
    pipe = redis.pipeline()
    pipe.incr(key)
    pipe.expire(key, window_seconds, nx=True)
    count, _ = await pipe.execute()
    if count <= max_events:
        return RateLimitDecision(allowed=True, retry_after_seconds=0)
    ttl = await redis.ttl(key)
    return RateLimitDecision(allowed=False, retry_after_seconds=ttl if ttl > 0 else window_seconds)
