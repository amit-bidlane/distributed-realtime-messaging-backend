import asyncio

import fakeredis

from app.redis.rate_limit import check_rate_limit


def test_requests_within_the_limit_are_allowed() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        first = await check_rate_limit(redis, "k", max_events=2, window_seconds=60)
        second = await check_rate_limit(redis, "k", max_events=2, window_seconds=60)

        assert first.allowed is True
        assert second.allowed is True

    asyncio.run(scenario())


def test_request_exceeding_the_limit_is_rejected_with_a_positive_retry_after() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        await check_rate_limit(redis, "k", max_events=2, window_seconds=60)
        await check_rate_limit(redis, "k", max_events=2, window_seconds=60)
        third = await check_rate_limit(redis, "k", max_events=2, window_seconds=60)

        assert third.allowed is False
        assert 0 < third.retry_after_seconds <= 60

    asyncio.run(scenario())


def test_distinct_keys_are_tracked_independently() -> None:
    async def scenario() -> None:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        await check_rate_limit(redis, "a", max_events=1, window_seconds=60)
        exhausted_a = await check_rate_limit(redis, "a", max_events=1, window_seconds=60)
        fresh_b = await check_rate_limit(redis, "b", max_events=1, window_seconds=60)

        assert exhausted_a.allowed is False
        assert fresh_b.allowed is True

    asyncio.run(scenario())


def test_first_use_of_a_key_sets_its_window_ttl() -> None:
    """The key's expiry must be attached atomically with its first
    increment (INCR + EXPIRE ... NX in one pipeline - see
    app.redis.rate_limit.check_rate_limit's docstring), or a crash between
    two separate round-trips could leave a counter with no expiry, locking
    the key out forever."""

    async def scenario() -> int:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        await check_rate_limit(redis, "k", max_events=5, window_seconds=45)
        return await redis.ttl("k")

    ttl = asyncio.run(scenario())

    assert 0 < ttl <= 45


def test_later_requests_in_the_same_window_do_not_extend_its_ttl() -> None:
    """NX on the EXPIRE means only the request that creates the key sets its
    TTL - later requests in the same window must not push the expiry
    forward, or a steady trickle of requests could keep a key alive
    indefinitely instead of resetting on a fixed wall-clock boundary."""

    async def scenario() -> int:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        await check_rate_limit(redis, "k", max_events=5, window_seconds=45)
        await redis.expire("k", 5)  # simulate 40 of the 45 seconds having already elapsed
        await check_rate_limit(redis, "k", max_events=5, window_seconds=45)
        return await redis.ttl("k")

    ttl_after_second_call = asyncio.run(scenario())

    # If EXPIRE ran unconditionally (NX omitted), this would jump back up
    # to (near) 45 instead of staying at the shrunken value.
    assert ttl_after_second_call <= 5


def test_rejection_reports_the_keys_actual_remaining_ttl_as_retry_after() -> None:
    async def scenario() -> tuple[int, int]:
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        await check_rate_limit(redis, "k", max_events=1, window_seconds=30)
        rejected = await check_rate_limit(redis, "k", max_events=1, window_seconds=30)
        actual_ttl = await redis.ttl("k")
        return rejected.retry_after_seconds, actual_ttl

    retry_after, actual_ttl = asyncio.run(scenario())

    assert retry_after == actual_ttl
