import logging
from collections.abc import Awaitable, Callable

from fastapi import HTTPException, Request, status
from redis.asyncio import Redis

from app.core.config import Settings
from app.core.metrics import redis_errors
from app.redis.rate_limit import check_rate_limit

logger = logging.getLogger(__name__)

RateLimitDependency = Callable[[Request], Awaitable[None]]


def _client_ip(request: Request) -> str:
    """Best-effort caller identity for pre-auth (no JWT yet) rate limiting.

    Trusts `X-Forwarded-For` because the only path into this app in
    docker-compose is through Nginx (see nginx/nginx.conf), whose
    `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for` overwrites
    any value a client tries to spoof rather than appending to it - the
    value FastAPI sees is Nginx's own view of the connecting IP, not
    something the client controls directly. Falls back to
    `request.client.host` for direct-to-FastAPI access (e.g. hitting
    localhost:8001 in local dev, or the test client).
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def build_rate_limiter(
    scope: str, limits: Callable[[Settings], tuple[int, int]]
) -> RateLimitDependency:
    """Build a FastAPI dependency enforcing a Redis-backed rate limit keyed by client IP.

    `limits` reads (max_events, window_seconds) from Settings at request
    time rather than at import time, since each app instance (and each test
    fixture) can carry its own Settings - see login_rate_limit/
    register_rate_limit below for the two current callers, and the
    contact-sync endpoint for a third.
    """

    async def dependency(request: Request) -> None:
        settings: Settings = request.app.state.settings
        redis: Redis = request.app.state.redis
        max_events, window_seconds = limits(settings)
        key = f"ratelimit:{scope}:{_client_ip(request)}"
        try:
            decision = await check_rate_limit(
                redis, key, max_events=max_events, window_seconds=window_seconds
            )
        except Exception:
            # Fail open: rate limiting is abuse protection, not a security
            # boundary like authentication, which must fail closed. A Redis
            # outage must not take down login/registration/contact sync
            # entirely just because Redis - not the source of truth for any
            # of them - is briefly unreachable. Every occurrence is logged
            # loudly so this degradation is visible in practice, never a
            # silent gap.
            redis_errors.labels(scope="http_rate_limit").inc()
            logger.warning("rate_limit_check_failed scope=%s key=%s", scope, key, exc_info=True)
            return
        if not decision.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="too many requests",
                headers={"Retry-After": str(decision.retry_after_seconds)},
            )

    return dependency


login_rate_limit = build_rate_limiter(
    "login", lambda s: (s.rate_limit_login_max_attempts, s.rate_limit_login_window_seconds)
)
register_rate_limit = build_rate_limiter(
    "register",
    lambda s: (s.rate_limit_register_max_attempts, s.rate_limit_register_window_seconds),
)
contacts_rate_limit = build_rate_limiter(
    "contacts",
    lambda s: (s.rate_limit_contacts_max_requests, s.rate_limit_contacts_window_seconds),
)
