from redis.asyncio import Redis

from app.core.config import Settings


def create_redis_client(settings: Settings) -> Redis:
    client: Redis = Redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    return client
