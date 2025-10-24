import time
import os
from redis.asyncio import Redis

MAX_CACHE = int(os.getenv("REDIS_MAX_CACHE", 10))
TTL_SECONDS = int(os.getenv("REDIS_TTL", 600))
CACHE_ZSET = os.getenv("REDIS_ZSET", "lru_cache_keys")


async def get_cached(redis: Redis, key: str):
    value = await redis.get(key)
    if value:
        # Update LRU timestamp
        await redis.zadd(CACHE_ZSET, {key: time.time()})
        return value
    return None


async def set_cached(redis: Redis, key: str, value: str):
    await redis.set(key, value, ex=TTL_SECONDS)
    await redis.zadd(CACHE_ZSET, {key: time.time()})

    # get the zset's cardinality
    count = await redis.zcard(CACHE_ZSET)
    if count > MAX_CACHE:
        oldest = await redis.zrange(CACHE_ZSET, 0, count - MAX_CACHE - 1)
        if oldest:
            await redis.delete(*oldest)
            await redis.zrem(CACHE_ZSET, *oldest)
