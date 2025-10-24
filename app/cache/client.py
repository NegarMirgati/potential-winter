from typing import AsyncGenerator, Optional
import redis.asyncio as redis
from fastapi import FastAPI
from app.core.config import settings

# Global Redis client instance
redis_client: Optional[redis.Redis] = None


def init_redis(app: FastAPI) -> None:
    """
    Initialize Redis connection and attach it to FastAPI app state.
    Should be called on startup.
    """
    global redis_client
    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
    )
    app.state.redis = redis_client


async def close_redis(app: FastAPI) -> None:
    """
    Close Redis connection on shutdown.
    """
    global redis_client
    if redis_client:
        try:
            await redis_client.close()
            await redis_client.connection_pool.disconnect()
        finally:
            redis_client = None
            if hasattr(app.state, "redis"):
                del app.state.redis
