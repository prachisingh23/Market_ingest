"""
Centralized Redis connection manager to eliminate duplicate connection logic.
"""
import redis.asyncio as redis
from typing import Optional
from utils.logger import setup_logger
logger = setup_logger("redis-manager")
class RedisManager:
    """
    Singleton-style Redis connection manager.
    Provides pooled connections with automatic reconnection.
    """
    _pools: dict[str, redis.ConnectionPool] = {}
    _clients: dict[str, redis.Redis] = {}
    @classmethod
    async def get_client(
        cls,
        redis_url: str,
        pool_size: int = 20,
        decode_responses: bool = True,
        name: Optional[str] = None
    ) -> redis.Redis:
        """
        Get or create a Redis client with connection pooling.
        Args:
            redis_url: Redis connection URL
            pool_size: Maximum connections in pool
            decode_responses: Whether to decode responses to strings
            name: Optional client name for tracking
        Returns:
            Redis client instance
        """
        key = f"{redis_url}:{decode_responses}:{name or 'default'}"
        if key not in cls._clients or not cls._clients[key]:
            pool_key = f"{redis_url}:{pool_size}:{decode_responses}"
            # Create or reuse connection pool
            if pool_key not in cls._pools:
                logger.info(f"Creating Redis pool | size={pool_size} | decode={decode_responses}")
                cls._pools[pool_key] = redis.ConnectionPool.from_url(
                    redis_url,
                    max_connections=pool_size,
                    decode_responses=decode_responses,
                    socket_keepalive=True,
                    socket_connect_timeout=3,
                    retry_on_timeout=True,
                    health_check_interval=30,
                )
            cls._clients[key] = redis.Redis(connection_pool=cls._pools[pool_key])
            await cls._clients[key].ping()
            logger.success(f"✅ Redis client ready: {name or 'default'}")
        return cls._clients[key]
    @classmethod
    async def close_all(cls) -> None:
        """Close all Redis connections and pools."""
        for client in cls._clients.values():
            if client:
                await client.aclose()
        for pool in cls._pools.values():
            if pool:
                await pool.aclose()
        cls._clients.clear()
        cls._pools.clear()
        logger.info("All Redis connections closed")
