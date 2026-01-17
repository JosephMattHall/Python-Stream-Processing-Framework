import redis
import asyncio
from typing import Optional
from pspf.log.interfaces import OffsetStore
from pspf.runtime.dedup import DeduplicationStore

class RedisOffsetStore(OffsetStore):
    """
    Durable Offset Store backed by Redis.
    Structure: HASH {prefix}:{group_id} -> {partition} -> {offset}
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, prefix: str = 'pspf:offsets'):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.prefix = prefix

    async def get_offset(self, consumer_group: str, partition: int) -> Optional[int]:
        # Sync Redis call wrapped in thread execution if needed, 
        # but for simple GET/SET fast local redis, direct call is often acceptable in async
        # for low throughput. Ideally use aioredis (now part of redis-py 4.2+ via .aio)
        
        # Using redis.asyncio since we installed redis>=5.0.0
        import redis.asyncio as aioredis
        self.redis = aioredis.Redis(host=self.redis.connection_pool.connection_kwargs['host'],
                                    port=self.redis.connection_pool.connection_kwargs['port'],
                                    decode_responses=True)
        
        key = f"{self.prefix}:{consumer_group}"
        val = await self.redis.hget(key, str(partition))
        return int(val) if val is not None else None

    async def commit_offset(self, consumer_group: str, partition: int, offset: int) -> None:
        key = f"{self.prefix}:{consumer_group}"
        await self.redis.hset(key, str(partition), str(offset))


class RedisDeduplicationStore(DeduplicationStore):
    """
    Durable Deduplication Store backed by Redis.
    Uses SET with Expiry for idempotency.
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, ttl_seconds: int = 86400, prefix: str = 'pspf:dedup'):
        import redis.asyncio as aioredis
        self.redis = aioredis.Redis(host=host, port=port, decode_responses=True)
        self.ttl = ttl_seconds
        self.prefix = prefix

    async def has_processed(self, event_id: str) -> bool:
        key = f"{self.prefix}:{event_id}"
        exists = await self.redis.exists(key)
        return bool(exists)

    async def mark_processed(self, event_id: str) -> None:
        key = f"{self.prefix}:{event_id}"
        # Set with TTL
        await self.redis.set(key, "1", ex=self.ttl)
