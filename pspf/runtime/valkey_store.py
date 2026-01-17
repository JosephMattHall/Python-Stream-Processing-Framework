import valkey.asyncio as valkey
from typing import Optional
from pspf.log.interfaces import OffsetStore
from pspf.runtime.dedup import DeduplicationStore

class ValkeyOffsetStore(OffsetStore):
    """
    Durable Offset Store backed by Valkey.
    Structure: HASH {prefix}:{group_id} -> {partition} -> {offset}
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, prefix: str = 'pspf:offsets'):
        self.valkey = valkey.Valkey(host=host, port=port, decode_responses=True)
        self.prefix = prefix

    async def get_offset(self, consumer_group: str, partition: int) -> Optional[int]:
        key = f"{self.prefix}:{consumer_group}"
        val = await self.valkey.hget(key, str(partition))
        return int(val) if val is not None else None

    async def commit_offset(self, consumer_group: str, partition: int, offset: int) -> None:
        key = f"{self.prefix}:{consumer_group}"
        await self.valkey.hset(key, str(partition), str(offset))


class ValkeyDeduplicationStore(DeduplicationStore):
    """
    Durable Deduplication Store backed by Valkey.
    Uses SET with Expiry for idempotency.
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, ttl_seconds: int = 86400, prefix: str = 'pspf:dedup'):
        self.valkey = valkey.Valkey(host=host, port=port, decode_responses=True)
        self.ttl = ttl_seconds
        self.prefix = prefix

    async def has_processed(self, event_id: str) -> bool:
        key = f"{self.prefix}:{event_id}"
        exists = await self.valkey.exists(key)
        return bool(exists)

    async def mark_processed(self, event_id: str) -> None:
        key = f"{self.prefix}:{event_id}"
        # Set with TTL
        await self.valkey.set(key, "1", ex=self.ttl)
