import asyncio
import time
import uuid
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class PartitionLeaseManager:
    """
    Manages distributed leases for partitions using Redis.
    Ensures only one consumer processes a partition at a time.
    """
    def __init__(self, 
                 service_name: str, 
                 host: str = 'localhost', 
                 port: int = 6379, 
                 lease_ttl_s: int = 10):
        import redis.asyncio as aioredis
        self.redis = aioredis.Redis(host=host, port=port, decode_responses=True)
        self.service_name = service_name
        self.worker_id = str(uuid.uuid4())
        self.lease_ttl = lease_ttl_s
        self.prefix = f"pspf:leases:{service_name}"

    async def acquire(self, partition: int) -> bool:
        """
        Attempt to acquire the lease for a partition.
        Returns True if acquired (or already held by us).
        """
        key = f"{self.prefix}:{partition}"
        now = time.time()
        
        # 1. Check owner
        owner = await self.redis.get(key)
        if owner == self.worker_id:
            # We own it, renew
            await self.redis.expire(key, self.lease_ttl)
            return True
            
        # 2. Try to acquire (SET NX EX)
        acquired = await self.redis.set(key, self.worker_id, ex=self.lease_ttl, nx=True)
        return bool(acquired)

    async def renew(self, partition: int) -> bool:
        """
        Renew a held lease. Returns False if lost.
        """
        key = f"{self.prefix}:{partition}"
        # Lua script to check ownership and renew atomically
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = await self.redis.eval(script, 1, key, self.worker_id, self.lease_ttl)
        return bool(result)

    async def release(self, partition: int):
        key = f"{self.prefix}:{partition}"
        # Only delete if we own it
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        await self.redis.eval(script, 1, key, self.worker_id)
