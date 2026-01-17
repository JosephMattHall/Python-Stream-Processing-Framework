import asyncio
from typing import Dict
from pspf.log.interfaces import OffsetStore

class MemoryOffsetStore(OffsetStore):
    """
    In-memory implementation of OffsetStore.
    Useful for testing and single-instance deployments.
    """
    def __init__(self):
        # Mapping: consumer_id -> partition -> offset
        self._offsets: Dict[str, Dict[int, int]] = {}
        self._lock = asyncio.Lock()

    async def get(self, consumer_id: str, partition: int) -> int:
        async with self._lock:
            if consumer_id not in self._offsets:
                return 0
            return self._offsets[consumer_id].get(partition, 0)

    async def commit(self, consumer_id: str, partition: int, offset: int) -> None:
        async with self._lock:
            if consumer_id not in self._offsets:
                self._offsets[consumer_id] = {}
            # Monotonic increase check could be added here, 
            # but we allow rewinds (setting lower offset) if needed for replay.
            self._offsets[consumer_id][partition] = offset
