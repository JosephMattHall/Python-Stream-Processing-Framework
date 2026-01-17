from abc import ABC, abstractmethod
import asyncio
from typing import Set

class DeduplicationStore(ABC):
    """
    Interface for storing processed event IDs to ensure exactly-once processing.
    """
    
    @abstractmethod
    async def has_processed(self, event_id: str) -> bool:
        """Check if an event ID has already been processed."""
        pass

    @abstractmethod
    async def mark_processed(self, event_id: str) -> None:
        """Mark an event ID as processed."""
        pass

class MemoryDeduplicationStore(DeduplicationStore):
    """
    In-memory implementation of DeduplicationStore.
    """
    def __init__(self):
        self._seen: Set[str] = set()
        self._lock = asyncio.Lock()

    async def has_processed(self, event_id: str) -> bool:
        async with self._lock:
            return event_id in self._seen

    async def mark_processed(self, event_id: str) -> None:
        async with self._lock:
            self._seen.add(event_id)
