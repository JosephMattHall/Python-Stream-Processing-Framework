from abc import ABC, abstractmethod
from typing import Any, Optional, Union, Dict, AsyncIterator
from contextlib import asynccontextmanager

class StateStore(ABC):
    """
    Abstract base class for Key-Value state stores.
    """
    
    @abstractmethod
    async def start(self) -> None:
        """Initialize the store (e.g. connect to DB)."""
        pass

    @abstractmethod
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Provide a transactional context for atomic operations."""
        yield

    @abstractmethod
    async def stop(self) -> None:
        """Close the store."""
        pass

    @abstractmethod
    async def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value by key. Returns default if not found."""
        pass

    @abstractmethod
    async def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Store a value by key with optional TTL."""
        pass

    @abstractmethod
    async def put_batch(self, entries: Dict[str, Any]) -> None:
        """Store multiple values in a batch."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a key."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Force write to durable storage."""
        pass

    @abstractmethod
    async def checkpoint(self, stream_id: str, group_id: str, offset: str) -> None:
        """
        Atomically store the processing offset.
        In persistent stores, this should be in the same transaction as state updates.
        """
        pass

    @abstractmethod
    async def get_checkpoint(self, stream_id: str, group_id: str) -> Optional[str]:
        """
        Retrieve the last processed offset for a given stream and group.
        """
        pass
