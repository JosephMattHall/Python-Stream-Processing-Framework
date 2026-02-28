from typing import Any, Optional, Dict, AsyncIterator
from contextlib import asynccontextmanager
from pspf.state.store import StateStore

class InMemoryStateStore(StateStore):
    """
    Volatile in-memory state store. 
    State is NOT persisted between restarts.
    Useful for testing or temporary caches.
    """
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._checkpoints: Dict[str, str] = {}

    async def start(self) -> None:
        pass

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        # Memory store is volatile, no real transactions needed.
        # Could implement clones/rollbacks if strictly needed for testing.
        yield

    async def stop(self) -> None:
        pass


    async def get(self, key: str, default: Any = None) -> Any:
        import time
        if key in self._expires and time.time() > self._expires[key]:
            # Lazy eviction
            await self.delete(key)
            return default
        return self._data.get(key, default)

    async def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        import time
        self._data[key] = value
        if ttl_seconds is not None:
            self._expires[key] = time.time() + ttl_seconds
        else:
            self._expires.pop(key, None)

    async def put_batch(self, entries: Dict[str, Any]) -> None:
        self._data.update(entries)

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)
        self._expires.pop(key, None)

    async def flush(self) -> None:
        pass

    async def checkpoint(self, stream_id: str, group_id: str, offset: str) -> None:
        self._checkpoints[f"{stream_id}:{group_id}"] = offset

    async def get_checkpoint(self, stream_id: str, group_id: str) -> Optional[str]:
        return self._checkpoints.get(f"{stream_id}:{group_id}")
