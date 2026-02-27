from typing import Any, Optional, Dict
from pspf.state.store import StateStore

class InMemoryStateStore(StateStore):
    """
    Volatile in-memory state store. 
    State is NOT persisted between restarts.
    Useful for testing or temporary caches.
    """
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._checkpoints: Dict[str, str] = {}

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self._data.clear()

    async def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    async def put(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def put_batch(self, entries: Dict[str, Any]) -> None:
        self._data.update(entries)

    async def delete(self, key: str) -> None:
        if key in self._data:
            del self._data[key]

    async def flush(self) -> None:
        pass

    async def checkpoint(self, stream_id: str, group_id: str, offset: str) -> None:
        self._checkpoints[f"{stream_id}:{group_id}"] = offset

    async def get_checkpoint(self, stream_id: str, group_id: str) -> Optional[str]:
        return self._checkpoints.get(f"{stream_id}:{group_id}")
