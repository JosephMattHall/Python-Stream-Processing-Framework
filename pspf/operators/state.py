from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Generic
from pspf.utils.typing import T

class State(ABC, Generic[T]):
    """Abstract base class for operator state."""

    @abstractmethod
    def get(self) -> Optional[T]:
        """Get the current state value."""
        pass

    @abstractmethod
    def set(self, value: T) -> None:
        """Update the state value."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear the state."""
        pass


class ValueState(State[T]):
    """Simple in-memory value state."""

    def __init__(self, default: Optional[T] = None):
        self._value: Optional[T] = default
        self._default = default

    def get(self) -> Optional[T]:
        return self._value

    def set(self, value: T) -> None:
        self._value = value

    def clear(self) -> None:
        self._value = self._default

    def snapshot(self) -> Any:
        return self._value

    def restore(self, state: Any) -> None:
        self._value = state


class KeyedState(Generic[T]):
    """Manages state per key."""

    def __init__(self) -> None:
        self._store: Dict[Any, T] = {}

    async def get(self, key: Any) -> Optional[T]:
        """Retrieve state for a given key."""
        return self._store.get(key)

    async def set(self, key: Any, value: T) -> None:
        """Set state for a given key."""
        self._store[key] = value

    async def clear(self, key: Any) -> None:
        """Clear state for a given key."""
        if key in self._store:
            del self._store[key]

    def snapshot(self) -> Dict[Any, T]:
        """Capture a snapshot of the current keyed state."""
        return self._store.copy()

    def restore(self, state: Dict[Any, T]) -> None:
        """Restore keyed state from a snapshot."""
        self._store = state.copy()


class ValkeyKeyedState(KeyedState[T]):
    """
    Keyed state backed by Valkey.
    Enables state to exceed worker memory and persists across restarts.
    """
    def __init__(self, prefix: str, host: str = "localhost", port: int = 6379):
        import valkey.asyncio as valkey
        import msgpack
        self.client = valkey.Valkey(host=host, port=port)
        self.prefix = f"pspf:state:{prefix}"
        self.msgpack = msgpack

    async def get(self, key: Any) -> Optional[T]:
        val = await self.client.hget(self.prefix, str(key))
        if val:
            return self.msgpack.unpackb(val)
        return None

    async def set(self, key: Any, value: T) -> None:
        data = self.msgpack.packb(value)
        await self.client.hset(self.prefix, str(key), data)

    async def clear(self, key: Any) -> None:
        await self.client.hdel(self.prefix, str(key))

    def snapshot(self) -> Dict[Any, T]:
        # Valkey state is already durable, so snapshot is a no-op/different concern
        return {}

    def restore(self, state: Dict[Any, T]) -> None:
        pass
