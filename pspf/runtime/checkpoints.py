import json
import os
import shutil
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class CheckpointBackend(ABC):
    """Abstract base class for checkpoint storage backends."""

    @abstractmethod
    async def save(self, checkpoint_id: str, state: Dict[str, Any]) -> None:
        """Save state to the backing store."""
        pass

    @abstractmethod
    async def load(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """Load state from the backing store."""
        pass


class FileCheckpointBackend(CheckpointBackend):
    """Durable file-based checkpoint backend."""

    def __init__(self, directory: str = ".pspf_checkpoints"):
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)

    def _get_path(self, checkpoint_id: str) -> str:
        return os.path.join(self.directory, f"{checkpoint_id}.json")

    async def save(self, checkpoint_id: str, state: Dict[str, Any]) -> None:
        path = self._get_path(checkpoint_id)
        temp_path = f"{path}.tmp"
        
        # In a real system we'd use aiofiles, but following constraints for standard lib
        with open(temp_path, 'w') as f:
            json.dump(state, f)
        
        # Atomic rename
        shutil.move(temp_path, path)

    async def load(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        path = self._get_path(checkpoint_id)
        if not os.path.exists(path):
            return None
        
        with open(path, 'r') as f:
            return json.load(f)


class InMemoryCheckpoint(CheckpointBackend):
    """Memory-only checkpoint backend (for testing/development)."""

    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}

    async def save(self, checkpoint_id: str, state: Dict[str, Any]) -> None:
        self._store[checkpoint_id] = state.copy()

    async def load(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        return self._store.get(checkpoint_id)



class CheckpointManager:
    """Orchestrates checkpointing logic."""

    def __init__(self, backend: CheckpointBackend):
        self._backend = backend

    async def trigger_checkpoint(self, checkpoint_id: str, state_snapshot: Dict[str, Any]) -> None:
        """Take a snapshot of the current state."""
        # TODO: Add logic to ensure consistency across operators
        await self._backend.save(checkpoint_id, state_snapshot)

    async def restore_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Restore state from a checkpoint."""
        return await self._backend.load(checkpoint_id)
