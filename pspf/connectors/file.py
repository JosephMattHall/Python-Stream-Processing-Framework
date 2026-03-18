import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Tuple
from pspf.connectors.base import StreamingBackend
from pspf.utils.logging import get_logger

logger = get_logger("FileBackend")

class FileStreamBackend(StreamingBackend):
    """
    Very simple file-based backend that reads/writes line-delimited JSON.
    Useful for testing or local-only processing.
    """
    def __init__(self, path: str, stream_key: str = "default", group_name: str = "default", consumer_name: str = "worker-1"):
        self.path = path
        self._stream_key = stream_key
        self._group_name = group_name
        self.consumer_name = consumer_name
        self._current_offset = 0

    @property
    def stream_key(self) -> str:
        return self._stream_key

    @property
    def group_name(self) -> str:
        return self._group_name

    def clone_with_topic(self, topic: str) -> "FileStreamBackend":
        return FileStreamBackend(
            path=self.path,
            stream_key=topic,
            group_name=self.group_name,
            consumer_name=self.consumer_name
        )

    async def connect(self) -> None:
        # Ensure file exists
        if not os.path.exists(self.path):
            with open(self.path, 'w') as f:
                pass
        logger.info(f"Connected to FileBackend at {self.path}")

    async def close(self) -> None:
        pass

    async def ping(self) -> bool:
        return os.path.exists(self.path)

    async def ensure_group_exists(self, start_id: str = "0") -> None:
        pass

    async def read_batch(self, count: int = 10, block_ms: int = 1000) -> List[Tuple[str, Dict[str, Any]]]:
        """Reads a batch of lines from the file."""
        messages: List[Tuple[str, Dict[str, Any]]] = []
        try:
            with open(self.path, 'r') as f:
                # Naive implementation: skip to current offset
                for _ in range(self._current_offset):
                    f.readline()
                
                for _ in range(count):
                    line = f.readline()
                    if not line:
                        break
                    
                    self._current_offset += 1
                    msg_id = str(self._current_offset)
                    try:
                        data = json.loads(line.strip())
                        messages.append((msg_id, data))
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping invalid JSON line {msg_id}")
                        continue
        except Exception as e:
            logger.error(f"Error reading file {self.path}: {e}")
            
        return messages

    async def ack_batch(self, message_ids: List[str]) -> None:
        # File backend is too simple for ACK tracking right now
        pass

    async def add_event(self, data: Dict[str, Any], max_len: Optional[int] = None) -> str:
        """Appends a JSON line to the file."""
        try:
            with open(self.path, 'a') as f:
                f.write(json.dumps(data) + "\n")
            # In accurate file-log we'd return the byte offset, 
            # but for this simple backend we'll just return a success dummy.
            return "ok"
        except Exception as e:
            logger.error(f"Error writing to {self.path}: {e}")
            raise

    async def claim_stuck_messages(self, min_idle_time_ms: int = 60000, count: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
        return []

    async def increment_retry_count(self, message_id: str) -> int:
        return 0

    async def move_to_dlq(self, message_id: str, data: Dict[str, Any], error: str) -> None:
        dlq_path = f"{self.path}.dlq"
        with open(dlq_path, 'a') as f:
            data["_error"] = error
            f.write(json.dumps(data) + "\n")

    async def get_pending_info(self) -> Dict[str, Any]:
        return {"pending": 0, "lag": 0, "consumers": 1}

