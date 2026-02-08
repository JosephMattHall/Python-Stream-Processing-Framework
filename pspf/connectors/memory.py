import asyncio
import time
import json
from typing import List, Tuple, Dict, Any, Optional
from pspf.connectors.base import StreamingBackend
from pspf.utils.logging import get_logger

logger = get_logger("MemoryBackend")

class MemoryBackend(StreamingBackend):
    """
    In-Memory backend for testing/local verification.
    Not persistent across restarts.
    """
    def __init__(self, stream_key: str = "default-stream", group_name: str = "default-group"):
        self._stream_key = stream_key
        self._group_name = group_name
        self._last_ts = 0
        self._last_seq = 0
        
        # Format: stream_key -> [ {'_id': '...', 'data': ...} ]
        self._streams: Dict[str, List[Dict[str, Any]]] = {}
        
        # Format: group_name -> consumer_name -> {msg_id, ...}
        
        # Format: group_name -> consumer_name -> {msg_id, ...}
        # Simplified PEL (Pending Entries List)
        self._pel: Dict[str, Dict[str, Dict[str, Any]]] = {}
        
        # Format: group_name -> stream_key -> last_read_id
        # We need to track offsets per group
        self._offsets: Dict[str, Dict[str, str]] = {}
        
        # Retry Tracker: msg_id -> count
        self._retries: Dict[str, int] = {}
        
        self.dlq: Dict[str, List[Dict[str, Any]]] = {}
        
        self._lock = asyncio.Lock()
        self._connected = False

    @property
    def stream_key(self) -> str:
        return self._stream_key

    @property
    def group_name(self) -> str:
        return self._group_name

    async def connect(self):
        self._connected = True
        logger.info("Connected to MemoryBackend")

    async def close(self):
        self._connected = False
        logger.info("Closed MemoryBackend")

    async def ping(self):
        if not self._connected:
            raise ConnectionError("Not connected")
        return True

    async def ensure_group_exists(self, start_id: str = "0"):
        # For memory, we just init the offset structure if missing
        pass

    async def add_event(self, data: Dict[str, Any], max_len: Optional[int] = None) -> str:
        async with self._lock:
            # Generate ID: timestamp-sequence
            ts = int(time.time() * 1000)
            if ts <= self._last_ts:
                # Same ms, increment sequence
                self._last_seq += 1
                # If we drifted too far behind clock? No, just sequence matters.
                # Actually, if we are in same MS, we use same TS and increment seq.
                ts = self._last_ts
            else:
                self._last_ts = ts
                self._last_seq = 0
            
            msg_id = f"{ts}-{self._last_seq}"
            
            # Store internal structure
            msg = {"_id": msg_id, **data}
            
            if self.stream_key not in self._streams:
                self._streams[self.stream_key] = []
                
            self._streams[self.stream_key].append(msg)
            return msg_id

    async def read_batch(self, count: int = 10, block_ms: int = 1000) -> List[Tuple[str, Dict[str, Any]]]:
        # Basic implementation: read from last offset
        async with self._lock:
            stream = self._streams.get(self.stream_key, [])
            
            # Return last 'count' messages for now to simulate flow
            
            current_offset = self._offsets.get(self.group_name, {}).get(self.stream_key, "0-0")
            
            # Filter messages > current_offset
            new_msgs = []
            for m in stream:
                if m["_id"] > current_offset:
                    new_msgs.append(m)
            
            batch = new_msgs[:count]
            
            if batch:
                # Update offset to the last one
                last_id = batch[-1]["_id"]
                if self.group_name not in self._offsets:
                    self._offsets[self.group_name] = {}
                self._offsets[self.group_name][self.stream_key] = last_id
                
            # Allow clean tuple return
            return [(m["_id"], {k:v for k,v in m.items() if k != "_id"}) for m in batch]

    async def ack_batch(self, message_ids: List[str]):
        # Remove from PEL, potentially
        pass

    async def claim_stuck_messages(self, min_idle_time_ms: int = 60000, count: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
        return []

    async def increment_retry_count(self, message_id: str) -> int:
        self._retries[message_id] = self._retries.get(message_id, 0) + 1
        return self._retries[message_id]

    async def move_to_dlq(self, message_id: str, data: Dict[str, Any], error: str):
        if self.stream_key not in self.dlq:
            self.dlq[self.stream_key] = []
        self.dlq[self.stream_key].append({"id": message_id, "data": data, "error": error})

    async def get_pending_info(self) -> Dict[str, Any]:
        """
        Returns dummy pending info for memory backend.
        Calculation of real lag in memory backend is possible but skipped for brevity.
        """
        # Calculate lag: total messages - processed
        total_msgs = len(self._streams.get(self.stream_key, []))
        # This is a rough approximation
        return {
            "pending": 0,
            "lag": total_msgs, 
            "consumers": 1
        }
