import asyncio
import msgpack
import struct
import os
import aiofiles
from datetime import datetime
from typing import AsyncIterator, List, Any
from pathlib import Path

from pspf.models import StreamRecord
from pspf.log.interfaces import Log

class LocalLog(Log):
    """
    Native file-based implementation of the Log interface.
    
    Features:
    - Partitioning by hash(key)
    - Append-only log files per partition
    - Binary MessagePack format for performance (Length-Prefixed Framing)
    - Async I/O for high concurrency
    """
    
    def __init__(self, data_dir: str, num_partitions: int = 4):
        self._data_dir = Path(data_dir)
        self._num_partitions = num_partitions
        self._locks = [asyncio.Lock() for _ in range(num_partitions)]
        
        # Ensure data directory exists
        self._data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize partition files if they don't exist
        for p in range(num_partitions):
            p_file = self._partition_file(p)
            if not p_file.exists():
                p_file.touch()

    def partitions(self) -> int:
        return self._num_partitions
        
    def _partition_file(self, partition: int) -> Path:
        return self._data_dir / f"partition_{partition}.bin"

    def _get_partition(self, key: str) -> int:
        return hash(key) % self._num_partitions

    async def _get_next_offset(self, partition: int) -> int:
        if not hasattr(self, '_offset_cache'):
            self._offset_cache = {}
            
        if partition not in self._offset_cache:
            # count frames
            count = 0
            p_file = self._partition_file(partition)
            if p_file.exists():
                async with aiofiles.open(p_file, mode='rb') as f:
                    while True:
                        # Read length header (4 bytes)
                        header = await f.read(4)
                        if not header or len(header) < 4:
                            break
                        length = struct.unpack(">I", header)[0]
                        # Skip payload
                        await f.seek(length, 1)
                        count += 1
            self._offset_cache[partition] = count
            
        return self._offset_cache[partition]

    async def append(self, record: StreamRecord) -> None:
        partition = self._get_partition(record.key)
        record.partition = partition
        
        async with self._locks[partition]:
             offset = await self._get_next_offset(partition)
             record.offset = offset
             
             data = {
                "id": record.id,
                "key": record.key,
                "value": record.value,
                "event_type": getattr(record, "event_type", ""),
                "timestamp": record.timestamp.isoformat(),
                "partition": partition,
                "offset": offset
             }
             
             payload = msgpack.packb(data)
             length = len(payload)
             # Frame: [4 byte len][payload]
             frame = struct.pack(">I", length) + payload
             
             async with aiofiles.open(self._partition_file(partition), mode='ab') as f:
                 await f.write(frame)
             
             # update cache
             self._offset_cache[partition] = offset + 1

    async def read(self, partition: int, offset: int) -> AsyncIterator[StreamRecord]:
        p_file = self._partition_file(partition)
        if not p_file.exists():
            return

        async with aiofiles.open(p_file, mode='rb') as f:
            current_idx = 0
            while True:
                header = await f.read(4)
                if not header or len(header) < 4:
                    break
                
                length = struct.unpack(">I", header)[0]
                payload = await f.read(length)
                
                if len(payload) < length:
                    # Partial write or corruption
                    break
                
                if current_idx >= offset:
                    try:
                        data = msgpack.unpackb(payload)
                        # Note: msgpack normally handles bytes->str decoding automatically
                        # but we should be aware that older versions or config might return bytes.
                        yield StreamRecord(
                            id=data["id"],
                            key=data["key"],
                            value=data["value"],
                            event_type=data.get("event_type", ""),
                            timestamp=datetime.fromisoformat(data["timestamp"]),
                            partition=data["partition"],
                            offset=data["offset"]
                        )
                    except Exception as e:
                        print(f"Read error: {e}")
                
                current_idx += 1
