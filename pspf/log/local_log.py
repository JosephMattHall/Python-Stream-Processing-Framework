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
    
    def __init__(self, data_dir: str, num_partitions: int = 4, max_segment_size: int = 100 * 1024 * 1024):
        self._data_dir = Path(data_dir)
        self._num_partitions = num_partitions
        self._max_segment_size = max_segment_size
        self._locks = [asyncio.Lock() for _ in range(num_partitions)]
        
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

    async def get_high_watermark(self, partition: int) -> int:
        """Returns the current max offset + 1 for the partition."""
        return await self._get_next_offset(partition)

    def _get_segments(self, partition: int) -> List[Path]:
        """Returns sorted list of segments for a partition, oldest first."""
        segments = list(self._data_dir.glob(f"partition_{partition}.*.bin"))
        segments.sort() # lexicographical sort by timestamp suffix
        active = self._partition_file(partition)
        if active.exists():
            segments.append(active)
        return segments

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
                 
             # Rotation check
             p_file = self._partition_file(partition)
             if p_file.stat().st_size >= self._max_segment_size:
                 timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
                 archive_path = self._data_dir / f"partition_{partition}.{timestamp}.bin"
                 p_file.rename(archive_path)
                 p_file.touch() # Start new active segment
             
             self._offset_cache[partition] = offset + 1

    async def read(self, partition: int, offset: int) -> AsyncIterator[StreamRecord]:
        segments = self._get_segments(partition)
        current_idx = 0
        
        for p_file in segments:
            async with aiofiles.open(p_file, mode='rb') as f:
                while True:
                    header = await f.read(4)
                    if not header or len(header) < 4:
                        break
                    
                    length = struct.unpack(">I", header)[0]
                    payload = await f.read(length)
                    
                    if len(payload) < length:
                        break
                    
                    if current_idx >= offset:
                        try:
                            data = msgpack.unpackb(payload)
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
                            print(f"Read error in {p_file}: {e}")
                    
                    current_idx += 1

    async def cleanup(self, retention_days: int) -> None:
        """Deletes archived segments older than retention_days."""
        import time
        now = time.time()
        cutoff = now - (retention_days * 86400)
        
        for p in range(self._num_partitions):
            # Only cleanup archived segments, never the active partition_{p}.bin
            archived = self._data_dir.glob(f"partition_{p}.*.bin")
            for segment in archived:
                if segment.stat().st_mtime < cutoff:
                    self.logger.info(f"Deleting old segment {segment.name}")
                    segment.unlink()
