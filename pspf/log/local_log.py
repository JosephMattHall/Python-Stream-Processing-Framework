import asyncio
import msgpack
import struct
import os
import zlib
import aiofiles
from datetime import datetime
from typing import AsyncIterator, List, Any, Dict, Optional, Tuple
from pathlib import Path

from pspf.models import StreamRecord
from pspf.log.interfaces import Log
from pspf.utils.logging import get_logger

logger = get_logger("LocalLog")

class LocalLog(Log):
    """
    Native file-based implementation of the Log interface.
    
    Features:
    - Partitioning by hash(key)
    - Append-only log files per partition
    - Binary MessagePack format with CRC32 Checksums
    - Startup Recovery & Safe Truncation
    
    Format:
    [Length (4B Big Endian)][CRC32 (4B Big Endian)][Payload (MsgPack)]
    """
    
    def __init__(self, data_dir: str, num_partitions: int = 4, max_segment_size: int = 100 * 1024 * 1024):
        self._data_dir = Path(data_dir)
        self._num_partitions = num_partitions
        self._max_segment_size = max_segment_size
        self._locks = [asyncio.Lock() for _ in range(num_partitions)]
        # Cache for next assignable offset per partition
        self._next_offsets: Dict[int, int] = {} 
        
        self._data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize
        for p in range(num_partitions):
            self._recover_partition_sync(p)

    def partitions(self) -> int:
        return self._num_partitions

    def _get_partition(self, key: str) -> int:
        return hash(key) % self._num_partitions

    def _get_segment_path(self, partition: int, start_offset: int) -> Path:
        return self._data_dir / f"partition_{partition}_{start_offset}.bin"

    def _list_segments(self, partition: int) -> List[Tuple[int, Path]]:
        """
        Returns sorted list of (start_offset, path) for a partition.
        """
        segments = []
        prefix = f"partition_{partition}_"
        for p in self._data_dir.glob(f"{prefix}*.bin"):
            try:
                # Filename format: partition_{p}_{start_offset}.bin
                # Extract start_offset
                parts = p.stem.split('_')
                if len(parts) >= 3:
                    start_offset = int(parts[2])
                    segments.append((start_offset, p))
            except ValueError:
                logger.warning(f"Ignoring invalid segment file: {p.name}")
                continue
        
        segments.sort(key=lambda x: x[0])
        return segments

    def _recover_partition_sync(self, partition: int) -> None:
        """
        Synchronous startup recovery.
        Scans segments to find the high water mark and truncates corrupt tail.
        """
        segments = self._list_segments(partition)
        
        # If no segments, initialize clean state
        if not segments:
            # Create first segment starting at 0
            first_seg = self._get_segment_path(partition, 0)
            first_seg.touch()
            self._next_offsets[partition] = 0
            return

        # We assume archived segments (all but last) are immutable and valid (simplification).
        # We only strictly Validate/Recover the LAST segment (active).
        last_start_offset, last_path = segments[-1]
        
        valid_records_in_last = 0
        
        # Scan the last segment to find end and validate
        with open(last_path, 'r+b') as f:
            while True:
                pos = f.tell()
                header = f.read(8) # 4 bytes len + 4 bytes crc
                if not header or len(header) < 8:
                    if len(header) > 0:
                        logger.warning(f"Truncating partial header at end of {last_path.name} (offset {pos})")
                        f.seek(pos)
                        f.truncate()
                    break
                
                length = struct.unpack(">I", header[0:4])[0]
                stored_crc = struct.unpack(">I", header[4:8])[0]
                
                payload = f.read(length)
                if len(payload) < length:
                    logger.warning(f"Truncating partial payload at end of {last_path.name} (offset {pos})")
                    f.seek(pos)
                    f.truncate()
                    break
                
                # Checksum verify
                computed_crc = zlib.crc32(payload) & 0xffffffff
                if computed_crc != stored_crc:
                    logger.error(f"CRC Mismatch at offset {pos} in {last_path.name}. Truncating.")
                    f.seek(pos)
                    f.truncate()
                    break
                
                valid_records_in_last += 1

        self._next_offsets[partition] = last_start_offset + valid_records_in_last
        logger.info(f"Partition {partition} recovered. Next Offset: {self._next_offsets[partition]}")

    async def _get_active_segment_path(self, partition: int) -> Path:
        """
        Returns the path of the current active segment for writing.
        """
        segments = self._list_segments(partition)
        if segments:
            return segments[-1][1]
        # Should be initialized in recovery, but safe fallback:
        return self._get_segment_path(partition, 0)

    async def get_high_watermark(self, partition: int) -> int:
        return self._next_offsets.get(partition, 0)

    async def append(self, record: StreamRecord) -> None:
        partition = self._get_partition(record.key)
        record.partition = partition
        
        async with self._locks[partition]:
            active_path = await self._get_active_segment_path(partition)
            
            # Check for Rotation BEFORE writing
            # If current file is too big, start a new one
            if active_path.exists() and active_path.stat().st_size >= self._max_segment_size:
                 next_offset = self._next_offsets[partition]
                 new_path = self._get_segment_path(partition, next_offset)
                 new_path.touch()
                 active_path = new_path
            
            offset = self._next_offsets[partition]
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
            crc = zlib.crc32(payload) & 0xffffffff
            
            # Frame: [4 byte len][4 byte crc][payload]
            frame = struct.pack(">II", length, crc) + payload
            
            async with aiofiles.open(active_path, mode='ab') as f:
                await f.write(frame)
            
            self._next_offsets[partition] += 1

    async def read(self, partition: int, offset: int) -> AsyncIterator[StreamRecord]:
        segments = self._list_segments(partition)
        
        for start_offset, path in segments:
            # Skip segments that end before requested offset
            # We don't know exact end offset without opening, but we know the NEXT segment's start.
            # So if requested offset >= next_segment_start, we can skip this one.
            # Finding the index of this segment in the list:
            idx = segments.index((start_offset, path))
            next_start = segments[idx+1][0] if idx + 1 < len(segments) else float('inf')
            
            if offset >= next_start:
                continue
            
            # This segment might contain our offset.
            # Local offset within this segment is tricky because records are variable length.
            # We have to scan from start of the segment if it's the target segment.
            # Optimization: If we are reading sequentially, we scan. 
            # Ideally we'd have an index, but for now we scan.
            
            current_log_offset = start_offset
            
            async with aiofiles.open(path, mode='rb') as f:
                while True:
                    header = await f.read(8)
                    if not header or len(header) < 8:
                        break
                    
                    length = struct.unpack(">I", header[0:4])[0]
                    stored_crc = struct.unpack(">I", header[4:8])[0]
                    
                    payload = await f.read(length)
                    if len(payload) < length:
                        break # Truncated
                    
                    # Only calculate/deserialize if we are at or past the requested offset
                    if current_log_offset >= offset:
                         computed_crc = zlib.crc32(payload) & 0xffffffff
                         if computed_crc != stored_crc:
                             logger.error(f"CRC Mismatch reading {path} at offset {current_log_offset}")
                             # Stop reading this segment? Or skip? 
                             # For safety, we stop.
                             break
                             
                         try:
                             data = msgpack.unpackb(payload)
                             yield StreamRecord(
                                 id=data.get("id", ""),
                                 key=data.get("key", ""),
                                 value=data.get("value"),
                                 event_type=data.get("event_type", ""),
                                 timestamp=datetime.fromisoformat(data["timestamp"]),
                                 partition=data.get("partition", partition),
                                 offset=data.get("offset", current_log_offset)
                             )
                         except Exception as e:
                             logger.error(f"Deserialization error: {e}")
                             break

                    current_log_offset += 1

    async def cleanup(self, retention_days: int) -> None:
        """Deletes old segments."""
        import time
        now = time.time()
        cutoff = now - (retention_days * 86400)
        
        for p in range(self._num_partitions):
            segments = self._list_segments(p)
            # Never delete the last (active) segment
            for start_offset, path in segments[:-1]:
                if path.stat().st_mtime < cutoff:
                    logger.info(f"Deleting old segment {path.name}")
                    path.unlink()
