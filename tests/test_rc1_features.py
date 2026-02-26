import asyncio
import pytest
import os
import struct
import zlib
from pathlib import Path
from pspf.log.local_log import LocalLog
from pspf.models import StreamRecord
from datetime import datetime
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.utils.logging import JSONFormatter
import logging
import json

@pytest.mark.asyncio
async def test_locallog_crc_corruption_recovery(tmp_path):
    """
    Verify that LocalLog correctly truncates corrupted records during recovery.
    """
    log_dir = tmp_path / "corrupt_log"
    log = LocalLog(str(log_dir), num_partitions=1)
    
    # 1. Append valid record
    rec1 = StreamRecord(id="1", key="k1", value={"data": "v1"}, timestamp=datetime.now())
    await log.append(rec1)
    
    # 2. Manually inject corrupted record into the segment file
    segments = log._list_segments(0)
    last_path = segments[-1][1]
    
    with open(last_path, "ab") as f:
        # Header: [Length 4B][CRC 4B (corrupted)]
        payload = b"corrupted payload"
        length = len(payload)
        bad_crc = 0xDEADBEEF
        f.write(struct.pack(">II", length, bad_crc) + payload)
        
    # 3. Create a new log instance to trigger recovery
    recovered_log = LocalLog(str(log_dir), num_partitions=1)
    
    # 4. Verify high watermark is back to 1 (corrupted record truncated)
    hw = await recovered_log.get_high_watermark(0)
    assert hw == 1
    
    # 5. Verify we can still read valid record
    records = []
    async for r in recovered_log.read(0, 0):
        records.append(r)
    assert len(records) == 1
    assert records[0].id == "1"

@pytest.mark.asyncio
async def test_sqlite_atomic_checkpoint(tmp_path):
    """
    Verify that SQLiteStateStore correctly stores offsets in the pspf_offsets table.
    """
    db_path = tmp_path / "test_state.db"
    store = SQLiteStateStore(str(db_path))
    await store.start()
    
    # 1. Update state
    await store.put("my_key", "my_value")
    
    # 2. Call checkpoint
    await store.checkpoint("stream1", "group1", "12345-0")
    
    # 3. Verify offset is persisted in the special table
    import aiosqlite
    async with aiosqlite.connect(str(db_path)) as db:
        async with db.execute("SELECT offset FROM pspf_offsets WHERE stream_id = ? AND group_id = ?", ("stream1", "group1")) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "12345-0"
            
    await store.stop()

def test_json_logging_format():
    """
    Verify that JSONFormatter produces valid JSON with required RC1 fields.
    """
    formatter = JSONFormatter()
    log_record = logging.LogRecord(
        name="pspf.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=None,
        exc_info=None
    )
    
    output = formatter.format(log_record)
    data = json.loads(output)
    
    assert data["message"] == "Test message"
    assert "pid" in data
    assert "timestamp" in data
    assert data["level"] == "INFO"

@pytest.mark.asyncio
async def test_locallog_rotation_enforcement(tmp_path):
    """
    Verify that LocalLog rotates segments when max_segment_size is exceeded.
    """
    # Small segment size to trigger rotation quickly
    log = LocalLog(str(tmp_path), num_partitions=1, max_segment_size=100)
    
    # Each record will be ~50-80 bytes
    for i in range(10):
        rec = StreamRecord(id=str(i), key="key", value={"x": "y"*10}, timestamp=datetime.now())
        await log.append(rec)
        
    # Check that we have multiple segments
    segments = log._list_segments(0)
    assert len(segments) > 1
