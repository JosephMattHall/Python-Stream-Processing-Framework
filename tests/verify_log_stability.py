import asyncio
import shutil
import struct
from pathlib import Path
from datetime import datetime
from pspf.log.local_log import LocalLog
from pspf.models import StreamRecord

DATA_DIR = "./data/test_stability"

async def setup():
    if Path(DATA_DIR).exists():
        shutil.rmtree(DATA_DIR)
    Path(DATA_DIR).mkdir(parents=True)

async def test_normal_write():
    print("--- Test 1: Normal Write & Read ---")
    log = LocalLog(DATA_DIR, num_partitions=1)
    
    # Write 3 records
    for i in range(3):
        record = StreamRecord(
            id=f"msg-{i}", key="key-1", value={"count": i}, 
            timestamp=datetime.now(), topic="test"
        )
        await log.append(record)
    
    # Read back
    count = 0
    async for record in log.read(partition=0, offset=0):
        print(f"Read: {record.offset} -> {record.value}")
        count += 1
    
    assert count == 3, f"Expected 3 records, got {count}"
    print("✅ Normal Write/Read Passed")

async def test_corruption_recovery():
    print("\n--- Test 2: Corruption Recovery ---")
    # Corrupt the last record of partition 0
    # Find the segment
    log_dir = Path(DATA_DIR)
    segments = list(log_dir.glob("partition_0_*.bin"))
    segments.sort()
    active_seg = segments[-1]
    
    print(f"Corrupting {active_seg}...")
    
    # Read content
    with open(active_seg, "r+b") as f:
        f.seek(0, 2) # End
        size = f.tell()
        # Truncate last 2 bytes to create partial write
        f.truncate(size - 2)
    
    print("Restarting Log...")
    # Re-init log (simulates restart)
    new_log = LocalLog(DATA_DIR, num_partitions=1)
    
    # Check High Watermark (Should be 2, because 3rd record (offset 2) was corrupted/truncated)
    hw = await new_log.get_high_watermark(0)
    print(f"High Watermark after recovery: {hw}")
    
    assert hw == 2, f"Expected HW 2 (2 valid records), got {hw}"
    
    # Read back to ensure we can read the 2 valid records
    count = 0
    async for record in new_log.read(partition=0, offset=0):
        count += 1
    assert count == 2, f"Expected 2 records readable, got {count}"
    
    # Append new record
    print("Appending new record after recovery...")
    record = StreamRecord(
        id="msg-new", key="key-1", value={"count": 99}, 
        timestamp=datetime.now(), topic="test"
    )
    await new_log.append(record)
    
    new_hw = await new_log.get_high_watermark(0)
    print(f"New HW: {new_hw}")
    assert new_hw == 3
    
    print("✅ Corruption Recovery Passed")

async def main():
    await setup()
    await test_normal_write()
    await test_corruption_recovery()

if __name__ == "__main__":
    asyncio.run(main())
