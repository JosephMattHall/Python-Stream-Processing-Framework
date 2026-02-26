import pytest
import shutil
import os
from pspf.state.backends.rocksdb_store import RocksDBStateStore

# Check if rocksdb is installed
try:
    import rocksdb
    HAS_ROCKSDB = True
except ImportError:
    HAS_ROCKSDB = False

@pytest.mark.skipif(not HAS_ROCKSDB, reason="rocksdb-python not installed")
@pytest.mark.asyncio
async def test_rocksdb_basic_ops(tmp_path):
    db_path = str(tmp_path / "rocksdb_test")
    store = RocksDBStateStore(db_path)
    await store.start()
    
    # Put/Get
    await store.put("key1", {"name": "test"})
    val = await store.get("key1")
    assert val == {"name": "test"}
    
    # Delete
    await store.delete("key1")
    val = await store.get("key1")
    assert val is None
    
    await store.stop()

@pytest.mark.skipif(not HAS_ROCKSDB, reason="rocksdb-python not installed")
@pytest.mark.asyncio
async def test_rocksdb_batch(tmp_path):
    db_path = str(tmp_path / "rocksdb_batch")
    store = RocksDBStateStore(db_path)
    await store.start()
    
    await store.put_batch({
        "k1": 1,
        "k2": 2
    })
    
    assert await store.get("k1") == 1
    assert await store.get("k2") == 2
    
    await store.stop()

@pytest.mark.skipif(not HAS_ROCKSDB, reason="rocksdb-python not installed")
@pytest.mark.asyncio
async def test_rocksdb_checkpoint(tmp_path):
    db_path = str(tmp_path / "rocksdb_checkpoint")
    store = RocksDBStateStore(db_path)
    await store.start()
    
    await store.checkpoint("stream1", "group1", "123-0")
    
    cp = await store.get_checkpoint("stream1", "group1")
    assert cp == "123-0"
    
    # Different group
    assert await store.get_checkpoint("stream1", "group2") is None
    
    await store.stop()
