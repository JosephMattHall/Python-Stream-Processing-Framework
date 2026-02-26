import pytest
from unittest.mock import MagicMock, AsyncMock
from pspf.processor import BatchProcessor
from pspf.state.backends.memory_store import InMemoryStateStore
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.state.backends.rocksdb_store import RocksDBStateStore
from pspf.connectors.memory import MemoryBackend
import os

@pytest.mark.asyncio
async def test_processor_with_memory_state():
    """Verify BatchProcessor works with InMemoryStateStore."""
    backend = MemoryBackend("test_stream", "group1")
    state = InMemoryStateStore()
    processor = BatchProcessor(backend, state_store=state)
    
    async def handler(msg_id, data, ctx):
        await ctx.state.put("processed", msg_id)
        
    # Mock some data
    await backend.add_event({"hello": "world"})
    
    # Run one cycle manually (since run_loop is infinite)
    messages = await backend.read_batch(count=1)
    for msg_id, data in messages:
        await processor._process_single_message(handler, msg_id, data, "test_stream")
        
    assert await state.get("processed") is not None

@pytest.mark.asyncio
async def test_processor_with_sqlite_state(tmp_path):
    """Verify BatchProcessor works with SQLiteStateStore."""
    db_path = tmp_path / "test.db"
    backend = MemoryBackend("test_stream", "group1")
    state = SQLiteStateStore(str(db_path))
    processor = BatchProcessor(backend, state_store=state)
    
    async def handler(msg_id, data, ctx):
        await ctx.state.put("key", "value")
        await ctx.state.checkpoint("s", "g", msg_id)

    await backend.add_event({"x": 1})
    await state.start()
    
    messages = await backend.read_batch(count=1)
    for msg_id, data in messages:
        await processor._process_single_message(handler, msg_id, data, "test_stream")
    
    val = await state.get("key")
    assert val == "value"
    cp = await state.get_checkpoint("s", "g")
    assert cp is not None
    
    await state.stop()

@pytest.mark.asyncio
async def test_processor_with_mocked_rocksdb():
    """
    Verify BatchProcessor can interact with RocksDBStateStore interface,
    even if the underlying library is missing (using Mocks).
    """
    backend = MemoryBackend("test_stream", "group1")
    # Mock the RocksDB store
    state = MagicMock(spec=RocksDBStateStore)
    state.start = AsyncMock()
    state.stop = AsyncMock()
    state.put = AsyncMock()
    state.get = AsyncMock(return_value="mocked")
    state.checkpoint = AsyncMock()
    
    processor = BatchProcessor(backend, state_store=state)
    
    async def handler(msg_id, data, ctx):
        await ctx.state.put("foo", "bar")
        
    await backend.add_event({"event": 1})
    
    messages = await backend.read_batch(count=1)
    for msg_id, data in messages:
        await processor._process_single_message(handler, msg_id, data, "test_stream")
        
    state.put.assert_called()

def test_state_store_interface_polymorphism():
    """Ensure all backends implement the required methods."""
    from pspf.state.store import StateStore
    
    # We can check subclasses
    subclasses = StateStore.__subclasses__()
    names = [s.__name__ for s in subclasses]
    assert "SQLiteStateStore" in names
    assert "InMemoryStateStore" in names
    assert "RocksDBStateStore" in names
