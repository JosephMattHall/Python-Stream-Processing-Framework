import pytest
import pytest_asyncio
from pspf.processor import BatchProcessor
from pspf.connectors.memory import MemoryBackend
from pspf.state.backends.memory_store import InMemoryStateStore
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.state.backends.rocksdb_store import RocksDBStateStore
from pspf.context import Context

# Check for RocksDB
try:
    import rocksdb # type: ignore
    HAS_ROCKSDB = hasattr(rocksdb, "DB")
except ImportError:
    HAS_ROCKSDB = False

@pytest_asyncio.fixture
async def sqlite_store(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "test.db"))
    await store.start()
    yield store
    await store.stop()

@pytest_asyncio.fixture
async def memory_store():
    store = InMemoryStateStore()
    await store.start()
    yield store
    await store.stop()

@pytest_asyncio.fixture
async def rocksdb_store(tmp_path):
    if not HAS_ROCKSDB:
        pytest.skip("RocksDB not installed")
    store = RocksDBStateStore(str(tmp_path / "rocks_test"))
    await store.start()
    yield store
    await store.stop()

@pytest_asyncio.fixture
async def valkey_backend():
    from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
    connector = ValkeyConnector(host="localhost", port=6379)
    try:
        await connector.connect()
    except Exception:
        pytest.skip("Valkey not available")
    
    backend = ValkeyStreamBackend(connector, "matrix-stream", "matrix-group", "matrix-consumer")
    await backend.ensure_group_exists()
    yield backend
    # Cleanup
    client = connector.get_client()
    await client.delete("matrix-stream")
    await connector.close()

async def run_test_combination(backend, state_store):
    """Generic test to verify backend + state store combination."""
    if state_store:
        await state_store.start()
        
    processor = BatchProcessor(backend, state_store=state_store)
    
    processed_events = []
    
    async def handler(msg_id, data, ctx: Context):
        # 1. Update State
        if ctx.state:
            count = await ctx.state.get("count", 0)
            await ctx.state.put("count", count + 1)
            # 2. Checkpoint
            await ctx.state.checkpoint(backend.stream_key, backend.group_name, msg_id)
        
        processed_events.append(data)

    # Emit test data
    await backend.add_event({"val": 1})
    await backend.add_event({"val": 2})
    
    # Process one batch
    messages = await backend.read_batch(count=10)
    for msg_id, data in messages:
        await processor._process_single_message(handler, msg_id, data, backend.stream_key)
    
    # Verify processing
    assert len(processed_events) == 2
    
    # Verify state persistence/checkpoints
    if state_store:
        assert await state_store.get("count") == 2
        cp = await state_store.get_checkpoint(backend.stream_key, backend.group_name)
        assert cp is not None
        await state_store.stop()

@pytest.mark.asyncio
async def test_memory_x_memory(memory_store):
    backend = MemoryBackend("s1", "g1")
    await run_test_combination(backend, memory_store)

@pytest.mark.asyncio
async def test_memory_x_sqlite(sqlite_store):
    backend = MemoryBackend("s1", "g1")
    await run_test_combination(backend, sqlite_store)

@pytest.mark.asyncio
async def test_memory_x_rocksdb(rocksdb_store):
    backend = MemoryBackend("s1", "g1")
    await run_test_combination(backend, rocksdb_store)

@pytest.mark.asyncio
async def test_valkey_x_sqlite(valkey_backend, sqlite_store):
    await run_test_combination(valkey_backend, sqlite_store)

@pytest.mark.asyncio
async def test_valkey_x_memory(valkey_backend, memory_store):
    await run_test_combination(valkey_backend, memory_store)

# More combinations can be added here for Valkey if a live instance is assumed
# or for FileBackend.
