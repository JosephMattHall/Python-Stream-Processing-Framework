import pytest
import pytest_asyncio
import asyncio
from typing import Dict, Any
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.processor import BatchProcessor
from pspf.state.backends.memory_store import InMemoryStateStore

@pytest_asyncio.fixture
async def valkey_test_connector():
    connector = ValkeyConnector(host="localhost", port=6379)
    try:
        await connector.connect()
    except Exception:
        pytest.skip("Valkey not available")
    yield connector
    await connector.close()

@pytest.mark.asyncio
async def test_crash_recovery_eos(valkey_test_connector: ValkeyConnector):
    """
    Simulates a worker crash and proves Exactly-Once Semantics (EOS) 
    using XAUTOCLAIM on a new worker.
    """
    import uuid
    stream_unique_id = uuid.uuid4().hex[:8]
    stream_name = f"test-eos-crash-stream-{stream_unique_id}"

    group_name = "eos-group"

    backend1 = ValkeyStreamBackend(
        connector=valkey_test_connector,
        stream_key=stream_name,
        group_name=group_name,
        consumer_name="worker-1"
    )
    # Ensure clean start
    await valkey_test_connector.get_client().delete(stream_name)
    await backend1.ensure_group_exists()


    state = InMemoryStateStore()

    processor1 = BatchProcessor(
        backend=backend1,
        max_retries=3,
        min_idle_time_ms=100, # 100ms idle time for fast test recovery
        state_store=state,
        start_admin_server=False
    )

    # Pre-populate 10 messages
    for i in range(10):
        await backend1.add_event({"tx_id": f"tx_{i}", "val": i})

    processed_by_w1 = []
    
    # Worker 1 Handler: Process 5 messages, then "crash"
    async def crash_handler(msg_id: str, data: Dict[str, Any], ctx: Any = None):
        if len(processed_by_w1) >= 5:


            # Simulate a crash: we cancel the task abruptly
            processor1._running = False
            raise asyncio.CancelledError("Worker crashed abruptly")
        
        # State update inside transaction
        val = data.get("val", 0)
        current = await state.get("sum", 0)
        await state.put("sum", current + val)
        
        processed_by_w1.append(data["tx_id"])

    # Run Worker 1
    # It should process 5 messages then crash
    try:
         await processor1.run_loop(crash_handler, batch_size=10, poll_interval=0.1)
    except asyncio.CancelledError:
         pass

    assert len(processed_by_w1) == 5
    
    # Verify State after w1
    sum_w1 = await state.get("sum")
    assert sum_w1 == sum(range(5)) # 0+1+2+3+4 = 10

    # Wait for min_idle_time_ms to pass so messages become claimable
    await asyncio.sleep(0.2)

    # Worker 2: The Rescuer
    backend2 = ValkeyStreamBackend(
        connector=valkey_test_connector,
        stream_key=stream_name,
        group_name=group_name,
        consumer_name="worker-2"  # Different consumer
    )
    
    processed_by_w2 = []
    
    async def recover_handler(msg_id: str, data: Dict[str, Any], ctx: Any = None):
        val = data.get("val", 0)
        current = await state.get("sum", 0)
        await state.put("sum", current + val)
        processed_by_w2.append(data["tx_id"])
        
        if len(processed_by_w1) + len(processed_by_w2) == 10:
             processor2.pause()
             processor2._running = False # stop gracefully

    processor2 = BatchProcessor(
        backend=backend2,
        max_retries=3,
        min_idle_time_ms=100,
        state_store=state,
        start_admin_server=False
    )

    # Run Worker 2 (It will auto-claim on startup)
    await processor2.run_loop(recover_handler, batch_size=10, poll_interval=0.1)

    assert len(processed_by_w2) == 5
    
    # Ensure they processed disjoint sets completely
    assert set(processed_by_w1).isdisjoint(set(processed_by_w2))
    assert len(set(processed_by_w1).union(set(processed_by_w2))) == 10
    
    # Final state check: Ensure exactly-once summation
    final_sum = await state.get("sum")
    assert final_sum == sum(range(10)) # 45
    print("Crash Output Exactness Validated")
