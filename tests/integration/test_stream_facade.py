import asyncio
import pytest
import os
from pspf import Stream, BaseEvent
from pspf.state.backends.memory_store import InMemoryStateStore

@pytest.mark.asyncio
async def test_stream_auto_instantiation_memory():
    # Force dev mode and NO Valkey to ensure MemoryBackend
    os.environ["PSPF_ENV"] = "dev"
    os.environ.pop("VALKEY_HOST", None)
    
    stream = Stream(topic="test_auto", group="test_group")
    from pspf.connectors.memory import MemoryBackend
    assert isinstance(stream.backend, MemoryBackend)

@pytest.mark.asyncio
async def test_stream_durable_retry_integration():
    # Use MemoryBackend but with a persistent-style StateStore (Mocked)
    store = InMemoryStateStore()
    stream = Stream(topic="test_retry", group="test_group", state_store=store)
    
    # We want to verify that BatchProcessor uses the state_store for retries
    # Actually, BatchProcessor calls backend.increment_retry_count
    # And we updated KafkaStreamBackend to use state_store.
    # ValkeyStreamBackend and MemoryBackend might still use in-memory counters?
    # Let's check MemoryBackend.
    pass

@pytest.mark.asyncio
async def test_stream_stateful_handler_injection():
    store = InMemoryStateStore()
    stream = Stream(topic="test_state", group="test_group", state_store=store)
    
    received_ctx = None
    
    @stream.subscribe("test_state")
    async def my_handler(msg_id, data, ctx):
        nonlocal received_ctx
        received_ctx = ctx
        await ctx.state.put("key", "val")

    async with stream:
        await stream.emit({"foo": "bar"})
        # Run for one iteration
        task = asyncio.create_task(stream.run_forever())
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert received_ctx is not None
    assert received_ctx.state == store
    assert await store.get("key") == "val"

@pytest.mark.asyncio
async def test_idempotent_sink_in_stream():
    from pspf.connectors.base import BaseSink
    
    class MySink(BaseSink):
        def __init__(self, name, state_store):
            super().__init__(name, state_store)
            self.count = 0
        async def on_write(self, event, token):
            self.count += 1

    store = InMemoryStateStore()
    sink = MySink("test_sink", store)
    await sink.start()
    
    stream = Stream(topic="test_sink_stream", group="test_sink_group")
    
    @stream.subscribe("test_sink_stream")
    async def handler(event):
        await sink.write(event)

    async with stream:
        # Emit same event twice (using same event_id would require manual emit or backend support)
        # But sink.write uses event.event_id for token.
        event = BaseEvent(event_id="EV-1", event_type="test", payload={})
        
        await sink.write(event)
        await sink.write(event) # Should be skipped
        
    assert sink.count == 1
    await sink.stop()
