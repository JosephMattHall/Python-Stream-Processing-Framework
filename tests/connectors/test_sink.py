import asyncio
import pytest
from pspf.schema import BaseEvent
from pspf.connectors.base import BaseSink
from pspf.state.backends.memory_store import InMemoryStateStore

class MockApiSink(BaseSink):
    def __init__(self, name, state_store):
        super().__init__(name, state_store)
        self.call_count = 0
        self.last_token = None

    async def on_write(self, event, idempotency_token):
        self.call_count += 1
        self.last_token = idempotency_token
        # Simulate API call
        await asyncio.sleep(0.01)

@pytest.mark.asyncio
async def test_base_sink_idempotency():
    state_store = InMemoryStateStore()
    sink = MockApiSink("test_api", state_store)
    
    event = BaseEvent(event_type="TestEvent", payload={"data": "foo"})
    
    # 1. First write
    await sink.write(event)
    assert sink.call_count == 1
    token = sink.last_token
    assert token.startswith("pspf:sink:test_api:")
    
    # 2. Duplicate write (simulated retry)
    await sink.write(event)
    assert sink.call_count == 1 # Should NOT increment
    
    # 3. Different event
    event2 = BaseEvent(event_type="TestEvent", payload={"data": "bar"})
    await sink.write(event2)
    assert sink.call_count == 2
    assert sink.last_token != token

if __name__ == "__main__":
    # Minimal runner if not using pytest
    async def run_manual():
        await test_base_sink_idempotency()
        print("✅ Idempotent BaseSink Verification PASSED")
    
    asyncio.run(run_manual())
