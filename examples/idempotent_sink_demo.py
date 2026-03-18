import asyncio
import logging
from pspf import Stream, BaseEvent
from pspf.connectors.base import BaseSink
from pspf.state.backends.memory_store import InMemoryStateStore

# 1. Define a custom Sink by inheriting from BaseSink
class MockExternalApiSink(BaseSink):
    """
    Simulates an external API that we want to call idempotently.
    In a real scenario, this would use a library like `httpx`.
    """
    def __init__(self, name: str, state_store):
        super().__init__(name, state_store)
        self.call_count = 0

    async def on_write(self, event: BaseEvent, token: str) -> None:
        """
        This is only called if the token hasn't been processed yet.
        """
        self.call_count += 1
        print(f"DEBUG: Calling External API for event {event.event_id}")
        print(f"DEBUG: Idempotency Token: {token}")
        # Simulate API call delay
        await asyncio.sleep(0.01)

async def main():
    logging.basicConfig(level=logging.INFO)
    
    # 1. Setup Sink with StateStore
    # Sinks use a StateStore to track which tokens have already been processed.
    state_store = InMemoryStateStore()
    api_sink = MockExternalApiSink(name="ExternalApi", state_store=state_store)
    
    await api_sink.start()
    
    print("\n--- Phase 1: Normal Processing ---")
    event_1 = BaseEvent(event_id="EVT-001", event_type="OrderEvent", payload={"order_id": "ORD-1"})
    print(f"Writing event {event_1.event_id} to sink...")
    await api_sink.write(event_1)
    print(f"API Call Count: {api_sink.call_count}")

    print("\n--- Phase 2: Processing a Different Event ---")
    event_2 = BaseEvent(event_id="EVT-002", event_type="OrderEvent", payload={"order_id": "ORD-2"})
    print(f"Writing event {event_2.event_id} to sink...")
    await api_sink.write(event_2)
    print(f"API Call Count: {api_sink.call_count}")

    print("\n--- Phase 3: Simulating a Retry (Duplicate Event ID) ---")
    print(f"Writing event {event_1.event_id} AGAIN (Simulation of a retry)...")
    await api_sink.write(event_1)
    print(f"API Call Count: {api_sink.call_count}")

    if api_sink.call_count == 2:
        print("\n✅ SUCCESS: Idempotency enforced! The duplicate event was skipped.")
    else:
        print(f"\n❌ FAILURE: API was called {api_sink.call_count} times.")

    await api_sink.stop()

if __name__ == "__main__":
    asyncio.run(main())
