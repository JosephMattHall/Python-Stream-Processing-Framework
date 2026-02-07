import asyncio
import logging
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.stream import Stream
from pspf.schema import BaseEvent
from pspf.utils.logging import setup_logging
import uuid

# Setup Logging
setup_logging(level=logging.INFO)

class TestEvent(BaseEvent):
    event_type: str = "TestEvent"
    val: int

from pspf.settings import settings

async def main():
    # settings automatically loads .env or defaults
    connector = ValkeyConnector(host=settings.VALKEY_HOST, port=settings.VALKEY_PORT)
    backend = ValkeyStreamBackend(connector, "test_stream", "test_group", "consumer_1")
    
    # Use the refactored Stream with composition
    async with Stream(backend, schema=TestEvent) as stream:
        # Verify Health
        health = await stream.health()
        print(f"Health Check: {health}")
        
        # 1. Produce
        print("--- Producing 5 events ---")
        for i in range(5):
            evt = TestEvent(val=i)
            msg_id = await stream.emit(evt)
            print(f"Produced {msg_id}")
            
        # 2. Consume (Simulated)
        # We will run the processor in a task and cancel it after a few seconds
        print("--- Consuming ---")
        
        async def handler(event: TestEvent):
            print(f"Handled event: {event.val}, ID: {event.offset}")
            if event.val == 3:
                raise ValueError("Simulated Failure!")

        # Run processor for 3 seconds
        try:
            await asyncio.wait_for(stream.run(handler, batch_size=2), timeout=3.0)
        except asyncio.TimeoutError:
            print("Stopped consumer loop.")
            
        # 3. Verification of DLO/Retry
        # Since we raised ValueError on val=3, and default max_retries is 3 in BatchProcessor...
        # We would need to run the loop multiple times to trigger DLO move.
        # But this script verifies basic flow is working.

if __name__ == "__main__":
    asyncio.run(main())
