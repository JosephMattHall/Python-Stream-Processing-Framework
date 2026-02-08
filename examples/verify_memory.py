import asyncio
import logging
from pspf.connectors.memory import MemoryBackend
from pspf.stream import Stream
from pspf.schema import BaseEvent
from pspf.utils.logging import setup_logging
import uuid

# Setup Logging
setup_logging(level=logging.INFO)

class TestEvent(BaseEvent):
    event_type: str = "TestEvent"
    val: int

async def main() -> None:
    # Use MemoryBackend instead of Valkey
    backend = MemoryBackend()
    
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
            
        # 2. Consume 
        print("--- Consuming ---")
        
        received_count = 0
        
        async def handler(event: TestEvent) -> None:
            nonlocal received_count
            print(f"Handled event: {event.val}, ID: {event.offset}")
            received_count += 1

        # Run processor for 2 seconds
        # MemoryBackend returns instantly, so this should process quickly
        try:
            await asyncio.wait_for(stream.run(handler, batch_size=2), timeout=2.0)
        except asyncio.TimeoutError:
            print("Stopped consumer loop.")
            
        print(f"Total Received: {received_count}")
        assert received_count == 5, f"Expected 5 messages, got {received_count}"
        print("Verification Successful!")

if __name__ == "__main__":
    asyncio.run(main())
