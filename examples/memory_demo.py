import asyncio
import time
from pspf.stream import Stream
from pspf.connectors.memory import MemoryBackend

async def main():
    # 1. Create a MemoryBackend and a Stream
    backend = MemoryBackend(stream_key="memory_topic", group_name="test_group")
    stream = Stream(backend)

    # 2. Subscribe to the stream
    @stream.subscribe("memory_topic")
    async def process_event(event):
        print(f"Received in memory: {event}")

    # 3. Emit some events
    print("Emitting events to memory backend...")
    for i in range(5):
        await stream.emit({"message": f"Hello {i}", "value": i})

    # 4. Start the processor in the background
    print("Starting Stream processor...")
    task = asyncio.create_task(stream.run_forever())

    # Wait a bit for processing
    await asyncio.sleep(2)
    
    # 5. Stop the stream gracefully
    print("Stopping Stream...")
    await stream.stop()
    await task
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
