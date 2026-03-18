import asyncio
import time
from pspf import Stream
from pspf.processing.dsl import StreamBuilder

async def main():
    # 1. Use auto-instantiation (MemoryBackend by default in dev)
    topic = "dsl_demo"
    stream = Stream(topic=topic, group="dsl_group")
    
    # 2. Use the Filter-Map DSL
    builder = StreamBuilder(stream)
    
    # Define a simple pipeline that routes "high value" events to a side-topic
    builder.map(lambda x: {**x, "processed_at": time.time()}) \
           .filter(lambda x: x.get("value", 0) > 5) \
           .map(lambda x: {"message": f"High Value Event: {x}"}) \
           .sink(Stream(topic="dsl_output", group="dsl_output_group"))

    print(f"🚀 DSL Demo Started on topic '{topic}'...")
    async with stream:
        # 3. Emit some test data
        print("📤 Emitting events (0-9)...")
        for i in range(10):
            await stream.emit({"value": i})
        
        # 4. Start processing
        # run_forever handles the DSL pipeline automatically
        print("Processing events...")
        task = asyncio.create_task(stream.run_forever())
        
        # Give it a few seconds to process
        await asyncio.sleep(3)
        
        print("Finishing DSL Demo.")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    asyncio.run(main())
