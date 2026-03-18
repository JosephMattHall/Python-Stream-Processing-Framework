import asyncio
import time
from pspf.stream import Stream
from pspf.settings import Settings
from pspf.connectors.valkey import ValkeyStreamBackend
from pspf.processing.dsl import StreamBuilder
from pspf.processing.windows import SessionWindow

async def main():
    from pspf.settings import settings
    
    # Disable telemetry to avoid port conflicts in Docker
    settings.telemetry.ENABLED = False
    
    # Use auto-instantiation (Valkey with Memory fallback)
    stream = Stream(topic="dsl_demo", group="dsl_group")
    
    # Use the new Functional DSL
    builder = StreamBuilder(stream)
    
    # Pipeline: transform data, filter, and print
    builder.map(lambda x: {**x, "processed_at": time.time()}) \
           .filter(lambda x: x.get("value", 0) > 5) \
           .map(lambda x: {"message": f"High Value Event: {x}"}) \
           .sink(Stream(topic="dsl_output", group="dsl_output_group"))

    print("Subscribed to dsl_demo with Functional DSL.")
    print("Producing some test data...")
    
    for i in range(10):
        await stream.emit({"value": i})
    
    # Start processor in background
    task = asyncio.create_task(stream.run_forever())
    
    print("Waiting for processing...")
    await asyncio.sleep(5)
    
    # Verify session window (dummy check)
    sw = SessionWindow(gap_ms=5000)
    print(f"Session Window Assign: {sw.assign_windows(time.time())}")
    
    await stream.stop()
    await task
    print("DSL Demo finished.")

if __name__ == "__main__":
    asyncio.run(main())
