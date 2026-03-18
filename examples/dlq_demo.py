import asyncio
import time
from pspf import Stream
from pspf.processing.windows import TumblingWindow

async def main():
    # 1. Define the stream with auto-instantiation
    # We use a unique topic for this demo
    topic = "dlq_demo_stream"
    stream = Stream(topic=topic, group="dlq_demo_group")

    # 2. Register a handler that fails for specific values
    @stream.subscribe(topic)
    async def process_with_failure(event):
        val = event.get("value")
        if val == "FAIL":
            print(f"❌ Simulating failure for event: {event}")
            raise ValueError("Intentional failure for DLQ demo")
        print(f"✅ Processed event: {event}")

    # 3. Register a windowed aggregation with a watermark
    # This will route late events to {topic}-late
    @stream.window(topic, TumblingWindow(size_ms=5000), watermark_delay_ms=2000)
    async def aggregate_events(event, current_state):
        return (current_state or 0) + 1

    print(f"🚀 DLQ Demo Started on topic '{topic}'...")
    async with stream:
        # Start processing in background
        task = asyncio.create_task(stream.run_forever())

        # 4. Emit events
        print("📤 Emitting events...")
        
        # A normal event
        await stream.emit({"value": "OK", "timestamp": time.time()})
        
        # An event that will fail and go to DLQ
        await stream.emit({"value": "FAIL", "timestamp": time.time()})
        
        # A late event that will go to {topic}-late
        # Window size 5s, watermark delay 2s. 
        # If we send an event 10s old, it's definitely late.
        late_ts = time.time() - 10 
        await stream.emit({"value": "LATE", "timestamp": late_ts})

        print("Waiting for processing (approx 5s)...")
        await asyncio.sleep(5)

        print("\n--- Demo Instructions ---")
        print(f"1. Inspect the Processing DLQ:  pspfctl dlq-inspect {topic}")
        print(f"2. Inspect the Late Event DLQ: pspfctl dlq-inspect {topic}-late")
        print(f"3. Replay from DLQ:            pspfctl replay {topic} dlq_demo_group")
        
        await stream.stop()
        await task

if __name__ == "__main__":
    asyncio.run(main())
