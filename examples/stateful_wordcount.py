import asyncio
import random
import os
from pspf import Stream
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.utils.logging import get_logger

logger = get_logger("WordCount")

WORDS = ["stream", "processing", "python", "framework", "stateful", "fast", "reliable"]

async def main():
    # 1. Setup State Store
    state_path = "data/state/wordcount.db"
    # Ensure directory exists
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    if os.path.exists(state_path):
        os.remove(state_path) # Clean start for demo
        
    store = SQLiteStateStore(path=state_path)
    
    # 2. Setup Stream with StateStore
    topic = "words_stream"
    stream = Stream(topic=topic, group="wordcount-group", state_store=store)

    # 3. Define Stateful Processing Function
    @stream.subscribe(topic)
    async def process_word(msg_id, data, ctx):
        word = data.get("word")
        if not word: return

        # ctx.state is automatically injected from the stream's state_store
        # Get current count from state
        current_count = await ctx.state.get(word, 0)
        
        # Increment
        new_count = current_count + 1
        
        # Save back to state
        await ctx.state.put(word, new_count)
        
        logger.info(f"Word: '{word}' | Count: {new_count}")

    print("🚀 Starting Stateful WordCount Demo...")
    async with stream:
        # 4. Simulate incoming data
        async def producer():
            while True:
                await asyncio.sleep(0.5)
                word = random.choice(WORDS)
                await stream.emit({"word": word})
                
        producer_task = asyncio.create_task(producer())
        
        # 5. Run for 10 seconds then stop
        try:
            await asyncio.wait_for(stream.run_forever(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.info("Demo finished.")
        finally:
            producer_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
