import asyncio
import random
import os
from pspf.processor import BatchProcessor
from pspf.connectors.memory import MemoryBackend
from pspf.state.backends.sqlite import SQLiteStateStore
from pspf.context import Context
from pspf.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger("WordCount")

WORDS = ["stream", "processing", "python", "framework", "stateful", "fast", "reliable"]

async def run():
    # 1. Setup Backend
    backend = MemoryBackend(stream_key="words_stream")
    
    # 2. Setup State Store
    state_path = "data/state/wordcount.db"
    if os.path.exists(state_path):
        os.remove(state_path) # Clean start for demo
        
    store = SQLiteStateStore(path=state_path)
    
    # 3. Setup Processor with State
    processor = BatchProcessor(backend, state_store=store)

    # 4. Define Stateful Processing Function
    async def process_word(msg_id, data, ctx: Context):
        word = data.get("word")
        if not word: return

        # Get current count from state
        current_count = await ctx.state.get(word, 0)
        
        # Increment
        new_count = current_count + 1
        
        # Save back to state
        await ctx.state.put(word, new_count)
        
        logger.info(f"Word: '{word}' | Count: {new_count}")

    # 5. Start Processing Loop
    
    # Simulate incoming data
    async def producer():
        while True:
            await asyncio.sleep(0.5)
            word = random.choice(WORDS)
            await backend.add_event({"word": word})
            
    asyncio.create_task(producer())
    
    logger.info("Starting Stateful WordCount Processor...")
    # Run for 10 seconds then stop
    try:
        await asyncio.wait_for(processor.run_loop(process_word), timeout=10.0)
    except asyncio.TimeoutError:
        logger.info("Demo finished.")
    finally:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
