import asyncio
import random
import os
import sys
import time
import multiprocessing
from pspf import Stream
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.utils.logging import get_logger
from typing import Dict, Any

# Ensure we can import pspf even if not installed
sys.path.insert(0, os.getcwd())

logger = get_logger("ChaosDemo")

# Configuration
STREAM_KEY = f"chaos-demo-stream-{int(time.time())}"
GROUP_NAME = "chaos-demo-group"
TOTAL_MESSAGES = 200 # Quicker demo

async def worker_process(node_id: str, duration: int):
    """
    Worker process that runs for a random duration or until stopped.
    """
    logger.info(f"Worker {node_id} started.")
    
    # Setup State Store for this node
    store_path = f"data/chaos_state/{node_id}.db"
    os.makedirs(os.path.dirname(store_path), exist_ok=True)
    store = SQLiteStateStore(store_path)
    
    # Initialize Stream with persistent state
    # It will auto-instantiate Valkey if available, or fall back to Memory (which won't work across processes, so we assume Valkey for this demo)
    stream = Stream(
        topic=STREAM_KEY, 
        group=GROUP_NAME, 
        state_store=store
    )
    
    # Handler: Count total messages processed in state
    @stream.subscribe(STREAM_KEY)
    async def handler(msg_id: str, data: Dict[str, Any], ctx) -> None:
        count = await ctx.state.get("processed_count", 0)
        await ctx.state.put("processed_count", count + 1)
        # Simulate work
        await asyncio.sleep(random.uniform(0.001, 0.01))

    logger.info(f"Worker {node_id} running loop for {duration}s.")
    async with stream:
        try:
            await asyncio.wait_for(stream.run_forever(), timeout=duration)
        except asyncio.TimeoutError:
            logger.info(f"Worker {node_id} finished duration.")
        except asyncio.CancelledError:
            logger.info(f"Worker {node_id} cancelled.")

def run_worker(node_id, duration):
    asyncio.run(worker_process(node_id, duration))

async def producer_process():
    # Use Stream to emit events
    stream = Stream(topic=STREAM_KEY, group="producer-group")
    async with stream:
        logger.info(f"Producing {TOTAL_MESSAGES} messages...")
        for i in range(TOTAL_MESSAGES):
            await stream.emit({"seq": i, "data": "x" * 100})
            if i % 100 == 0:
                await asyncio.sleep(0.1)
        logger.info("Producer done.")

import argparse

async def main():
    parser = argparse.ArgumentParser(description="PSPF Chaos Stress Test Demo")
    parser.add_argument("--duration", type=int, default=15, help="Duration of the chaos test in seconds")
    parser.add_argument("--messages", type=int, default=200, help="Total messages to produce")
    args = parser.parse_args()

    global TOTAL_MESSAGES
    TOTAL_MESSAGES = args.messages

    # Setup
    os.makedirs("data/chaos_state", exist_ok=True)
    
    # Start Producer
    prod_task = asyncio.create_task(producer_process())
    
    # Chaos Controller
    workers = []
    start_time = time.time()
    logger.info(f"Starting chaos controller for {args.duration}s...")
    
    while time.time() - start_time < args.duration:
        # Spawn a worker
        node_id = f"n{int(time.time()*1000)}"
        worker_duration = random.randint(3, 6)
        p = multiprocessing.Process(target=run_worker, args=(node_id, worker_duration))
        p.start()
        workers.append(p)
        logger.info(f"Spawned worker {node_id} (pid {p.pid})")
        
        # Kill random worker
        if len(workers) > 2:
            victim = random.choice(workers)
            if victim.is_alive():
                logger.warning(f"Killing worker pid {victim.pid}")
                victim.terminate() # SIGTERM
                victim.join()
                workers.remove(victim)
        
        await asyncio.sleep(2)
        
    await prod_task
    
    # Cleanup workers
    for p in workers:
        if p.is_alive():
            p.terminate()
            p.join()
            
    # Wait for final stability
    await asyncio.sleep(2)
            
    # Verify Data
    logger.info("Starting final recovery worker to verify lag...")
    # Using a dummy stream to check lag
    stream = Stream(topic=STREAM_KEY, group=GROUP_NAME)
    async with stream:
        info = await stream.backend.get_pending_info()
        logger.info(f"Final Status: {info}")
        
        if info["lag"] == 0:
            logger.info("✅ SUCCESS: All messages rebalanced and processed.")
        else:
            logger.error("❌ FAILURE: Messages lost or stuck.")
            sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

