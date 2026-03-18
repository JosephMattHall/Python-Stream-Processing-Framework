import asyncio
import random
import signal
import os
import sys
import time
import multiprocessing
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.processor import BatchProcessor
from pspf.state.backends.sqlite_store import SQLiteStateStore
from pspf.context import Context
from pspf.utils.logging import setup_logging, get_logger
from typing import Dict, Any

# Ensure we can import pspf even if not installed
sys.path.insert(0, os.getcwd())


setup_logging()
logger = get_logger("ChaosDemo")

# Configuration
STREAM_KEY = f"chaos-demo-stream-{int(time.time())}"
GROUP_NAME = "chaos-demo-group"
TOTAL_MESSAGES = 500 # Reduced for a quicker demo


async def worker_process(node_id: str, duration: int):
    """
    Worker process that runs for a random duration or until stopped.
    """
    logger.info(f"Worker {node_id} started.")
    connector = ValkeyConnector(host="localhost", port=6379)
    try:
        await connector.connect()
        backend = ValkeyStreamBackend(connector, STREAM_KEY, GROUP_NAME, f"worker-{node_id}")
        await backend.ensure_group_exists()
        
        store_path = f"data/chaos_state/{node_id}.db"
        store = SQLiteStateStore(store_path)
        await store.start()
        
        processor = BatchProcessor(
            backend, 
            state_store=store, 
            max_retries=5, 
            min_idle_time_ms=2000,
            start_admin_server=False # Avoid port conflicts in multi-process demo

        )

        
        # Handler: Count total messages processed in state
        async def handler(msg_id: str, data: Dict[str, Any], ctx: Context) -> None:
            # We track global count in Redis to verify later? 
            # Or just local state count.
            # Let's verify no data loss.
            # We will use state to count "processed"
            assert ctx.state
            count = await ctx.state.get("processed_count", 0)
            await ctx.state.put("processed_count", count + 1)
            
            # Simulate work
            await asyncio.sleep(random.uniform(0.001, 0.01))

        # Run loop
        logger.info(f"Worker {node_id} running loop.")
        try:
            await asyncio.wait_for(processor.run_loop(handler), timeout=duration) # type: ignore
        except asyncio.TimeoutError:
            logger.info(f"Worker {node_id} finished duration.")
        except asyncio.CancelledError:
            logger.info(f"Worker {node_id} cancelled.")
        finally:
            await processor.shutdown()
            await store.stop()
            await connector.close()
            
    except Exception as e:
        logger.error(f"Worker {node_id} crashed: {e}")

def run_worker(node_id, duration):
    asyncio.run(worker_process(node_id, duration))

async def producer_process():
    connector = ValkeyConnector(host="localhost", port=6379)
    await connector.connect()
    backend = ValkeyStreamBackend(connector, STREAM_KEY, GROUP_NAME, "producer")
    
    logger.info(f"Producing {TOTAL_MESSAGES} messages...")
    for i in range(TOTAL_MESSAGES):
        await backend.add_event({"seq": i, "data": "x" * 100})
        if i % 100 == 0:
            await asyncio.sleep(0.1)
            
    logger.info("Producer done.")
    await connector.close()

import argparse

async def main():
    parser = argparse.ArgumentParser(description="PSPF Chaos Stress Test Demo")
    parser.add_argument("--duration", type=int, default=20, help="Duration of the chaos test in seconds")
    parser.add_argument("--messages", type=int, default=500, help="Total messages to produce")
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
    
    # Run for the specified duration, spawning and killing workers
    while time.time() - start_time < args.duration:
        # Spawn a worker
        node_id = f"n{int(time.time()*1000)}"
        worker_duration = random.randint(3, 8)
        p = multiprocessing.Process(target=run_worker, args=(node_id, worker_duration))
        p.start()
        workers.append(p)
        logger.info(f"Spawned worker {node_id} (pid {p.pid})")
        
        # Kill random worker
        if len(workers) > 3:
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
            
    # Wait for idle time
    time.sleep(2)
            
    # Verify Data
    logger.info("Starting final recovery worker...")
    final_p = multiprocessing.Process(target=run_worker, args=("recovery", 5))
    final_p.start()
    final_p.join()
    
    connector = ValkeyConnector(host="localhost", port=6379)
    await connector.connect()
    backend = ValkeyStreamBackend(connector, STREAM_KEY, GROUP_NAME, "monitor")
    info = await backend.get_pending_info()
    logger.info(f"Final Status: {info}")
    
    if info["lag"] == 0:
        logger.info("✅ SUCCESS: All messages rebalanced and processed.")
    else:
        logger.error("❌ FAILURE: Messages lost or stuck.")
        sys.exit(1)

    await connector.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

