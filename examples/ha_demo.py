import asyncio
import os
import uuid
from datetime import datetime
from pspf.processor import BatchProcessor
from pspf.connectors.memory import MemoryBackend
from pspf.utils.logging import setup_logging, get_logger
from pspf.cluster.coordinator import ClusterCoordinator
from pspf.log.local_log import LocalLog
from pspf.log.replicated_log import ReplicatedLog
from pspf.models import StreamRecord
import pspf.settings as settings

setup_logging()
logger = get_logger("HADemo")

# Read config from Env
NODE_ID = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:4]}")
VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
ADMIN_PORT = int(os.getenv("ADMIN_PORT", 8001))
DATA_DIR = os.getenv("DATA_DIR", f"./data/{NODE_ID}")
HOST = os.getenv("HOST", "localhost")

async def run():
    # 1. Setup Backend (Source) - Just a dummy memory backend for driving the loop
    backend = MemoryBackend(stream_key="ha_source", group=f"ha_group_{NODE_ID}")
    
    # 2. Setup HA Components
    coordinator = ClusterCoordinator(
        valkey_url=f"redis://{VALKEY_HOST}:6379",
        host=HOST, # In real docker this would be container IP/hostname
        port=ADMIN_PORT,
        node_id=NODE_ID
    )
    
    # Ensure data dir
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    local_log = LocalLog(DATA_DIR, num_partitions=1)
    # Inject admin port passing to replicated log if needed (it wasn't in init but useful for debugging)
    replicated_log = ReplicatedLog(local_log, coordinator, admin_port=ADMIN_PORT)
    
    # 3. Setup Processor
    processor = BatchProcessor(backend)
    
    # Attach HA components to processor so Admin API can find them
    processor.replicated_log = replicated_log
    processor.coordinator = coordinator
    
    # Override settings for Admin Port
    # settings.ADMIN_PORT is global, so we need to be careful if running multiple in same process (not case here)
    settings.ADMIN_PORT = ADMIN_PORT

    # Define Processing Logic
    async def process_msg(event):
        """
        Receive event, try to write to Replicated Log.
        """
        # Create a record to append
        record = StreamRecord(
            id=event.event_id,
            key=event.payload.get("key", "default"),
            value=event.payload,
            timestamp=datetime.now(),
            topic="replicated_topic"
        )
        
        try:
            await replicated_log.append(record)
            logger.info(f"Leader Append Success: {record.id}")
            return True
        except Exception as e:
            if "Not leader" in str(e):
                logger.debug(f"Follower skipping write: {e}")
                # We are not leader, so we don't write. 
                # In a real app, we might redirect or just consume without writing state.
                return True 
            else:
                logger.error(f"Append Error: {e}")
                raise e

    # 4. Start Everything
    await coordinator.start()
    
    # Seed some data into memory backend so we have loop activity
    async def seed_data():
        while True:
            await asyncio.sleep(1)
            await backend.send({"event_type": "tick", "key": "p0", "val": datetime.now().isoformat()})
            
    asyncio.create_task(seed_data())

    logger.info(f"Starting HA Node {NODE_ID} on port {ADMIN_PORT}")
    try:
        await processor.run(process_msg)
    finally:
        await coordinator.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
