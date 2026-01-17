import asyncio
import uuid
import os
from contextlib import asynccontextmanager
from typing import Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from pspf.models import StreamRecord
from pspf.log.local_log import LocalLog
from pspf.log.memory_store import MemoryOffsetStore
from pspf.connectors.log_source import LogSource
from pspf.runtime.executor import PartitionedExecutor
from pspf.runtime.dedup import MemoryDeduplicationStore

from .state import InventoryStateStore
from .pipeline import InventoryProcessor
from .events import ItemCreated, ItemCheckedIn, ItemCheckedOut

# --- Configuration ---
DATA_DIR = os.getenv("PSPF_DATA_DIR", ".pspf_data")
NUM_PARTITIONS = 4

# --- Globals ---
# In a real app, use dependency injection
log: LocalLog = None
state_store: InventoryStateStore = None
executor: PartitionedExecutor = None

# --- Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global log, state_store, executor
    
    # Init Core Components
    log = LocalLog(data_dir=DATA_DIR, num_partitions=NUM_PARTITIONS)
    state_store = InventoryStateStore()
    offset_store = MemoryOffsetStore()
    dedup_store = MemoryDeduplicationStore()
    
    # Setup Pipeline
    # 1. Source (Consumes from Log)
    source = LogSource(
        log=log, 
        consumer_group="inventory-processor", 
        offset_store=offset_store
    )
    
    # 2. Processor (Business Logic)
    processor = InventoryProcessor(
        state_store=state_store, 
        log=log, 
        dedup_store=dedup_store
    )
    
    # Wiring: Source -> Processor
    # LogSource emits StreamRecord. 
    # We assign the processor as the 'sink' or 'downstream operator'.
    # Since LogSource is a Source(Operator), we can add downstream.
    # But Processor is not a PSPF Operator class here, just a class with process().
    # Let's wrap it or just monkeypatch emit for simplicity/MVP?
    # Or cleaner: Make Processor an Operator?
    # Given the previous step where I didn't make InventoryProcessor inherit Operator,
    # I'll just bridge it.
    
    async def bridge_emit(record: StreamRecord):
        await processor.process(record)
        
    # Override source.emit to call our processor
    source.emit = bridge_emit
    
    # 3. Executor
    executor = PartitionedExecutor(source=source, dedup_store=dedup_store)
    
    # Start Executor (Background Task)
    executor_task = asyncio.create_task(executor.start())
    print("PSPF Inventory Pipeline Started")
    
    yield
    
    # Shutdown
    await executor.stop()
    executor_task.cancel()
    try:
        await executor_task
    except asyncio.CancelledError:
        pass
    print("PSPF Inventory Pipeline Stopped")

app = FastAPI(lifespan=lifespan)

# --- Pydantic Models for Requests ---
class CreateItemRequest(BaseModel):
    name: str
    initial_qty: int

class CheckInRequest(BaseModel):
    qty: int
    user_id: str

class CheckOutRequest(BaseModel):
    qty: int
    user_id: str

# --- API Endpoints ---

@app.get("/items/{item_id}")
async def get_item(item_id: str):
    item = state_store.get_item(item_id)
    return {
        "item_id": item.item_id,
        "name": item.name,
        "qty": item.qty,
        "exists": item.created
    }

from protos import inventory_pb2

# ... (Imports remain, remove .events imports if not needed, or keep for helper types)
# Actually we don't need .events classes here anymore for creation.

@app.post("/items/{item_id}")
async def create_item(item_id: str, req: CreateItemRequest):
    # 1. Create Protobuf Message
    event = inventory_pb2.ItemCreated(
        item_id=item_id,
        name=req.name,
        owner="user-api"
    )
    
    # 2. Append to Log
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=item_id,
        value=event.SerializeToString(),
        event_type="ItemCreated",
        timestamp=datetime.now(timezone.utc)
    )
    await log.append(record)
    
    # 3. Handle Initial Qty (as CheckIn)
    if req.initial_qty > 0:
        event_in = inventory_pb2.ItemCheckedIn(
            item_id=item_id,
            qty=req.initial_qty,
            user_id="system-init"
        )
        record_in = StreamRecord(
            id=str(uuid.uuid4()),
            key=item_id,
            value=event_in.SerializeToString(),
            event_type="ItemCheckedIn",
            timestamp=datetime.now(timezone.utc)
        )
        await log.append(record_in)

    return {"status": "accepted", "event_id": record.id}

@app.post("/items/{item_id}/checkin")
async def check_in(item_id: str, req: CheckInRequest):
    event = inventory_pb2.ItemCheckedIn(
        item_id=item_id,
        qty=req.qty,
        user_id=req.user_id
    )
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=item_id,
        value=event.SerializeToString(),
        event_type="ItemCheckedIn",
        timestamp=datetime.now(timezone.utc)
    )
    await log.append(record)
    return {"status": "accepted", "event_id": record.id}

@app.post("/items/{item_id}/checkout")
async def check_out(item_id: str, req: CheckOutRequest):
    event = inventory_pb2.ItemCheckedOut(
        item_id=item_id,
        qty=req.qty,
        user_id=req.user_id
    )
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=item_id,
        value=event.SerializeToString(),
        event_type="ItemCheckedOut",
        timestamp=datetime.now(timezone.utc)
    )
    await log.append(record)
    return {"status": "accepted", "request_id": record.id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
