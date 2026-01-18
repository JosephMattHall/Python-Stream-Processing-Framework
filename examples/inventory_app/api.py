import asyncio
import uuid
import os
from contextlib import asynccontextmanager
from typing import Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pspf.models import StreamRecord
from pspf.log.local_log import LocalLog
from pspf.connectors.log_source import LogSource
from pspf.runtime.executor import PartitionedExecutor
from pspf.runtime.valkey_store import ValkeyOffsetStore, ValkeyDeduplicationStore
from pspf.runtime.coordination import PartitionLeaseManager

from .state import ValkeyInventoryStateStore
from .pipeline import InventoryProcessor
from protos import inventory_pb2

DATA_DIR = os.getenv("PSPF_DATA_DIR", ".pspf_data")
NUM_PARTITIONS = 4

# Globals for the background services
log: LocalLog = None
state_store: ValkeyInventoryStateStore = None
executor: PartitionedExecutor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global log, state_store, executor
    
    # Wire up the fundamental infrastructure
    log = LocalLog(data_dir=DATA_DIR, num_partitions=NUM_PARTITIONS)
    state_store = ValkeyInventoryStateStore(host='localhost', port=6379)
    offset_store = ValkeyOffsetStore(host='localhost', port=6379)
    dedup_store = ValkeyDeduplicationStore(host='localhost', port=6379)
    lease_manager = PartitionLeaseManager(service_name="inventory-api", host='localhost', port=6379)
    
    source = LogSource(log=log, consumer_group="inventory-processor", offset_store=offset_store)
    processor = InventoryProcessor(state_store=state_store, log=log, dedup_store=dedup_store)
    
    # Connect the log source to our business processor
    async def bridge_emit(record: StreamRecord):
        await processor.process(record)
    source.emit = bridge_emit
    
    executor = PartitionedExecutor(source=source, dedup_store=dedup_store, lease_manager=lease_manager)
    
    # Start the continuous processing loop in the background
    executor_task = asyncio.create_task(executor.start())
    print("PSPF Inventory Pipeline Started")
    
    yield
    
    # Clean shutdown
    await executor.stop()
    executor_task.cancel()
    try:
        await executor_task
    except asyncio.CancelledError:
        pass
    print("PSPF Inventory Pipeline Stopped")

app = FastAPI(lifespan=lifespan)

class CreateItemRequest(BaseModel):
    name: str
    initial_qty: int

class CheckInRequest(BaseModel):
    qty: int
    user_id: str

class CheckOutRequest(BaseModel):
    qty: int
    user_id: str

from pspf.utils.metrics import MetricsManager

@app.get("/metrics")
async def get_metrics():
    mm = MetricsManager()
    if hasattr(mm, "_has_prom") and mm._has_prom:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi import Response
        return Response(content=generate_latest(mm.prom_registry), media_type=CONTENT_TYPE_LATEST)
    return mm.get_all()

@app.get("/items/{item_id}")
async def get_item(item_id: str):
    item = await state_store.get_item(item_id)
    return {
        "item_id": item.item_id,
        "name": item.name,
        "qty": item.qty,
        "exists": item.created
    }

@app.post("/items/{item_id}")
async def create_item(item_id: str, req: CreateItemRequest):
    event = inventory_pb2.ItemCreated(
        item_id=item_id,
        name=req.name,
        owner="user-api"
    )
    
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=item_id,
        value=event.SerializeToString(),
        event_type="ItemCreated",
        timestamp=datetime.now(timezone.utc)
    )
    await log.append(record)
    
    # If we have initial stock, we record it as a separate check-in event
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
