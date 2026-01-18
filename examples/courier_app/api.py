import asyncio
import uuid
import os
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

from pspf.models import StreamRecord
from pspf.log.local_log import LocalLog
from pspf.connectors.log_source import LogSource
from pspf.runtime.executor import PartitionedExecutor
from pspf.runtime.valkey_store import ValkeyOffsetStore, ValkeyDeduplicationStore
from pspf.runtime.coordination import PartitionLeaseManager
from pspf.utils.metrics import MetricsManager

from .state import CourierStateStore
from .pipeline import CourierProcessor
from protos import courier_pb2

DATA_DIR = os.getenv("PSPF_DATA_DIR", ".pspf_data_courier")
NUM_PARTITIONS = 4

log = None
state_store = None
executor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global log, state_store, executor
    log = LocalLog(data_dir=DATA_DIR, num_partitions=NUM_PARTITIONS)
    state_store = CourierStateStore()
    
    # Using Valkey for durability (assumed running from previous task)
    offset_store = ValkeyOffsetStore(prefix="pspf:courier:offsets")
    dedup_store = ValkeyDeduplicationStore(prefix="pspf:courier:dedup")
    lease_manager = PartitionLeaseManager(service_name="courier-api")
    
    source = LogSource(log, "courier-processor", offset_store)
    processor = CourierProcessor(state_store, log, dedup_store)
    
    async def bridge_emit(record: StreamRecord):
        await processor.process(record)
    source.emit = bridge_emit
    
    executor = PartitionedExecutor(source, dedup_store, lease_manager)
    asyncio.create_task(executor.start())
    yield
    await executor.stop()

app = FastAPI(lifespan=lifespan)

class CreateJobRequest(BaseModel):
    customer_id: str
    pickup: str
    dropoff: str

class DriverOnlineRequest(BaseModel):
    driver_id: str
    vehicle: str

@app.post("/jobs")
async def create_job(req: CreateJobRequest):
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    event = courier_pb2.JobCreated(
        job_id=job_id,
        customer_id=req.customer_id,
        pickup_address=req.pickup,
        delivery_address=req.dropoff
    )
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=job_id,
        value=event.SerializeToString(),
        event_type="JobCreated",
        timestamp=datetime.now()
    )
    await log.append(record)
    return {"job_id": job_id, "status": "pending"}

@app.post("/drivers/online")
async def driver_online(req: DriverOnlineRequest):
    event = courier_pb2.DriverOnline(
        driver_id=req.driver_id,
        vehicle_type=req.vehicle
    )
    record = StreamRecord(
        id=str(uuid.uuid4()),
        key=req.driver_id, # Partition by Driver (Caution: Matching happens across partitions??)
        # Danger: If Job is part-0 and Driver part-1, they are processed by different workers (if scaled).
        # Our In-Memory matching logic assumes SINGLE PROCESS or GLOBAL STATE.
        # With "Valkey State Store" and single worker it works.
        # With multiple workers, "Matching" needs a shared queue (Valkey List).
        # For this MVP, we rely on Single Node.
        value=event.SerializeToString(),
        event_type="DriverOnline",
        timestamp=datetime.now()
    )
    await log.append(record)
    return {"status": "online"}

@app.get("/metrics")
async def get_metrics():
    mm = MetricsManager()
    if hasattr(mm, "_has_prom") and mm._has_prom:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi import Response
        return Response(content=generate_latest(mm.prom_registry), media_type=CONTENT_TYPE_LATEST)
    return mm.get_all()

@app.get("/assignments")
async def get_assignments():
    return state_store.assignments

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
