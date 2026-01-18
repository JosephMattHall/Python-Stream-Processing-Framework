import asyncio
import uuid
import os
from contextlib import asynccontextmanager
from typing import List
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

from pspf.models import StreamRecord
from pspf.log.local_log import LocalLog
from pspf.connectors.log_source import LogSource
from pspf.runtime.executor import PartitionedExecutor
from pspf.runtime.valkey_store import ValkeyOffsetStore, ValkeyDeduplicationStore
from pspf.utils.metrics import MetricsManager

from .pipeline import FraudDetector
from protos import fraud_pb2

DATA_DIR = os.getenv("PSPF_DATA_DIR", ".pspf_data_fraud")
NUM_PARTITIONS = 4

log = None
executor = None
detector = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global log, executor, detector
    log = LocalLog(data_dir=DATA_DIR, num_partitions=NUM_PARTITIONS)
    
    # Use distinct prefixes for Fraud App
    offset_store = ValkeyOffsetStore(prefix="pspf:fraud:offsets")
    dedup_store = ValkeyDeduplicationStore(prefix="pspf:fraud:dedup")
    
    source = LogSource(log, "fraud-processor", offset_store)
    detector = FraudDetector(log, dedup_store)
    
    async def bridge_emit(record: StreamRecord):
        await detector.process(record)
    source.emit = bridge_emit
    
    executor = PartitionedExecutor(source, dedup_store)
    asyncio.create_task(executor.start())
    yield
    await executor.stop()

app = FastAPI(lifespan=lifespan)

class TransactionRequest(BaseModel):
    user_id: str
    amount: float
    merchant: str

@app.post("/transactions")
async def create_transaction(req: TransactionRequest):
    event = fraud_pb2.Transaction(
        transaction_id=str(uuid.uuid4()),
        user_id=req.user_id,
        amount=req.amount,
        merchant=req.merchant,
        timestamp=datetime.now().isoformat()
    )
    record = StreamRecord(
        id=event.transaction_id,
        key=req.user_id,
        value=event.SerializeToString(),
        event_type="Transaction",
        timestamp=datetime.now()
    )
    await log.append(record)
    return {"status": "accepted"}

@app.get("/metrics")
async def get_metrics():
    mm = MetricsManager()
    if hasattr(mm, "_has_prom") and mm._has_prom:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi import Response
        return Response(content=generate_latest(mm.prom_registry), media_type=CONTENT_TYPE_LATEST)
    return mm.get_all()

@app.get("/history/{user_id}")
async def get_history(user_id: str):
    return detector.history.get(user_id, [])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
