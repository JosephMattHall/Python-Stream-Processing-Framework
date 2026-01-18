import asyncio
import uuid
from datetime import datetime
from typing import Dict, List
from statistics import mean

from pspf.models import StreamRecord
from pspf.log.interfaces import Log
from pspf.runtime.dedup import DeduplicationStore
from pspf.utils.metrics import MetricsManager
from protos import fraud_pb2

class FraudDetector:
    """
    Analyzes transaction streams for anomalous spending patterns.
    """
    def __init__(self, log: Log, dedup_store: DeduplicationStore):
        self.log = log
        self.dedup_store = dedup_store
        # Local state: User -> List of recent transaction amounts
        self.history: Dict[str, List[float]] = {}
        self.metrics = MetricsManager()
        self.events_total = self.metrics.counter("fraud_events_total")
        self.alerts_total = self.metrics.counter("fraud_alerts_total")

    async def process(self, record: StreamRecord) -> None:
        if await self.dedup_store.has_processed(record.id):
            return

        self.events_total.inc()

        event_type = getattr(record, "event_type", "")
        if event_type == "Transaction":
            txn = fraud_pb2.Transaction()
            txn.ParseFromString(record.value)
            await self._analyze(txn)

        await self.dedup_store.mark_processed(record.id)

    async def _analyze(self, txn: fraud_pb2.Transaction):
        user_history = self.history.get(txn.user_id, [])
        
        # Simple anomaly check: current transaction > 2x average of the last few
        if len(user_history) >= 3:
            avg_spend = mean(user_history)
            if txn.amount > avg_spend * 2.0:
                print(f"FRAUD DETECTED: User {txn.user_id} spent {txn.amount} (Avg: {avg_spend})")
                
                alert = fraud_pb2.FraudAlert(
                    alert_id=str(uuid.uuid4()),
                    user_id=txn.user_id,
                    reason="High Value Anomaly",
                    transaction_amount=txn.amount,
                    avg_spend=avg_spend,
                    timestamp=datetime.now().isoformat()
                )
                
                # Emit the alert back to the log as a derived event stream
                record = StreamRecord(
                    id=alert.alert_id,
                    key=txn.user_id,
                    value=alert.SerializeToString(),
                    event_type="FraudAlert",
                    timestamp=datetime.now()
                )
                self.alerts_total.inc()
                await self.log.append(record)

        # Update our sliding window of user history
        user_history.append(txn.amount)
        if len(user_history) > 10:
            user_history.pop(0)
        self.history[txn.user_id] = user_history
