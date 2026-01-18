import asyncio
import uuid
from datetime import datetime
from typing import Optional

from pspf.models import StreamRecord
from pspf.log.interfaces import Log
from pspf.runtime.dedup import DeduplicationStore
from pspf.utils.metrics import MetricsManager
from .state import CourierStateStore
from protos import courier_pb2

class CourierProcessor:
    """
    Coordinates drivers and jobs.
    When a driver comes online or a job is created, we try to match them.
    """
    def __init__(self, state_store: CourierStateStore, log: Log, dedup_store: DeduplicationStore):
        self.state_store = state_store
        self.log = log
        self.dedup_store = dedup_store
        self.metrics = MetricsManager()
        self.events_total = self.metrics.counter("courier_events_total")
        self.matches_total = self.metrics.counter("courier_matches_total")

    async def process(self, record: StreamRecord) -> None:
        if await self.dedup_store.has_processed(record.id):
            return

        self.events_total.inc()

        event_type = getattr(record, "event_type", "")
        
        try:
            if event_type == "JobCreated":
                job = courier_pb2.JobCreated()
                job.ParseFromString(record.value)
                self.state_store.add_job(job)
                self._try_match()
                
            elif event_type == "DriverOnline":
                driver = courier_pb2.DriverOnline()
                driver.ParseFromString(record.value)
                self.state_store.add_driver(driver)
                self._try_match()
                
            elif event_type == "JobAssigned":
                 # Update state locally if we see an assignment (for replay/multi-node sync)
                 assignment = courier_pb2.JobAssigned()
                 assignment.ParseFromString(record.value)
                 self.state_store.assign(assignment.job_id, assignment.driver_id)

        except Exception as e:
            print(f"Error processing {event_type}: {e}")

        await self.dedup_store.mark_processed(record.id)

    def _try_match(self):
        # Naive first-come first-served matching
        job = self.state_store.get_next_job()
        driver = self.state_store.get_next_driver()
        
        if job and driver:
            # We found a pair! 
            # We schedule the emission to handle the IO without blocking the main event stream
            asyncio.create_task(self._emit_assignment(job, driver))

    async def _emit_assignment(self, job, driver):
        event = courier_pb2.JobAssigned(
            job_id=job.job_id,
            driver_id=driver.driver_id,
            timestamp=datetime.now().isoformat()
        )
        record = StreamRecord(
            id=str(uuid.uuid4()),
            key=job.job_id,
            value=event.SerializeToString(),
            event_type="JobAssigned",
            timestamp=datetime.now()
        )
        
        # Reserve the match in our state store immediately to prevent double-booking
        # while the event is being persisted to the log.
        self.state_store.assign(job.job_id, driver.driver_id)
        self.matches_total.inc()
        await self.log.append(record)
        print(f"MATCHED: Job {job.job_id} :: Driver {driver.driver_id}")
