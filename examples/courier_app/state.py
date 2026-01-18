from typing import List, Dict, Optional
from protos.courier_pb2 import JobCreated, DriverOnline, JobAssigned, JobDelivered

class CourierStateStore:
    def __init__(self):
        # Queues
        self.pending_jobs: List[JobCreated] = []
        self.idle_drivers: List[DriverOnline] = []
        
        # Lookups
        self.jobs: Dict[str, JobCreated] = {}
        self.assignments: Dict[str, str] = {} # job_id -> driver_id

    def add_job(self, job: JobCreated):
        self.jobs[job.job_id] = job
        self.pending_jobs.append(job)

    def add_driver(self, driver: DriverOnline):
        # Only add if not already idle? For MVP, just append.
        self.idle_drivers.append(driver)

    def assign(self, job_id: str, driver_id: str):
        # Remove from queues
        self.pending_jobs = [j for j in self.pending_jobs if j.job_id != job_id]
        self.idle_drivers = [d for d in self.idle_drivers if d.driver_id != driver_id]
        self.assignments[job_id] = driver_id

    def get_next_job(self) -> Optional[JobCreated]:
        return self.pending_jobs[0] if self.pending_jobs else None

    def get_next_driver(self) -> Optional[DriverOnline]:
        return self.idle_drivers[0] if self.idle_drivers else None
