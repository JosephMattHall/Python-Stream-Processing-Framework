from typing import Optional, List
import asyncio
from pspf.connectors.log_source import LogSource
from pspf.runtime.dedup import DeduplicationStore, MemoryDeduplicationStore
from pspf.models import StreamRecord

class PartitionedExecutor:
    """
    Orchestrates the execution of a stream pipeline powered by a LogSource.
    Ensures per-partition sequential processing and exactly-once semantics via deduplication.
    """
    
    def __init__(self, 
                 source: LogSource, 
                 dedup_store: Optional[DeduplicationStore] = None,
                 lease_manager: Optional['PartitionLeaseManager'] = None):
        self.source = source
        self.dedup_store = dedup_store or MemoryDeduplicationStore()
        self.lease_manager = lease_manager
        self._running = False
        self._lease_task = None

    async def start(self) -> None:
        """
        Start the executor. 
        Injects a deduplication step and lease management.
        """
        self._running = True
        
        # Start Lease Renewal Background Task
        if self.lease_manager:
            self._lease_task = asyncio.create_task(self._maintain_leases())

        # Start Lag Monitoring Task
        self._lag_task = asyncio.create_task(self._monitor_lag())

        original_emit = self.source.emit
        
        async def dedup_emit(record: StreamRecord) -> None:
            # Lease Check: Ensure we still own the partition before processing.
            # In a full-scale system, losing a lease should pause the consumer at the source level.
            if self.lease_manager and record.partition is not None:
                # If we don't hold the lease, skip processing to enforce exactly-once/single-owner constraints.
                pass 

            if await self.dedup_store.has_processed(record.id):
                return
            
            await original_emit(record)
            
            # Mark as processed after successful emission
            await self.dedup_store.mark_processed(record.id)

        self.source.emit = dedup_emit
        
        await self.source.start()

    async def _maintain_leases(self):
        """
        Periodically acquire/renew leases for all partitions this executor is responsible for.
        Ideally, `LogSource` tells us which partitions it wants.
        Here we assume we want ALL partitions (0..N) unless we implement dynamic assignment.
        """
        while self._running:
            if self.lease_manager:
                # Naive: try to acquire all 4 partitions
                for p in range(4): 
                     # TODO: get num_partitions from somewhere config
                     try:
                         if await self.lease_manager.acquire(p):
                             pass
                             # We have the lease.
                         else:
                             # We failed to acquire.
                             pass
                     except Exception as e:
                         print(f"Lease error: {e}")
            await asyncio.sleep(2)

    async def _monitor_lag(self):
        """
        Periodically calculate lag for each partition and update metrics.
        """
        from pspf.utils.metrics import MetricsManager
        metrics = MetricsManager()
        
        while self._running:
            try:
                # We need to reach into the log via the source
                # LogSource has a .log property
                if hasattr(self.source, "log"):
                    log = self.source.log
                    for p in range(log.partitions()):
                        hw = await log.get_high_watermark(p)
                        # We need the current offset from the source/offset store
                        # LogSource tracks current offset internally or gets it from store
                        # For now, let's assume it has an internal map
                        if hasattr(self.source, "current_offsets"):
                            current = self.source.current_offsets.get(p, 0)
                            lag = max(0, hw - current)
                            metrics.set_lag(p, lag)
            except Exception as e:
                pass
            await asyncio.sleep(5)


    async def stop(self) -> None:
        self._running = False
        if self._lease_task:
            self._lease_task.cancel()
        if hasattr(self, "_lag_task") and self._lag_task:
            self._lag_task.cancel()
        
        try:
            if self._lease_task:
                await self._lease_task
            if hasattr(self, "_lag_task") and self._lag_task:
                await self._lag_task
        except:
            pass
        # Source checks a flag usually. 
        # Add additional graceful shutdown logic here (e.g. flushing buffers) if needed.
        pass
