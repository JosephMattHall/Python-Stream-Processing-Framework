import asyncio
from typing import Any, List, Optional
from pspf.connectors.base import Source
from pspf.log.interfaces import Log, OffsetStore
from pspf.models import StreamRecord
from pspf.utils.logging import get_logger

class LogSource(Source[StreamRecord]):
    """
    Source connector that reads from the native PSPF Log.
    Replaces Kafka consumer functionality reliably without external dependencies.
    """
    
    def __init__(self, 
                 log: Log, 
                 consumer_group: str, 
                 offset_store: OffsetStore,
                 partitions: Optional[List[int]] = None,
                 poll_interval: float = 0.1):
        super().__init__(name=f"LogSource({consumer_group})")
        self.log = log
        self.consumer_group = consumer_group
        self.offset_store = offset_store
        self.poll_interval = poll_interval
        self._running = False
        # If partitions not specified, read all
        self.partitions_to_read = partitions

    async def start(self) -> None:
        """Start consuming from the log."""
        self._running = True
        
        if self.partitions_to_read is None:
            self.partitions_to_read = list(range(self.log.partitions()))
            
        self.logger.info(f"Starting LogSource for group {self.consumer_group} on partitions {self.partitions_to_read}")
        
        # Start a task for each partition
        tasks = [
            asyncio.create_task(self._consume_partition(p))
            for p in self.partitions_to_read
        ]
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.logger.info("LogSource stopped.")
            self._running = False
            # Wait for tasks to finish?
            raise

    async def _consume_partition(self, partition: int) -> None:
        """Consume loop for a single partition."""
        # Restore offset
        current_offset = await self.offset_store.get(self.consumer_group, partition)
        self.logger.info(f"Partition {partition} starting at offset {current_offset}")
        
        while self._running:
            # Read from log
            found_records = False
            async for record in self.log.read(partition, current_offset):
                found_records = True
                
                # Emit to pipeline
                # Note: StreamRecord is the item type
                await self.emit(record)
                
                # Commit offset (or update local tracking)
                # Commit offset immediately to ensure at-least-once delivery.
                # While a periodic autocommit (like Kafka) would offer higher throughput,
                # committing per-message provides stronger safety guarantees for this implementation.
                # TODO: Implement batched commits for better performance under high load.
                current_offset = record.offset + 1
                # in this simpler implimentation, to be safe.
                # commit every message - correctness > perfection.
                await self.offset_store.commit(self.consumer_group, partition, current_offset)
                
            if not found_records:
                await asyncio.sleep(self.poll_interval)
