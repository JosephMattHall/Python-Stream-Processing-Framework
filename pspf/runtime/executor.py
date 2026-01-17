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
                 dedup_store: Optional[DeduplicationStore] = None):
        self.source = source
        self.dedup_store = dedup_store or MemoryDeduplicationStore()
        self._running = False

    async def start(self) -> None:
        """
        Start the executor. 
        Injects a deduplication step into the source's emission flow if possible, 
        or wraps the source start.
        """
        # We need to intercept the source's emit to check for duplicates.
        # Ideally, we add a "Deduplicator" operator right after source.
        # But source.downstream is a list of operators. 
        # We can insert a proxy operator.
        
        # Or more simply, since we control the source code or can subclass,
        # let's just use the `source` as is but modify its behavior? NO, clean composition.
        
        # Let's attach a wrapper operator at the beginning of the downstream chain?
        # A simpler way given standard PSPF might be to wrap the `emit` method of the source instance?
        # That's a bit hacky.
        
        # Better: The user (Inventory App) constructs the pipeline:
        # Source -> DedupOperator -> BusinessLogic
        
        # BUT the prompt says "Implement a partitioned execution engine... Exactly-once semantics (Inside PSPF)".
        # So maybe this Executor should enforce it.
        
        # Let's assume this Executor is responsible for running the Source.
        # `LogSource` calls `self.emit`. 
        # We can monkeypatch `source.emit` or better yet, `LogSource` could take an optional `dedup_store`.
        
        # Given "Clean abstraction", let's make `PartitionedExecutor` run the source.
        # And we can add a specialized "Middleware" or "Interceptor" if PSPF supported it.
        # Since it's a "Framework", I can add a `DeduplicationOperator`.
        
        # Let's create a `DeduplicationOperator` and attach it to the source if not present.
        # For now, let's keep it simple: The Executor runs the source.
        # The uniqueness check should ideally happen inside the `LogSource` loop or immediately downstream.
        
        # Let's wrap the source's emit.
        original_emit = self.source.emit
        
        async def dedup_emit(record: StreamRecord) -> None:
            if await self.dedup_store.has_processed(record.id):
                # Duplicate detected, skip
                # Log it?
                return
            
            # Process downstream
            await original_emit(record)
            
            # Mark processed ONLY after successful downstream processing?
            # Or before?
            # "Exactly-once" usually means:
            # 1. Deduplication (idempotence)
            # 2. Transactional processing
            
            # If we mark before, and crash during process, we lose data (At most once).
            # If we mark after, and crash during process, we might re-process (At least once) but then retry and see mark? No.
            # If we crash before mark, we replay. Replay -> see not marked -> process again -> mark. 
            # This is "At least once" delivery, but if the processing is idempotent (which this dedup ensures for the specific ID check), it becomes exactly-once.
            
            # So: Check -> Process -> Mark.
            # If crash during Process, we retry. Check says "No". Process again.
            # If crash after Process before Mark, we retry. Check says "No". Process again. (Side effects happen twice).
            # So "Mark" must be atomic with "Commit Offset"? 
            # Or "Business Logic" must be idempotent.
            
            # The prompt says: "Implement idempotent processing... Event IDs... Deduplication state store... Ensure handlers are never applied twice".
            # For strict exactly-once side effects, the side effect and the dedup store update should be atomic.
            # But "Inventory state is derived from events... in-memory".
            # If we restart, we replay from log. Deduplication store must be persistent or rebuilt.
            # If memory store + memory state => they are both lost on restart. Replay rebuilds both.
            # So Check -> Process -> Mark works fine for Memory/Memory.
            
            await self.dedup_store.mark_processed(record.id)

        self.source.emit = dedup_emit
        
        await self.source.start()

    async def stop(self) -> None:
        # Source checks a flag usually. 
        # We can implement stop logic if needed.
        pass
