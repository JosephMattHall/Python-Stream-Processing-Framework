from abc import ABC, abstractmethod
from typing import Generic, TYPE_CHECKING, Optional, Callable, Any
from pspf.utils.typing import T
from pspf.operators.core import Operator

if TYPE_CHECKING:
    pass

class Source(Operator[None, T]):
    """Base class for data sources.
    
    Sources do not take input from other operators (T is None),
    but they emit T. Sources can optionally extract event timestamps
    and generate watermarks for event-time processing.
    """
    
    def __init__(self, name: str = "Source"):
        super().__init__(name)
        self.timestamp_extractor: Optional[Callable[[T], float]] = None
        self.last_emitted_watermark: float = float('-inf')
        self.offset_store: Optional['OffsetStore'] = None
        self.pipeline_id: Optional[str] = None
    
    @abstractmethod
    async def start(self) -> None:
        """Start generating data."""
        pass

    async def restore_watermark(self) -> None:
        """Restore the latest watermark from the offset store."""
        if self.offset_store and self.pipeline_id:
            self.last_emitted_watermark = await self.offset_store.get_watermark(self.pipeline_id)
            self.logger.info(f"Restored watermark: {self.last_emitted_watermark}")
    
    async def _process_captured(self, element: None) -> None:
        """Sources do not process input from upstream."""
        pass
    
    async def emit(self, element: T) -> None:
        """Emit an element and optionally a watermark.
        
        If a timestamp extractor is configured, this will extract the event time
        and potentially emit a watermark.
        
        Args:
            element: The element to emit downstream
        """
        # Emit the element itself
        for op in self.downstream:
            await op.process(element)
        
        # If event-time processing is enabled, emit watermarks
        if self.timestamp_extractor:
            event_time = self.timestamp_extractor(element)
            # Simple watermark strategy: watermark = max observed event time
            # In a real production environment, we'd probably want something more robust
            # like watermark = event_time - allowed_lateness to handle out-of-order data.
            if event_time > self.last_emitted_watermark:
                self.last_emitted_watermark = event_time
                self.logger.debug(f"Emitting watermark: {event_time}")
                await self.emit_watermark(event_time)
                
                # Persist watermark
                if self.offset_store and self.pipeline_id:
                    await self.offset_store.commit_watermark(self.pipeline_id, event_time)


class Sink(Operator[T, None]):
    """Base class for data sinks.
    
    Sinks receive elements and perform side effects (write to file, database, etc.)
    but do not emit elements downstream.
    """
    pass
