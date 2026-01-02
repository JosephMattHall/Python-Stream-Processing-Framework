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
    
    @abstractmethod
    async def start(self) -> None:
        """Start generating data."""
        pass
    
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
            # In production, you'd typically have watermark = event_time - allowed_lateness
            if event_time > self.last_emitted_watermark:
                self.last_emitted_watermark = event_time
                self.logger.debug(f"Emitting watermark: {event_time}")
                await self.emit_watermark(event_time)


class Sink(Operator[T, None]):
    """Base class for data sinks.
    
    Sinks receive elements and perform side effects (write to file, database, etc.)
    but do not emit elements downstream.
    """
    pass
