from typing import Callable, Any, Dict, List, Optional
from pspf.stream import Stream
from pspf.models import StreamRecord

class StreamBuilder:
    """
    Fluent API for building stream processing pipelines.
    
    Example:
        builder = StreamBuilder(stream)
        builder.filter(lambda x: x['val'] > 10).map(lambda x: x.upper()).sink(output_stream)
    """
    def __init__(self, stream: Stream):
        self.stream = stream
        self._ops: List[Callable[[Any], Any]] = []

    def map(self, func: Callable[[Any], Any]) -> "StreamBuilder":
        """Transform each element."""
        self._ops.append(lambda x: func(x))
        return self

    def filter(self, func: Callable[[Any], bool]) -> "StreamBuilder":
        """Filter elements based on a predicate."""
        def _filter(x):
            if func(x):
                return x
            return None
        self._ops.append(_filter)
        return self

    def sink(self, target_stream: Stream) -> None:
        """Execute the pipeline and send results to another stream."""
        
        async def handler(data: Any):
            # Convert to dict for easier manipulation in functional pipeline if needed
            current = data
            if hasattr(current, "model_dump"):
                current = current.model_dump()
            
            for op in self._ops:
                current = op(current)
                if current is None:
                    return # Filtered out
            
            await target_stream.emit(current)

        # Register with the underlying stream
        topic = self.stream.backend.stream_key
        self.stream.subscribe(topic)(handler)

    async def run_forever(self):
        """Shortcut to run the underlying stream processor."""
        await self.stream.run_forever()
