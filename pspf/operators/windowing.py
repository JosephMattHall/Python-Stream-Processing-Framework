import asyncio
from typing import List, TypeVar, Dict, Any, Optional
from pspf.operators.core import Operator
from pspf.utils.typing import T

class Window(Operator[T, List[T]]):
    """Collects elements into windows (simplified count window for now)."""
    
    def __init__(self, size: int, name: str = "Window"):
        super().__init__(name)
        self.size = size
        self.buffer: List[T] = []

    async def _process_captured(self, element: T) -> None:
        self.buffer.append(element)
        if len(self.buffer) >= self.size:
            window_data = list(self.buffer)
            self.buffer.clear()
            await self.emit(window_data)

    def snapshot_state(self) -> Dict[str, Any]:
        return {"buffer": self.buffer.copy()}

    def restore_state(self, state: Dict[str, Any]) -> None:
        if "buffer" in state:
            self.buffer = state["buffer"].copy()


class SlidingTimeWindow(Operator[T, List[T]]):
    """Simple processing-time sliding window."""

    def __init__(self, size_seconds: float, name: str = "SlidingTimeWindow"):
        super().__init__(name)
        self.size_seconds = size_seconds
        self.buffer: List[T] = []
        self._window_task: Optional[asyncio.Task] = None

    async def _process_captured(self, element: T) -> None:
        self.buffer.append(element)
        if self._window_task is None or self._window_task.done():
            self._window_task = asyncio.create_task(self._wait_and_emit())

    async def _wait_and_emit(self) -> None:
        await asyncio.sleep(self.size_seconds)
        if self.buffer:
            window_data = list(self.buffer)
            self.buffer.clear()
            await self.emit(window_data)

    def snapshot_state(self) -> Dict[str, Any]:
        return {"buffer": self.buffer.copy()}

    def restore_state(self, state: Dict[str, Any]) -> None:
        if "buffer" in state:
            self.buffer = state["buffer"].copy()


class SlidingEventTimeWindow(Operator[tuple, List[tuple]]):
    """Event-time window operator.
    
    Creates fixed windows based on event timestamps. Windows are triggered
    by watermarks, which signal that no more events with earlier timestamps
    will arrive.
    
    Expected input: (timestamp, value) tuples
    Output: List of (timestamp, value) tuples in the window
    """

    def __init__(self, window_size: float, name: str = "SlidingEventTimeWindow"):
        super().__init__(name)
        self.window_size = window_size
        self.buffer: List[tuple] = []
        self.current_watermark: float = float('-inf')

    async def _process_captured(self, element: tuple) -> None:
        """Buffer elements as they arrive."""
        self.buffer.append(element)
        self.logger.debug(f"Buffered element: {element}, buffer size: {len(self.buffer)}")

    async def on_watermark(self, timestamp: float) -> None:
        """When watermark advances, emit complete windows.
        
        Args:
            timestamp: The new watermark timestamp
        """
        self.logger.debug(f"Received watermark: {timestamp}, current: {self.current_watermark}")
        
        if timestamp <= self.current_watermark:
            return  # Watermark must advance
        
        old_watermark = self.current_watermark
        self.current_watermark = timestamp
        
        # Determine which windows are now complete
        # A window [start, end) is complete when watermark >= end
        # We use tumbling windows here: [0, size), [size, 2*size), ...
        
        if len(self.buffer) == 0:
            return
        
        # Find the earliest and latest event times
        event_times = [elem[0] for elem in self.buffer]
        min_time = min(event_times)
        
        # Calculate window boundaries
        first_window_start = (min_time // self.window_size) * self.window_size
        
        # Emit all complete windows
        window_start = first_window_start
        while window_start + self.window_size <= timestamp:
            window_end = window_start + self.window_size
            
            # Collect elements in this window
            window_elements = [
                elem for elem in self.buffer
                if window_start <= elem[0] < window_end
            ]
            
            if window_elements:
                self.logger.debug(f"Emitting window [{window_start}, {window_end}): {len(window_elements)} elements")
                await self.emit(window_elements)
                
                # Remove emitted elements from buffer
                self.buffer = [elem for elem in self.buffer if elem[0] >= window_end]
            
            window_start = window_end

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "buffer": self.buffer.copy(),
            "current_watermark": self.current_watermark
        }

    def restore_state(self, state: Dict[str, Any]) -> None:
        if "buffer" in state:
            self.buffer = state["buffer"].copy()
        if "current_watermark" in state:
            self.current_watermark = state["current_watermark"]

