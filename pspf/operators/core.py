import asyncio
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Generic, TypeVar, TYPE_CHECKING, Dict
from pspf.utils.typing import T, U, MapFunction, FilterFunction, KeySelector, ReduceFunction
from pspf.utils.logging import get_logger
from pspf.utils.metrics import MetricsManager

if TYPE_CHECKING:
    from pspf.connectors.base import Source, Sink

class Operator(ABC, Generic[T, U]):
    """Base class for all stream processing operators.
    
    Operators form the building blocks of a pipeline. They receive elements,
    process them, and emit results downstream. All operators support:
    - Asynchronous processing with backpressure
    - State snapshotting for checkpoints
    - Watermark propagation for event-time processing
    """

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.downstream: List['Operator'] = []
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=100)  # backpressure buffer
        self._work_task: Optional[asyncio.Task] = None
        self.logger = get_logger(self.name)

    def connect(self, operator: 'Operator') -> None:
        """Connect this operator to a downstream operator."""
        self.downstream.append(operator)

    async def emit(self, element: U) -> None:
        """Emit an element to all downstream operators.
        
        Args:
            element: The processed element to send downstream
        """
        for op in self.downstream:
            await op.process(element)

    async def process(self, element: T) -> None:
        """Receive an element. This will block if the internal buffer is full (backpressure).
        
        Args:
            element: The input element to process
        """
        await self._queue.put(element)
        if self._work_task is None or self._work_task.done():
            self._work_task = asyncio.create_task(self._worker_loop())

    async def process_watermark(self, timestamp: float) -> None:
        """Receive a watermark. Watermarks bypass the regular queue to ensure timely propagation.
        
        Args:
            timestamp: The watermark timestamp in event time
        """
        await self.on_watermark(timestamp)
        await self.emit_watermark(timestamp)

    async def on_watermark(self, timestamp: float) -> None:
        """Hook for operators to respond to a watermark (e.g., trigger windows).
        
        Args:
            timestamp: The watermark timestamp in event time
        """
        pass

    async def emit_watermark(self, timestamp: float) -> None:
        """Propagate a watermark downstream.
        
        Args:
            timestamp: The watermark timestamp to propagate
        """
        for op in self.downstream:
            await op.process_watermark(timestamp)

    async def _worker_loop(self) -> None:
        """Process elements from the queue until it's empty.
        
        This cooperative async loop processes buffered elements and provides
        natural backpressure when the queue fills.
        """
        while not self._queue.empty():
            element = await self._queue.get()
            await self._process_captured(element)
            self._queue.task_done()

    @abstractmethod
    async def _process_captured(self, element: T) -> None:
        """Process a single element. Must be implemented by subclasses.
        
        Args:
            element: The element to process
        """
        pass

    def snapshot_state(self) -> Dict[str, Any]:
        """Capture the current state for checkpointing.
        
        Default implementation returns empty state. Override in stateful operators.
        
        Returns:
            Dictionary containing the operator's state
        """
        return {}

    def restore_state(self, state: Dict[str, Any]) -> None:
        """Restore operator state from a checkpoint.
        
        Default implementation does nothing. Override in stateful operators.
        
        Args:
            state: Dictionary containing the operator's state
        """
        pass


class Map(Operator[T, U]):
    """Applies a transformation function to each element."""

    def __init__(self, func: MapFunction[T, U], name: str = "Map"):
        super().__init__(name)
        self.func = func

    async def _process_captured(self, element: T) -> None:
        result = self.func(element)
        await self.emit(result)


class Filter(Operator[T, T]):
    """Filters elements based on a predicate."""

    def __init__(self, predicate: FilterFunction[T], name: str = "Filter"):
        super().__init__(name)
        self.predicate = predicate

    async def _process_captured(self, element: T) -> None:
        if self.predicate(element):
            await self.emit(element)


class KeyBy(Operator[T, tuple[Any, T]]):
    """Partitions the stream by key."""

    def __init__(self, key_selector: KeySelector[T], name: str = "KeyBy"):
        super().__init__(name)
        self.key_selector = key_selector

    async def _process_captured(self, element: T) -> None:
        key = self.key_selector(element)
        await self.emit((key, element))


class Reduce(Operator[tuple[Any, T], tuple[Any, T]]):
    """Reduces elements by key."""

    def __init__(self, reducer: ReduceFunction[T], name: str = "Reduce"):
        super().__init__(name)
        self.reducer = reducer
        # Need to import locally to avoid circular import issues if state.py imports core
        from pspf.operators.state import KeyedState
        self.state = KeyedState[T]()

    async def _process_captured(self, element: tuple[Any, T]) -> None:
        key, value = element
        current_state = self.state.get(key)

        if current_state is None:
            new_state = value
        else:
            new_state = self.reducer(current_state, value)

        self.state.set(key, new_state)
        await self.emit((key, new_state))

    def snapshot_state(self) -> Dict[str, Any]:
        return {"keyed_state": self.state.snapshot()}

    def restore_state(self, state: Dict[str, Any]) -> None:
        if "keyed_state" in state:
            self.state.restore(state["keyed_state"])


class Pipeline:
    """Fluent API for building stream processing pipelines."""

    def __init__(self) -> None:
        from pspf.connectors.base import Source # Lazy import
        self.sources: List['Source'] = []
        self._current_operator: Operator | None = None
        self.timestamp_extractor: Optional[Any] = None

    def assign_timestamps(self, extractor: Any) -> 'Pipeline':
        """Assign an event time extractor for this pipeline."""
        self.timestamp_extractor = extractor
        return self

    def read_from(self, source: 'Source') -> 'Pipeline':
        """Start a pipeline from a source."""
        self.sources.append(source)
        self._current_operator = source
        return self

    def map(self, func: MapFunction) -> 'Pipeline':
        """Apply a map transformation."""
        op = Map(func)
        self._add_operator(op)
        return self

    def filter(self, predicate: FilterFunction) -> 'Pipeline':
        """Apply a filter transformation."""
        op = Filter(predicate)
        self._add_operator(op)
        return self

    def key_by(self, key_selector: KeySelector) -> 'Pipeline':
        """Partition stream by key."""
        op = KeyBy(key_selector)
        self._add_operator(op)
        return self
        
    def reduce(self, reducer: ReduceFunction) -> 'Pipeline':
        """Reduce elements stream by key."""
        op = Reduce(reducer)
        self._add_operator(op)
        return self

    def window(self, size: int) -> 'Pipeline':
        """Apply a windowing operator. Uses event time if assign_timestamps was called."""
        from pspf.operators.windowing import Window, SlidingEventTimeWindow
        if self.timestamp_extractor:
            op = SlidingEventTimeWindow(float(size))
        else:
            op = Window(size)
        self._add_operator(op)
        return self


    def write_to(self, sink: 'Sink') -> 'Pipeline':
        """Connect the pipeline to a sink."""
        self._add_operator(sink)
        return self

    def _add_operator(self, op: Operator) -> None:
        if self._current_operator is None:
            raise ValueError("Pipeline must start with a source (call read_from first).")
        self._current_operator.connect(op)
        self._current_operator = op

    def run(self) -> None:
        """Run the pipeline using the default runner."""
        if self.timestamp_extractor:
            for s in self.sources:
                s.timestamp_extractor = self.timestamp_extractor
        
        from pspf.runtime.runner import Runner
        runner = Runner()
        runner.run(self)

