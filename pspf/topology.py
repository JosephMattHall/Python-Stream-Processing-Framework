from typing import Callable, Any, Dict, List, Optional, Tuple
from pspf.stream import Stream
from pspf.state.store import StateStore
from pspf.processing.windows import Window
from pydantic import BaseModel
from pspf.utils.logging import get_logger

logger = get_logger("Topology")

class Router:
    """
    Branching primitive. Evaluates an event against a series of predicates 
    and routes it to the first matching target topic.
    """
    def __init__(self, stream: Stream):
        self.stream = stream
        self.routes: List[Tuple[Callable[[BaseModel], bool], str]] = []
        self._default_topic: Optional[str] = None

    def add_route(self, predicate: Callable[[BaseModel], bool], target_topic: str) -> "Router":
        """Adds a conditional branch to the router."""
        self.routes.append((predicate, target_topic))
        return self

    def default_route(self, target_topic: str) -> "Router":
        """Sets the fallback topic if no predicates match."""
        self._default_topic = target_topic
        return self

    async def route(self, event: BaseModel) -> Optional[str]:
        """
        Evaluates the routes in order and emits the event.
        
        Returns:
            str: The message_id of the emitted event, or None if dropped.
        """
        for predicate, target_topic in self.routes:
            if predicate(event):
                logger.debug(f"Routing event to {target_topic}")
                return await self.stream.emit(event, topic=target_topic)
                
        if self._default_topic:
            logger.debug(f"Routing event to default topic {self._default_topic}")
            return await self.stream.emit(event, topic=self._default_topic)
            
        return None


class Joiner:
    """
    Co-partitions two streams and joins their records over a defined window 
    using the provided StateStore.
    """
    def __init__(self, primary_stream: Stream, state_store: StateStore):
        """
        Args:
            primary_stream (Stream): The stream context used to emit joined results.
            state_store (StateStore): Persistent storage for buffering windowed events.
        """
        self.stream = primary_stream
        self.state_store = state_store

    async def buffer_event(self, side: str, key: str, timestamp: float, event: BaseModel, window: Window) -> None:
        """
        Buffers an event from one side of the join into the state store.
        """
        windows = window.assign_windows(timestamp)
        for start, end in windows:
            state_key = f"join:{side}:{key}:{start}:{end}"
            
            async with self.state_store.transaction():
                # Fetch existing buffered events for this window
                current_buffer = await self.state_store.get(state_key) or []
                current_buffer.append(event.model_dump(mode='json'))
                
                await self.state_store.put(state_key, current_buffer)

    async def get_buffered_events(self, side: str, key: str, timestamp: float, window: Window) -> List[Dict[str, Any]]:
        """
        Retrieves buffered events from the opposing side that fall into the same window.
        """
        windows = window.assign_windows(timestamp)
        results = []
        for start, end in windows:
            state_key = f"join:{side}:{key}:{start}:{end}"
            buffer = await self.state_store.get(state_key) or []
            results.extend(buffer)
        return results

    async def inner_join(self, left_event: BaseModel, right_event: BaseModel) -> BaseModel:
        """
        Abstract or customizable method to define how to merge two matching events.
        Defaults to a dict merge.
        """
        class JoinedEvent(BaseModel):
            left: Dict[str, Any]
            right: Dict[str, Any]
            
        return JoinedEvent(
            left=left_event.model_dump(mode='json'), 
            right=right_event.model_dump(mode='json')
        )
