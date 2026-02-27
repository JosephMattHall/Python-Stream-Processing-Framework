import asyncio
import pytest
import time
from datetime import datetime, timezone
from pydantic import BaseModel
from pspf.stream import Stream
from pspf.connectors.memory import MemoryBackend
from pspf.state.backends.memory_store import InMemoryStateStore
from pspf.processing.windows import TumblingWindow

class EventSchema(BaseModel):
    key: str
    user_id: str
    amount: float
    timestamp: float

@pytest.fixture
def temp_dir(tmp_path):
    yield str(tmp_path)

@pytest.mark.asyncio
async def test_tumbling_window_aggregation(temp_dir):
    """
    Test windowed aggregation using TumblingWindow and MemoryBackend
    """
    backend = MemoryBackend(stream_key="test_stream", group_name="test_group")
    
    # We use an in-memory StateStore
    store = InMemoryStateStore()
    
    stream = Stream[EventSchema](backend=backend, schema=EventSchema, state_store=store)
    
    async with stream:
        # Pre-seed some data into the log
        # MemoryBackend add_event takes a dict and doesn't natively support injecting an exact StreamRecord
        # But our processing loop pulls data and evaluates the schema and the 'timestamp' field.
        # We will structure the data to have a timestamp field.
        
        # Window 0-10s
        await backend.add_event({"key": "user_1", "user_id": "user_1", "amount": 10.0, "timestamp": 5.0})
        await backend.add_event({"key": "user_1", "user_id": "user_1", "amount": 5.0, "timestamp": 8.0})
        
        # Window 10-20s
        await backend.add_event({"key": "user_1", "user_id": "user_1", "amount": 20.0, "timestamp": 15.0})
        
        # Different user, Window 0-10s
        await backend.add_event({"key": "user_2", "user_id": "user_2", "amount": 100.0, "timestamp": 6.0})
        
        # Define the aggregator
        async def sum_amount(event: EventSchema, current_state: float) -> float:
            if current_state is None:
                return event.amount
            return current_state + event.amount
            
        # Run aggregation loop in background
        task = asyncio.create_task(
            stream.aggregate(
                window=TumblingWindow(size_ms=10000), 
                handler=sum_amount,
                batch_size=10
            )
        )
        
        # Give it a second to process the log
        await asyncio.sleep(1.0)
        
        # Cancel the loop
        task.cancel()
        try:
             await task
        except asyncio.CancelledError:
             pass
             
        # Check the state store
        # Expected keys:
        # "test_stream:user_1:0.0:10.0" -> 15.0
        # "test_stream:user_1:10.0:20.0" -> 20.0
        # "test_stream:user_2:0.0:10.0" -> 100.0
        
        print("STATE STORE DATA:", store._data)
        val1 = await store.get("test_stream:user_1:0.0:10.0")
        assert val1 == 15.0
        
        val2 = await store.get("test_stream:user_1:10.0:20.0")
        assert val2 == 20.0
        
        val3 = await store.get("test_stream:user_2:0.0:10.0")
        assert val3 == 100.0

@pytest.mark.asyncio
async def test_state_store_lifecycle(temp_dir):
    """
    Test that the context manager calls start() and stop() on the state store.
    """
    backend = MemoryBackend(stream_key="test_stream", group_name="test_group")
    
    class MockStore(InMemoryStateStore):
        def __init__(self):
            super().__init__()
            self.started = False
            self.stopped = False
            
        async def start(self):
            self.started = True
            
        async def stop(self):
            self.stopped = True
            
    store = MockStore()
    stream = Stream(backend=backend, state_store=store)
    
    assert not store.started
    assert not store.stopped
    
    async with stream:
        assert store.started
        assert not store.stopped
        
    assert store.stopped
