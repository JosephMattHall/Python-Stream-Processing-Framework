import pytest
import asyncio
from typing import Dict, Any
from pydantic import BaseModel
from pspf.stream import Stream
from pspf.connectors.memory import MemoryBackend

class DummyEvent(BaseModel):
    id: int
    data: str

@pytest.mark.asyncio
async def test_multiple_decorators_concurrent_routing():
    backend = MemoryBackend(stream_key="default", group_name="test-group")
    stream = Stream(backend=backend, schema=DummyEvent)

    received_a = []
    received_b = []

    @stream.subscribe("topic-A", batch_size=2)
    async def handle_a(event: DummyEvent):
        received_a.append(event.id)

    @stream.subscribe("topic-B", batch_size=2)
    async def handle_b(event: DummyEvent):
        received_b.append(event.id)

    # Connect manually for emit
    await stream.backend.connect()

    # Emit some events
    await stream.emit(DummyEvent(id=1, data="a"), topic="topic-A")
    await stream.emit(DummyEvent(id=2, data="b"), topic="topic-B")
    await stream.emit(DummyEvent(id=3, data="a2"), topic="topic-A")

    # Run forever in background
    task = asyncio.create_task(stream.run_forever())

    # Wait for processing
    await asyncio.sleep(0.5)

    # Cancel loop
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert set(received_a) == {1, 3}
    assert set(received_b) == {2}
