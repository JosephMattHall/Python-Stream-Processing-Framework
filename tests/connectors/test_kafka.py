import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from pspf.connectors.kafka import KafkaStreamBackend
from pspf.state.backends.memory_store import InMemoryStateStore

@pytest.mark.asyncio
async def test_kafka_retry_durability_with_state_store():
    # Setup
    state_store = InMemoryStateStore()
    await state_store.start()
    
    # Mock AIOKafka modules to avoid actual connection
    with patch("aiokafka.AIOKafkaProducer", autospec=True), \
         patch("aiokafka.AIOKafkaConsumer", autospec=True):
        
        backend = KafkaStreamBackend(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            group_id="test_group",
            client_id="test_client",
            state_store=state_store
        )
        
        # 1. Test Increment
        msg_id = "0-100"
        count = await backend.increment_retry_count(msg_id)
        assert count == 1
        
        # Verify in StateStore
        retry_key = f"pspf:retries:test_group:test_topic:{msg_id}"
        val = await state_store.get(retry_key)
        assert val == 1
        
        # Increment again
        count = await backend.increment_retry_count(msg_id)
        assert count == 2
        assert await state_store.get(retry_key) == 2
        
        # 2. Test ACK clears state
        # Mock consumer for commit
        backend.consumer = AsyncMock()
        await backend.ack_batch([msg_id])
        
        assert await state_store.get(retry_key) is None
        backend.consumer.commit.assert_called_once()

@pytest.mark.asyncio
async def test_kafka_dlq_clears_retry_state():
    state_store = InMemoryStateStore()
    await state_store.start()
    
    with patch("aiokafka.AIOKafkaProducer", autospec=True), \
         patch("aiokafka.AIOKafkaConsumer", autospec=True):
        
        backend = KafkaStreamBackend(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            group_id="test_group",
            client_id="test_client",
            state_store=state_store
        )
        backend.producer = AsyncMock()
        
        msg_id = "0-200"
        await backend.increment_retry_count(msg_id)
        assert await state_store.get(f"pspf:retries:test_group:test_topic:{msg_id}") == 1
        
        # Move to DLQ
        await backend.move_to_dlq(msg_id, {"data": "foo"}, "error")
        
        # Verify cleared
        assert await state_store.get(f"pspf:retries:test_group:test_topic:{msg_id}") is None
        backend.producer.send_and_wait.assert_called_once()

@pytest.mark.asyncio
async def test_kafka_clone_with_topic_preserves_state_store():
    state_store = InMemoryStateStore()
    
    backend = KafkaStreamBackend(
        bootstrap_servers="localhost:9092",
        topic="topic_a",
        group_id="group",
        client_id="client",
        state_store=state_store
    )
    
    cloned = backend.clone_with_topic("topic_b")
    assert cloned.topic == "topic_b"
    assert cloned.state_store == state_store
    assert cloned.retry_tracker_prefix == "pspf:retries:group:topic_b:"
