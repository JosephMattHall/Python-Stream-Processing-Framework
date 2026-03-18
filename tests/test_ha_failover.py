import pytest
import asyncio
from unittest.mock import AsyncMock
from pspf.cluster.coordinator import ClusterCoordinator

@pytest.mark.asyncio
async def test_coordinator_failover_and_rebalance():
    """
    Simulates a 3-node cluster failover.
    Node 1 (Leader) holds partition '0'.
    Node 1 dies. Node 2 and Node 3 try to acquire. One of them wins.
    """
    # Use a mocked valkey client for all coordinators since we don't have a real redis cluster in CI
    mock_redis = AsyncMock()
    
    # Simple simulated state for a mock Redis
    state = {}
    
    async def mock_set(key, value, nx=False, ex=None):
        if nx and key in state:
            return False
        state[key] = value
        return True
        
    async def mock_get(key):
        return state.get(key)
        
    async def mock_delete(key):
        if key in state:
            del state[key]
            
    mock_redis.set.side_effect = mock_set
    mock_redis.get.side_effect = mock_get
    mock_redis.delete.side_effect = mock_delete
    
    c1 = ClusterCoordinator("redis://mock", "h1", 8001, "node1")
    c2 = ClusterCoordinator("redis://mock", "h2", 8002, "node2")
    c3 = ClusterCoordinator("redis://mock", "h3", 8003, "node3")
    
    # Inject our mock client
    c1._client = mock_redis
    c2._client = mock_redis
    c3._client = mock_redis
    
    # Node 1 acquires partition 0
    acquired = await c1.try_acquire_leadership("0")
    assert acquired is True
    assert "0" in c1._held_partitions
    
    # Node 2 tries, fails
    acquired2 = await c2.try_acquire_leadership("0")
    assert acquired2 is False
    
    # Simulate Node 1 crash (keys expire or are deleted)
    await mock_delete("pspf:partition:0:leader")
    c1._held_partitions.remove("0")
    
    # Node 2 and 3 notice dead node and try to acquire
    # In a real scenario they run in a loop `_maintain_leadership`, here we call manually
    acquired2 = await c2.try_acquire_leadership("0")
    assert acquired2 is True
    assert "0" in c2._held_partitions
    
    # Node 3 tries, fails
    acquired3 = await c3.try_acquire_leadership("0")
    assert acquired3 is False
