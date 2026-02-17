import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import json
from pspf.cluster.coordinator import ClusterCoordinator

class TestClusterCoordinator(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_redis = AsyncMock()
        self.coordinator = ClusterCoordinator("redis://localhost", "localhost", 8001, "node-1")
        # Inject mock client
        self.coordinator._client = self.mock_redis

    async def test_register(self):
        await self.coordinator._register()
        self.mock_redis.set.assert_called_once()
        args = self.mock_redis.set.call_args[0]
        self.assertEqual(args[0], "pspf:nodes:node-1")
        self.assertIn("node-1", args[1])

    async def test_acquire_leadership_success(self):
        # Setup mock to return True (acquired)
        self.mock_redis.set.return_value = True
        
        result = await self.coordinator.try_acquire_leadership("p0")
        self.assertTrue(result)
        self.assertIn("p0", self.coordinator._held_partitions)

    async def test_acquire_leadership_failure(self):
        # Setup mock to return False (not acquired)
        self.mock_redis.set.return_value = False
        # Setup get to return someone else
        self.mock_redis.get.return_value = "node-2"
        
        result = await self.coordinator.try_acquire_leadership("p0")
        self.assertFalse(result)
        self.assertNotIn("p0", self.coordinator._held_partitions)

    async def test_acquire_leadership_reacquire(self):
        # Setup mock to return False (already exists)
        self.mock_redis.set.return_value = False
        # But WE are the owner
        self.mock_redis.get.return_value = "node-1"
        
        result = await self.coordinator.try_acquire_leadership("p0")
        self.assertTrue(result)
        self.assertIn("p0", self.coordinator._held_partitions)

if __name__ == '__main__':
    unittest.main()
