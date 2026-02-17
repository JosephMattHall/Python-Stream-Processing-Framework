import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
from pspf.log.replicated_log import ReplicatedLog
from pspf.models import StreamRecord

class TestReplicatedLog(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_local = AsyncMock()
        self.mock_coordinator = AsyncMock()
        self.mock_http = AsyncMock()
        
        self.log = ReplicatedLog(self.mock_local, self.mock_coordinator)
        self.log._http_client = self.mock_http # Inject mock http
        
        # fix: _get_partition is synchronous in LocalLog, but AsyncMock makes children async by default
        self.mock_local._get_partition = MagicMock(return_value=0)

    async def test_append_as_leader(self):
        # Setup: We are leader
        self.mock_coordinator.try_acquire_leadership.return_value = True
        self.mock_coordinator.get_other_nodes.return_value = [{"id": "node-2", "host": "h2", "port": 8002}]
        
        # Setup: HTTP success
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        self.mock_http.post.return_value = mock_resp

        record = StreamRecord(id="1", key="k1", value={"v": 1}, timestamp=datetime.now(), topic="t1")
        
        await self.log.append(record)
        
        # Verify Local Write
        self.mock_local.append.assert_called_once_with(record)
        
        # Verify Replication Call
        self.mock_http.post.assert_called_once()
        call_args = self.mock_http.post.call_args
        self.assertEqual(call_args[0][0], "http://h2:8002/internal/replicate")

    async def test_append_not_leader(self):
        # Setup: We are NOT leader
        self.mock_coordinator.try_acquire_leadership.return_value = False
        
        record = StreamRecord(id="1", key="k1", value={"v": 1}, timestamp=datetime.now(), topic="t1")
        
        with self.assertRaises(Exception) as cm:
            await self.log.append(record)
        
        self.assertIn("Not leader", str(cm.exception))
        self.mock_local.append.assert_not_called()

if __name__ == '__main__':
    unittest.main()
