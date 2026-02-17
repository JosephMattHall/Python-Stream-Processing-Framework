import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List, Tuple, Dict, Any

from pspf.stream import Stream
from pspf.schema import BaseEvent
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.processor import BatchProcessor

class TestEnterpriseFeatures(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Setup mocks
        self.mock_connector = MagicMock(spec=ValkeyConnector)
        self.mock_client = AsyncMock()
        self.mock_connector.get_client.return_value = self.mock_client
        self.mock_connector.connect = AsyncMock()
        # Configure pipeline mock
        self.mock_pipeline = MagicMock()
        self.mock_pipeline.__aenter__ = AsyncMock(return_value=self.mock_pipeline)
        self.mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        
        self.mock_pipeline.xack = MagicMock()
        self.mock_pipeline.hdel = MagicMock()
        self.mock_pipeline.execute = AsyncMock() 
        
        self.mock_client.pipeline.return_value = self.mock_pipeline
        
        # Helper to create backend with mocked connector
        self.backend = ValkeyStreamBackend(self.mock_connector, "test-stream", "group1", "consumer1")
        self.processor = BatchProcessor(self.backend, max_retries=2)

    async def test_worker_recovery_xautoclaim(self):
        """
        Verify that BatchProcessor calls claim_stuck_messages on startup,
        which calls XAUTOCLAIM.
        """
        # Mock XAUTOCLAIM response: (next_id, messages)
        mock_messages = [
            ("1678888888000-0", {"event_type": "BaseEvent", "payload": "stuck_msg"}),
        ]
        self.mock_client.xautoclaim.return_value = ("0-0", mock_messages)
        self.mock_client.xreadgroup.return_value = [] # Return empty batch so loop exits if we wanted
        
        # Manually invoke recovery
        processed_ids = []
        async def handler(msg_id, data):
            processed_ids.append(msg_id)

        await self.processor._recover_stuck_messages(handler)

        self.mock_client.xautoclaim.assert_called_once()
        self.assertEqual(len(processed_ids), 1)
        self.assertEqual(processed_ids[0], "1678888888000-0")
        
        # Verify ACK
        self.mock_client.pipeline.assert_called() # ack_batch uses pipeline

    async def test_retry_logic_increment(self):
        """
        Verify that processing error increments retry count.
        """
        # Mock Redis Hash methods
        self.mock_client.hincrby.return_value = 1 # 1st failure
        self.mock_client.hget.return_value = 1
        
        msg_id = "1000-0"
        data = {"foo": "bar"}
        error = ValueError("Processing failed")

        await self.processor._handle_processing_error(msg_id, data, error)

        # Should increment retry counter
        self.mock_client.hincrby.assert_called_with(
            self.backend.retry_tracker_key, msg_id, 1
        )
        # Should NOT move to DLQ yet (count 1 <= max_retries 2)
        self.mock_client.xadd.assert_not_called()

    async def test_dlo_move_after_max_retries(self):
        """
        Verify that exceeding max_retries moves message to DLQ and ACKs it.
        """
        # Mock Redis Hash methods
        self.mock_client.hincrby.return_value = 3 # 3rd failure > max_retries 2
        
        msg_id = "1000-0"
        data = {"foo": "bar"}
        error = ValueError("Fatal Error")

        await self.processor._handle_processing_error(msg_id, data, error)

        # Should increment
        self.mock_client.hincrby.assert_called()
        
        # Should move to DLQ
        # Expected DLQ Key: test-stream-dlq
        args, kwargs = self.mock_client.xadd.call_args
        stream_key = args[0]
        payload = args[1]
        
        self.assertEqual(stream_key, "test-stream-dlq")
        self.assertEqual(payload["foo"], "bar")
        self.assertIn("_error", payload)
        self.assertEqual(payload["_error"], "Fatal Error")
        
        # Should ACK original message
        self.mock_client.xack.assert_called_with("test-stream", "group1", msg_id)
        
        # Should cleanup retries
        self.mock_client.hdel.assert_called_with(self.backend.retry_tracker_key, msg_id)

    async def test_stream_composition(self):
        """
        Verify Stream class uses the injected backend correctly.
        """
        # Setup Backend Mock
        self.mock_client.xgroup_create.return_value = True
        self.mock_client.xadd.return_value = "msg-new"
        
        # Init Stream
        async with Stream(self.backend, schema=BaseEvent) as stream:
            # Check Group Creation
            self.mock_client.xgroup_create.assert_called_once()
            
            # Emit
            evt = BaseEvent(event_type="Test")
            await stream.emit(evt)
            self.mock_client.xadd.assert_called()

if __name__ == '__main__':
    unittest.main()
