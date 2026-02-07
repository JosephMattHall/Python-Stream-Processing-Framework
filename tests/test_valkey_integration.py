import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from pspf.stream import Stream
from pspf.schema import BaseEvent
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend

class TestStreamArchitecture(unittest.IsolatedAsyncioTestCase):

    async def test_stream_composition_and_flow(self):
        """
        Verify that Stream composes Backend and Processor correctly,
        and data flows from mocked backend -> processor -> handler -> ack.
        """
        # Mock the entire ValkeyConnector and Client
        mock_connector = MagicMock(spec=ValkeyConnector)
        mock_client = AsyncMock()
        mock_connector.get_client.return_value = mock_client
        mock_connector.connect = AsyncMock()
        mock_connector.close = AsyncMock()
        
        # Mock XREADGROUP response: List[Tuple[str, Dict[str, Any]]]
        # Structure: [(msg_id, data_dict), ...]
        mock_messages = [
            ("1678888888000-0", {"event_type": "BaseEvent", "payload": "test1"}),
            ("1678888888000-1", {"event_type": "BaseEvent", "payload": "test2"}),
        ]
        mock_client.xreadgroup.return_value = [["stream_key", mock_messages]]
        mock_client.xgroup_create = AsyncMock()
        mock_client.xack = AsyncMock()

        # Mock XADD
        mock_client.xadd.return_value = "1678888888000-2"

        # Mock XAUTOCLAIM
        mock_client.xautoclaim.return_value = ("0-0", [])

        # Patch the ValkeyConnector constructor inside Stream to return our mock
        with patch('pspf.stream.ValkeyConnector', return_value=mock_connector):
            
            async with Stream("test-stream", "group1", "consumer1") as stream:
                
                # 1. Test Emit
                evt = BaseEvent(event_type="TestEvent")
                msg_id = await stream.emit(evt)
                self.assertEqual(msg_id, "1678888888000-2")
                mock_client.xadd.assert_called_once()
                
                # 2. Test Processing Loop (Run one iteration manually or via small hack)
                # We'll inject a "run once" behavior into the processor or just call the handler manually
                # to verify value-add logic, but let's try to run the processor for a split second.
                
                processed_events = []
                async def handler(event):
                    processed_events.append(event)
                
                # We can't easily wait for "one batch" in the infinite loop without expose internals.
                # Instead, let's cancel the loop after a short delay.
                task = asyncio.create_task(stream.run(handler, batch_size=2))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                
                # Verify Handler was called
                self.assertEqual(len(processed_events), 2)
                self.assertIsInstance(processed_events[0], BaseEvent)
                self.assertEqual(processed_events[0].offset, "1678888888000-0")
                
                # Verify ACK was called
                mock_client.xack.assert_called()
                # Check arguments: xack(stream, group, id1, id2)
                call_args = mock_client.xack.call_args
                self.assertEqual(call_args[0][0], "test-stream")
                self.assertEqual(call_args[0][1], "group1")
                self.assertIn("1678888888000-0", call_args[0])

if __name__ == '__main__':
    unittest.main()
