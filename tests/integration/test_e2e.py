import asyncio
import unittest
import os
import uuid
import time
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.processor import BatchProcessor
from pspf.state.backends.sqlite import SQLiteStateStore
from pspf.context import Context

class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # check if valkey is available
        self.host = os.getenv("VALKEY_HOST", "localhost")
        self.port = int(os.getenv("VALKEY_PORT", 6379))
        self.password = os.getenv("VALKEY_PASSWORD", None)
        
        self.connector = ValkeyConnector(host=self.host, port=self.port, password=self.password)
        try:
            await self.connector.connect()
        except OSError:
            self.skipTest("Valkey not available at localhost:6379")

        self.stream_key = f"e2e-stream-{uuid.uuid4()}"
        self.group_name = "e2e-group"
        self.consumer_name = f"e2e-consumer-{uuid.uuid4()}"
        
        self.backend = ValkeyStreamBackend(self.connector, self.stream_key, self.group_name, self.consumer_name)
        await self.backend.ensure_group_exists()
        
        # Cleanup state
        if os.path.exists("data/test_state/e2e.db"):
            os.remove("data/test_state/e2e.db")
        self.store = SQLiteStateStore("data/test_state/e2e.db")
        await self.store.start()

        self.processor = BatchProcessor(self.backend, state_store=self.store)

    async def asyncTearDown(self):
        await self.processor.shutdown() # stops admin, metrics
        await self.store.stop()
        
        # Cleanup Stream in Valkey
        client = self.connector.get_client()
        await client.delete(self.stream_key)
        await client.delete(f"{self.stream_key}-dlq")
        await client.delete(f"pspf:retries:{self.group_name}:{self.stream_key}")
        
        await self.connector.close()
        if os.path.exists("data/test_state/e2e.db"):
            os.remove("data/test_state/e2e.db")

    async def test_full_pipeline(self):
        """
        Produce -> Process (Stateful) -> Validate
        """
        # 1. Produce messages
        await self.backend.add_event({"key": "foo", "val": 1})
        await self.backend.add_event({"key": "foo", "val": 2})
        await self.backend.add_event({"key": "bar", "val": 5})
        
        # 2. Define Handler
        # Counts sum of 'val' per 'key'
        processed_count = 0
        
        async def handler(msg_id, data, ctx: Context):
            nonlocal processed_count
            key = data.get("key")
            val = data.get("val")
            
            curr = await ctx.state.get(key, 0)
            await ctx.state.put(key, curr + val)
            processed_count += 1

        # 3. Run Loop in background
        task = asyncio.create_task(self.processor.run_loop(handler, poll_interval=0.1))
        
        # Wait for processing
        start = time.time()
        while processed_count < 3:
            if time.time() - start > 5:
                break
            await asyncio.sleep(0.1)
            
        # Stop processor
        await self.processor.shutdown()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # 4. Verify Context
        self.assertEqual(processed_count, 3)
        
        # Store is closed by processor shutdown. Re-open to verify persistence.
        await self.store.start()
        self.assertEqual(await self.store.get("foo"), 3) # 1 + 2
        self.assertEqual(await self.store.get("bar"), 5) # 5

if __name__ == '__main__':
    unittest.main()
