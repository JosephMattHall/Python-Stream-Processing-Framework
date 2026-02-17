import asyncio
import unittest
import os
import shutil
from pspf.state.backends.sqlite import SQLiteStateStore

class TestSQLiteStateStore(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = "data/test_state"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.store = SQLiteStateStore(path=self.db_path)
        await self.store.start()

    async def asyncTearDown(self):
        await self.store.stop()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    async def test_put_get(self):
        await self.store.put("key1", "value1")
        val = await self.store.get("key1")
        self.assertEqual(val, "value1")
        
        # Test default
        val = await self.store.get("missing", "default")
        self.assertEqual(val, "default")

    async def test_put_batch(self):
        entries = {
            "k1": "v1",
            "k2": "v2",
            "k3": "v3"
        }
        await self.store.put_batch(entries)
        
        self.assertEqual(await self.store.get("k1"), "v1")
        self.assertEqual(await self.store.get("k2"), "v2")
        self.assertEqual(await self.store.get("k3"), "v3")

    async def test_complex_types(self):
        data = {"nested": [1, 2, 3], "foo": "bar"}
        await self.store.put("complex", data)
        
        val = await self.store.get("complex")
        self.assertEqual(val, data)

    async def test_corruption(self):
        # Manually insert garbage data
        await self.store._db.execute(
            f"INSERT INTO {self.store.table_name} (key, value) VALUES (?, ?)",
            ("corrupt_key", b"not_a_pickle")
        )
        await self.store._db.commit()
        
        # Should return default and log error
        val = await self.store.get("corrupt_key", "default_val")
        self.assertEqual(val, "default_val")

if __name__ == '__main__':
    unittest.main()
