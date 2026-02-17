import aiosqlite
import pickle
import pickle
import os
from typing import Any, Optional, Dict
from pspf.state.store import StateStore
from pspf.utils.logging import get_logger

logger = get_logger("SQLiteStateStore")

class SQLiteStateStore(StateStore):
    """
    Persistent state store using SQLite.
    Values are pickled before storage.
    """
    def __init__(self, path: str, table_name: str = "kv_store"):
        self.path = path
        self.table_name = table_name
        self._db: Optional[aiosqlite.Connection] = None

    async def start(self):
        # Ensure directory exists
        dirname = os.path.dirname(self.path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
            
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (key TEXT PRIMARY KEY, value BLOB)"
        )
        await self._db.commit()
        logger.info(f"Opened SQLite State Store at {self.path}")

    async def stop(self):
        if self._db:
            await self._db.close()

    async def get(self, key: str, default: Any = None) -> Any:
        if not self._db: raise RuntimeError("Store not started")
        
        async with self._db.execute(f"SELECT value FROM {self.table_name} WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            if row:
                try:
                    return pickle.loads(row[0])
                except (pickle.UnpicklingError, AttributeError, EOFError, ImportError, IndexError) as e:
                    logger.error(f"CORRUPTION DETECTED: Failed to unpickle value for key '{key}': {e}")
                    return default
            return default

    async def put(self, key: str, value: Any):
        if not self._db: raise RuntimeError("Store not started")
        
        data = pickle.dumps(value)
        await self._db.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
            (key, data)
        )
        # Auto-commit for single puts
        await self._db.commit()

    async def put_batch(self, entries: Dict[str, Any]):
        if not self._db: raise RuntimeError("Store not started")
        
        # Prepare data
        rows = []
        for k, v in entries.items():
            rows.append((k, pickle.dumps(v)))
            
        if not rows: return

        await self._db.executemany(
            f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
            rows
        )
        await self._db.commit()

    async def delete(self, key: str):
        if not self._db: raise RuntimeError("Store not started")
        await self._db.execute(f"DELETE FROM {self.table_name} WHERE key = ?", (key,))
        await self._db.commit()

    async def flush(self):
        if self._db:
            await self._db.commit()
