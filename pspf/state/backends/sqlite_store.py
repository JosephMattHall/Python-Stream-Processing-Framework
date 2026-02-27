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
    def __init__(self, path: str, table_name: str = "kv_store") -> None:
        self.path = path
        self.table_name = table_name
        self._db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        # Ensure directory exists
        dirname = os.path.dirname(self.path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
            
        self._db = await aiosqlite.connect(self.path)
        # Main KV table
        await self._db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (key TEXT PRIMARY KEY, value BLOB)"
        )
        # Offset tracking table
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS pspf_offsets (stream_id TEXT, group_id TEXT, offset TEXT, PRIMARY KEY (stream_id, group_id))"
        )
        await self._db.commit()
        logger.info(f"Opened SQLite State Store at {self.path}")

    async def stop(self) -> None:
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

    async def put(self, key: str, value: Any) -> None:
        if not self._db: raise RuntimeError("Store not started")
        
        data = pickle.dumps(value)
        await self._db.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
            (key, data)
        )
        # Auto-commit for single puts
        await self._db.commit()

    async def put_batch(self, entries: Dict[str, Any]) -> None:
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

    async def delete(self, key: str) -> None:
        if not self._db: raise RuntimeError("Store not started")
        await self._db.execute(f"DELETE FROM {self.table_name} WHERE key = ?", (key,))
        await self._db.commit()

    async def flush(self) -> None:
        if self._db:
            await self._db.commit()

    async def checkpoint(self, stream_id: str, group_id: str, offset: str) -> None:
        """
        Atomically store the offset. 
        Note: If multiple puts were done without commit, this commit will finalize them too.
        """
        if not self._db: raise RuntimeError("Store not started")
        
        await self._db.execute(
            "INSERT OR REPLACE INTO pspf_offsets (stream_id, group_id, offset) VALUES (?, ?, ?)",
            (stream_id, group_id, offset)
        )
        await self._db.commit()

    async def get_checkpoint(self, stream_id: str, group_id: str) -> Optional[str]:
        if not self._db: raise RuntimeError("Store not started")
        
        async with self._db.execute(
            "SELECT offset FROM pspf_offsets WHERE stream_id = ? AND group_id = ?",
            (stream_id, group_id)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
