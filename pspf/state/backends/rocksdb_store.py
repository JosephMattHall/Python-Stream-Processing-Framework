import pickle
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional, Dict
from pspf.state.store import StateStore
from pspf.utils.logging import get_logger

logger = get_logger("RocksDBStateStore")

class RocksDBStateStore(StateStore):
    """
    Persistent state store using RocksDB.
    Values are pickled before storage.
    Operations are wrapped in a thread pool to avoid blocking the event loop.
    """
    def __init__(self, path: str, read_only: bool = False):
        self.path = path
        self.read_only = read_only
        self._db: Any = None
        self._executor = ThreadPoolExecutor(max_workers=1) # Sequential access for safety
        self._checkpoint_prefix = b"__pspf_offset__:"

    async def start(self) -> None:
        try:
            import rocksdb # type: ignore
        except ImportError:
            logger.error("RocksDB not installed. Please install 'rocksdb-python'.")
            raise RuntimeError("RocksDB is not installed on this system.")

        # Ensure directory exists
        if not os.path.exists(self.path):
            os.makedirs(self.path)
            
        loop = asyncio.get_running_loop()
        
        def _open() -> Any:
            opts = rocksdb.Options()
            opts.create_if_missing = True
            return rocksdb.DB(self.path, opts, read_only=self.read_only)
            
        self._db = await loop.run_in_executor(self._executor, _open)
        logger.info(f"Opened RocksDB State Store at {self.path}")

    async def stop(self) -> None:
        if self._db:
            self._db = None
            self._executor.shutdown()
            logger.info("Closed RocksDB State Store")

    async def get(self, key: str, default: Any = None) -> Any:
        import time
        if not self._db: raise RuntimeError("Store not started")
        
        loop = asyncio.get_running_loop()
        val = await loop.run_in_executor(self._executor, lambda: self._db.get(key.encode()))
        
        if val is not None:
            try:
                obj = pickle.loads(val)
                if isinstance(obj, dict) and "_v" in obj and "_exp" in obj:
                    if obj["_exp"] is not None and time.time() > obj["_exp"]:
                        await self.delete(key) # Lazy eviction
                        return default
                    return obj["_v"]
                return obj # Backwards compatibility
            except Exception as e:
                logger.error(f"Failed to unpickle key '{key}': {e}")
                return default
        return default

    async def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        import time
        if not self._db: raise RuntimeError("Store not started")
        
        expires_at = time.time() + ttl_seconds if ttl_seconds is not None else None
        wrapped = {"_v": value, "_exp": expires_at}
        data = pickle.dumps(wrapped)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, lambda: self._db.put(key.encode(), data))

    async def put_batch(self, entries: Dict[str, Any]) -> None:
        if not self._db: raise RuntimeError("Store not started")
        
        import rocksdb
        def _batch() -> None:
            batch = rocksdb.WriteBatch()
            for k, v in entries.items():
                batch.put(k.encode(), pickle.dumps(v))
            self._db.write(batch)
            
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, _batch)

    async def delete(self, key: str) -> None:
        if not self._db: raise RuntimeError("Store not started")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, lambda: self._db.delete(key.encode()))

    async def flush(self) -> None:
        pass

    async def checkpoint(self, stream_id: str, group_id: str, offset: str) -> None:
        if not self._db: raise RuntimeError("Store not started")
        
        key = self._checkpoint_prefix + f"{stream_id}:{group_id}".encode()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, lambda: self._db.put(key, offset.encode()))
        
    async def get_checkpoint(self, stream_id: str, group_id: str) -> Optional[str]:
        if not self._db: raise RuntimeError("Store not started")
        
        key = self._checkpoint_prefix + f"{stream_id}:{group_id}".encode()
        loop = asyncio.get_running_loop()
        val = await loop.run_in_executor(self._executor, lambda: self._db.get(key))
        return val.decode() if val else None
