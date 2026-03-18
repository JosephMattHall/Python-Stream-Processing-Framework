import json
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional, Dict
from pspf.state.store import StateStore
from pspf.utils.logging import get_logger

logger = get_logger("RocksDBStateStore")

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

def serialize_state(value: Any) -> bytes:
    """Serializes state using msgpack if available, otherwise json."""
    if HAS_MSGPACK:
        try:
            return msgpack.packb(value, use_bin_type=True)
        except Exception as e:
            logger.warning(f"msgpack serialization failed: {e}. Falling back to JSON.")
    
    # Fallback or if msgpack not available
    return json.dumps(value).encode("utf-8")

def deserialize_state(data: bytes, key: str, default: Any) -> Any:
    """Deserializes state, attempting msgpack first, then json."""
    if HAS_MSGPACK:
        try:
            return msgpack.unpackb(data, raw=False)
        except Exception:
            pass # Fallthrough to JSON
            
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        logger.error(f"CORRUPTION DETECTED: Failed to deserialize value for key '{key}': {e}")
        return default

class RocksDBStateStore(StateStore):
    """
    Persistent state store using RocksDB.
    Values are stored via msgpack (preferred) or json.
    Operations are wrapped in a thread pool to avoid blocking the event loop.
    """
    def __init__(self, path: str, read_only: bool = False, options: Optional[Dict[str, Any]] = None):
        self.path = path
        self.read_only = read_only
        self.options = options or {}
        self._db: Any = None
        self._executor = ThreadPoolExecutor(max_workers=1) # Sequential access for safety
        self._checkpoint_prefix = b"__pspf_offset__:"
        self._gc_task: Optional[asyncio.Task] = None

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
            
            # Apply tuning options
            if "block_cache_size" in self.options:
                opts.block_cache = rocksdb.LRUCache(self.options["block_cache_size"])
            if "write_buffer_size" in self.options:
                opts.write_buffer_size = self.options["write_buffer_size"]
            if "max_open_files" in self.options:
                opts.max_open_files = self.options["max_open_files"]
            if "compression" in self.options:
                opts.compression = self.options["compression"] # e.g. rocksdb.CompressionType.snappy_compression
                
            return rocksdb.DB(self.path, opts, read_only=self.read_only)
            
        self._db = await loop.run_in_executor(self._executor, _open)
        logger.info(f"Opened RocksDB State Store at {self.path}")
        
        if not self.read_only:
            self._gc_task = asyncio.create_task(self._gc_loop())

    async def _gc_loop(self) -> None:
        import time
        import rocksdb
        loop = asyncio.get_running_loop()
        while True:
            try:
                await asyncio.sleep(60)
                if not self._db:
                    break
                    
                def _do_gc() -> None:
                    it = self._db.iteritems()
                    it.seek_to_first()
                    now = time.time()
                    batch = rocksdb.WriteBatch()
                    count = 0
                    for key, val in it:
                        if key.startswith(self._checkpoint_prefix):
                            continue
                            
                        obj = deserialize_state(val, key.decode(errors='ignore'), None)
                        if isinstance(obj, dict) and "_v" in obj and "_exp" in obj:
                            if obj["_exp"] is not None and now > obj["_exp"]:
                                batch.delete(key)
                                count += 1
                                
                    if count > 0:
                        self._db.write(batch)
                        logger.debug(f"GC evicted {count} expired keys")

                await loop.run_in_executor(self._executor, _do_gc)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"GC loop error: {e}")

    async def stop(self) -> None:
        if self._gc_task:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass
                
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
            obj = deserialize_state(val, key, default)
            
            if isinstance(obj, dict) and "_v" in obj and "_exp" in obj:
                if obj["_exp"] is not None and time.time() > obj["_exp"]:
                    await self.delete(key) # Lazy eviction
                    return default
                return obj["_v"]
            return obj # Backwards compatibility
        return default

    async def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        import time
        if not self._db: raise RuntimeError("Store not started")
        
        expires_at = time.time() + ttl_seconds if ttl_seconds is not None else None
        wrapped = {"_v": value, "_exp": expires_at}
        data = serialize_state(wrapped)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, lambda: self._db.put(key.encode(), data))

    async def put_batch(self, entries: Dict[str, Any]) -> None:
        if not self._db: raise RuntimeError("Store not started")
        
        import rocksdb
        def _batch() -> None:
            batch = rocksdb.WriteBatch()
            for k, v in entries.items():
                data = serialize_state(v)
                batch.put(k.encode(), data)
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
