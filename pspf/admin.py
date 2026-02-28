from typing import Dict, Any, TYPE_CHECKING
from fastapi import FastAPI, HTTPException
from pspf.utils.logging import get_logger
from pspf.models import StreamRecord

if TYPE_CHECKING:
    from pspf.processor import BatchProcessor

logger = get_logger("AdminAPI")

def create_admin_app(processor: "BatchProcessor") -> FastAPI:
    """
    Creates the FastAPI Admin Application.
    
    Args:
        processor: The active BatchProcessor instance to control.
    """
    app = FastAPI(title="PSPF Admin API", version="0.1.0")

    @app.get("/health")
    async def health_check() -> Dict[str, str]:
        """Returns the health status of the worker."""
        if processor._running:
            status = "paused" if processor._paused else "running"
            return {"status": "ok", "worker_state": status}
        return {"status": "stopped", "worker_state": "stopped"}

    @app.post("/control/pause")
    async def pause_worker() -> Dict[str, str]:
        """Pauses the message consumption loop."""
        processor.pause()
        return {"status": "paused"}

    @app.post("/control/resume")
    async def resume_worker() -> Dict[str, str]:
        """Resumes message consumption."""
        processor.resume()
        return {"status": "resumed"}

    @app.get("/control/status")
    async def worker_status() -> Dict[str, Any]:
        """Detailed status of the worker."""
        return {
            "running": processor._running,
            "paused": processor._paused,
            # Handle backend that might not have stream_key
            "backend": getattr(processor.backend, "stream_key", "unknown")
        }

    @app.get("/state/{key}")
    async def get_state(key: str) -> Dict[str, Any]:
        """Interactive Query: fetch a key from the local state store."""
        if not processor.state_store:
            raise HTTPException(status_code=404, detail="No state store configured on this worker")
        
        try:
            val = await processor.state_store.get(key)
            if val is None:
                raise HTTPException(status_code=404, detail=f"Key {key} not found")
            return {"key": key, "value": val}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error querying state: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/cluster/status")
    async def cluster_status() -> Dict[str, Any]:
        """Returns detailed cluster and partition status."""
        status: Dict[str, Any] = {
            "ha_enabled": False,
            "node_id": None,
            "nodes": [],
            "held_partitions": []
        }
        
        if hasattr(processor, "replicated_log") and processor.replicated_log:
            try:
                coordinator = processor.replicated_log._coordinator # type: ignore
                status["ha_enabled"] = True
                status["node_id"] = coordinator.node_id
                status["held_partitions"] = coordinator._held_partitions
                status["nodes"] = await coordinator.get_other_nodes()
            except Exception as e:
                logger.error(f"Error fetching cluster status: {e}")
                status["error"] = str(e)
                
        return status

    @app.post("/internal/replicate")
    async def replicate_record(record: StreamRecord) -> Dict[str, str]:
        """
        Internal endpoint for receiving replicated records from the leader.
        """
        try:
            # We assume the processor has 'replicated_log' attached if HA is enabled
            if hasattr(processor, "replicated_log") and processor.replicated_log:
                 await processor.replicated_log.append_follower(record)
                 return {"status": "acked"}
            else:
                 raise HTTPException(status_code=501, detail="Replication not enabled on this node")
        except Exception as e:
            logger.error(f"Replication failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/internal/pull/{partition}")
    async def pull_records(partition: int, offset: int = 0) -> list[Dict[str, Any]]:
        """
        Internal endpoint for followers to pull missing records from the leader.
        """
        try:
            if hasattr(processor, "replicated_log") and processor.replicated_log:
                records = []
                # Read up to 100 records
                async for r in processor.replicated_log._local.read(partition, offset):
                    records.append(r.model_dump(mode='json'))
                    if len(records) >= 100:
                        break
                return records
            else:
                 raise HTTPException(status_code=501, detail="Replication not enabled on this node")
        except Exception as e:
            logger.error(f"Pull failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return app
