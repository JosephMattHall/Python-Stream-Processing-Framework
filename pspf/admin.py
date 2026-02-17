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

    return app
