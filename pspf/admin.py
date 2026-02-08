from typing import Dict, Any, TYPE_CHECKING
from fastapi import FastAPI, HTTPException
from pspf.utils.logging import get_logger

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
        # We could expose more internal stats here
        return {
            "running": processor._running,
            "paused": processor._paused,
            "backend": processor.backend.stream_key
        }

    return app
