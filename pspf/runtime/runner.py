import asyncio
from typing import List, TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from pspf.operators.core import Pipeline

from pspf.runtime.checkpoints import CheckpointManager, FileCheckpointBackend
from pspf.utils.logging import get_logger, setup_logging
from pspf.utils.metrics import MetricsManager

class Runner:
    """Executes a pipeline with optional checkpointing."""

    def __init__(self, checkpoint_interval: float = 0.0, log_level: str = "INFO"):
        setup_logging()
        self.logger = get_logger("Runner")
        self.metrics = MetricsManager()
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_manager = CheckpointManager(FileCheckpointBackend())

    def run(self, pipeline: 'Pipeline', checkpoint_id: Optional[str] = None) -> None:
        """Run the pipeline synchronously (blocks until completion)."""
        asyncio.run(self.run_async(pipeline, checkpoint_id))

    async def run_async(self, pipeline: 'Pipeline', checkpoint_id: Optional[str] = None) -> None:
        """Run the pipeline asynchronously."""
        # 1. Recovery phase
        if checkpoint_id:
            await self._recover(pipeline, checkpoint_id)

        # 2. Start workers & sources
        tasks = []
        for source in pipeline.sources:
            tasks.append(asyncio.create_task(source.start()))
        
        # 3. Periodic Checkpointing
        if self.checkpoint_interval > 0 and checkpoint_id:
            tasks.append(asyncio.create_task(self._checkpoint_loop(pipeline, checkpoint_id)))

        if tasks:
            await asyncio.gather(*tasks)
        
        await asyncio.sleep(0.5)

    async def _recover(self, pipeline: 'Pipeline', checkpoint_id: str) -> None:
        state_map = await self.checkpoint_manager.restore_checkpoint(checkpoint_id)
        if state_map:
            self.logger.info(f"Restoring from checkpoint: {checkpoint_id}")
            # Map state back to operators by name
            self._apply_state(pipeline, state_map)

    def _apply_state(self, pipeline: 'Pipeline', state_map: Dict[str, Any]) -> None:
        # Traverse pipeline and apply state
        seen = set()
        stack = list(pipeline.sources)
        while stack:
            op = stack.pop()
            if id(op) in seen:
                continue
            seen.add(id(op))
            
            if op.name in state_map:
                self.logger.debug(f"Restoring state for operator: {op.name}")
                op.restore_state(state_map[op.name])
            
            stack.extend(op.downstream)

    async def _checkpoint_loop(self, pipeline: 'Pipeline', checkpoint_id: str) -> None:
        while True:
            await asyncio.sleep(self.checkpoint_interval)
            state_snapshot = self._capture_state(pipeline)
            await self.checkpoint_manager.trigger_checkpoint(checkpoint_id, state_snapshot)
            self.logger.info(f"Checkpoint saved: {checkpoint_id}")

    def _capture_state(self, pipeline: 'Pipeline') -> Dict[str, Any]:
        state_map = {}
        seen = set()
        stack = list(pipeline.sources)
        while stack:
            op = stack.pop()
            if id(op) in seen:
                continue
            seen.add(id(op))
            
            op_state = op.snapshot_state()
            if op_state:
                state_map[op.name] = op_state
                
            stack.extend(op.downstream)
        return state_map

