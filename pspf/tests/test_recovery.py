import unittest
import asyncio
import os
import shutil
from typing import List, Any
from pspf.operators.core import Pipeline
from pspf.connectors.base import Source, Sink
from pspf.runtime.runner import Runner

class SequenceSource(Source[int]):
    def __init__(self, data: List[int]):
        super().__init__("SequenceSource")
        self.data = data

    async def start(self) -> None:
        for item in self.data:
            await self.emit(item)

class ListSink(Sink[Any]):
    def __init__(self):
        super().__init__("ListSink")
        self.results: List[Any] = []

    async def _process_captured(self, element: Any) -> None:
        self.results.append(element)

class TestRecovery(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.checkpoint_dir = ".pspf_test_checkpoints"
        if os.path.exists(self.checkpoint_dir):
            shutil.rmtree(self.checkpoint_dir)

    def tearDown(self):
        if os.path.exists(self.checkpoint_dir):
            shutil.rmtree(self.checkpoint_dir)

    async def test_state_recovery(self):
        # 1. Run a pipeline to build state and capture a checkpoint
        source = SequenceSource([1, 2, 3])
        p1 = Pipeline()
        p1.read_from(source) \
          .key_by(lambda x: "sum") \
          .reduce(lambda acc, cur: ("sum", acc[1] + cur[1])) \
          .write_to(ListSink()) # Sink doesn't matter for state capture

        from pspf.runtime.checkpoints import FileCheckpointBackend
        runner = Runner()
        runner.checkpoint_manager.backend = FileCheckpointBackend(self.checkpoint_dir)
        
        # Manually trigger a checkpoint for testing after run
        await runner.run_async(p1)
        state = runner._capture_state(p1)
        await runner.checkpoint_manager.trigger_checkpoint("job1", state)

        # 2. Start a NEW pipeline and NEW runner, then recover from "job1"
        source2 = SequenceSource([10])
        p2 = Pipeline()
        sink = ListSink()
        p2.read_from(source2) \
          .key_by(lambda x: "sum") \
          .reduce(lambda acc, cur: ("sum", acc[1] + cur[1])) \
          .write_to(sink)

        runner2 = Runner()
        runner2.checkpoint_manager.backend = FileCheckpointBackend(self.checkpoint_dir)
        
        # Running with job1 checkpoint
        await runner2.run_async(p2, checkpoint_id="job1")
        
        # State before p2 start was sum=6 (1+2+3).
        # p2 adds 10. Result should be 16.
        # Keyed state structure: ("sum", 16)
        # Note: list sink results will contain (key, current_result)
        self.assertEqual(sink.results[-1], ("sum", 16))

if __name__ == "__main__":
    unittest.main()
