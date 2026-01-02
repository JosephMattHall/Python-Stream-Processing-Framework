import unittest
import asyncio
from pspf.operators.core import Pipeline
from pspf.connectors.base import Source, Sink
from pspf.runtime.runner import Runner

class NoOpSource(Source[str]):
    async def start(self) -> None:
        await self.emit("scouting")

class NoOpSink(Sink[str]):
    async def _process_captured(self, element: str) -> None:
        pass

class TestRuntime(unittest.IsolatedAsyncioTestCase):
    async def test_runner_execution(self):
        source = NoOpSource()
        sink = NoOpSink()
        pipeline = Pipeline()
        pipeline.read_from(source).write_to(sink)
        
        runner = Runner()
        # Should finish without error
        await runner.run_async(pipeline)

if __name__ == "__main__":
    unittest.main()
