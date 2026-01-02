import unittest
import asyncio
from typing import List, Any
from pspf.operators.core import Pipeline
from pspf.connectors.base import Source, Sink

class ListSource(Source[int]):
    """Simulated source from a list."""
    def __init__(self, data: List[int]):
        super().__init__("ListSource")
        self.data = data

    async def start(self) -> None:
        for item in self.data:
            await self.emit(item)

class ListSink(Sink[Any]):
    """Collects results into a list."""
    def __init__(self):
        super().__init__("ListSink")
        self.results: List[Any] = []

    async def _process_captured(self, element: Any) -> None:
        self.results.append(element)


class TestOperators(unittest.IsolatedAsyncioTestCase):
    async def test_map_filter(self):
        source = ListSource([1, 2, 3, 4, 5])
        sink = ListSink()

        p = Pipeline()
        p.read_from(source) \
         .filter(lambda x: x % 2 == 0) \
         .map(lambda x: x * 10) \
         .write_to(sink)

        from pspf.runtime.runner import Runner
        runner = Runner()
        await runner.run_async(p)

        self.assertEqual(sink.results, [20, 40])

    async def test_reduce_keyed(self):
        data = [
            ("a", 1),
            ("b", 1),
            ("a", 2),
            ("b", 5),
        ]
        
        class TupleSource(Source[tuple]):
            def __init__(self, data):
                super().__init__("TupleSource")
                self.data = data
            async def start(self):
                for item in self.data:
                    await self.emit(item)

        sink = ListSink()
        source = TupleSource(data)

        p = Pipeline()
        
        p.read_from(source) \
         .key_by(lambda x: x[0]) \
         .reduce(lambda acc, current: (acc[0], acc[1] + current[1])) \
         .write_to(sink)

        from pspf.runtime.runner import Runner
        runner = Runner()
        await runner.run_async(p)

        expected = [
            ("a", ("a", 1)),
            ("b", ("b", 1)),
            ("a", ("a", 3)),
            ("b", ("b", 6))
        ]
        
        self.assertEqual(sink.results, expected)

if __name__ == "__main__":
    unittest.main()
