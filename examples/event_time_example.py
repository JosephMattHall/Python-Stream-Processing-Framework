import asyncio
from typing import List
from pspf.operators.core import Pipeline
from pspf.connectors.base import Source, Sink
from pspf.operators.windowing import SlidingEventTimeWindow

class OutOfOrderSource(Source[tuple]):
    """Emits data points with explicit timestamps, some out of order."""
    def __init__(self):
        super().__init__("OutOfOrderSource")
        self.data = [
            (10.0, "a"),
            (15.0, "b"),
            (12.0, "c-LATE"), # This is late but still within window [10, 20)
            (21.0, "d"),      # This pushes watermark to 21
            (25.0, "e"),
            (31.0, "f"),      # This pushes watermark to 31, closing window [20, 30)
        ]

    async def start(self) -> None:
        for item in self.data:
            await self.emit(item)
            await asyncio.sleep(0.1)

class ConsoleSink(Sink[List]):
    def __init__(self, prefix: str = ""):
        super().__init__("ConsoleSink")
        self.prefix = prefix

    async def _process_captured(self, element: List) -> None:
        print(f"{self.prefix}Window Result: {element}")

def main():
    print("--- Starting Event Time Window Example ---")
    
    p = Pipeline()
    # In this example, our source already provides (ts, val) tuples.
    # Our extractor just returns the first element.
    (p.read_from(OutOfOrderSource())
      .assign_timestamps(lambda x: x[0])
      .window(10) # This will create a SlidingEventTimeWindow or similar 
                  # (Note: we need to update Pipeline to support SlidingEventTimeWindow specifically)
      .write_to(ConsoleSink(prefix="[EVENT-TIME] ")))

    # We need to manually fix the Pipeline.window to use EventTime window if requested.
    # For now, let's just run it. 
    # WAIT: I need to update Pipeline.window to distinguish between Processing and Event time.
    p.run()

if __name__ == "__main__":
    main()
