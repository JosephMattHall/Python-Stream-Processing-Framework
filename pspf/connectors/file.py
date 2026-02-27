import asyncio
from typing import Any
from pspf.connectors.base import Source, Sink
from pspf.utils.logging import get_logger

class FileSource(Source):
    """Reads lines from a file."""

    def __init__(self, path: str, delay: float = 0.0) -> None:
        super().__init__(name=f"FileSource({path})")
        self.path = path
        self.delay = delay
        self.logger = get_logger(self.name)

    async def emit(self, element: Any) -> None:
        pass

    async def start(self) -> None:
        try:
            with open(self.path, 'r') as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    await self.emit(stripped)
                    if self.delay > 0:
                        await asyncio.sleep(self.delay)
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading {self.path}: {e}")
            raise


class StorageSink(Sink):
    def __init__(self, path: str) -> None:
        super().__init__(name=f"StorageSink({path})")
        self.path = path
        self.logger = get_logger(self.name)

    async def _process_captured(self, element: Any) -> None:
        try:
            with open(self.path, 'a') as f:
                f.write(f"{str(element)}\n")
        except Exception as e:
            self.logger.error(f"Error writing to {self.path}: {e}")
            raise


class ConsoleSink(Sink):
    def __init__(self, prefix: str = ""):
        super().__init__("ConsoleSink")
        self.prefix = prefix

    async def _process_captured(self, element: Any) -> None:
        print(f"{self.prefix}{element}")
