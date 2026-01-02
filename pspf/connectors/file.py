import asyncio
from typing import Any
from pspf.connectors.base import Source, Sink
from pspf.utils.logging import get_logger

class FileSource(Source[str]):
    """Reads lines from a file."""

    def __init__(self, path: str, delay: float = 0.0):
        super().__init__(name=f"FileSource({path})")
        self.path = path
        self.delay = delay

    async def start(self) -> None:
        """Read lines from file and emit them."""
        try:
            with open(self.path, 'r') as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped:  # Skip empty lines
                        continue
                    await self.emit(stripped)
                    if self.delay > 0:
                        await asyncio.sleep(self.delay)
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading file {self.path}: {e}")
            raise


class StorageSink(Sink[Any]):
    """Writes elements to a file."""

    def __init__(self, path: str):
        super().__init__(name=f"StorageSink({path})")
        self.path = path

    async def _process_captured(self, element: Any) -> None:
        """Write element to file."""
        try:
            with open(self.path, 'a') as f:
                f.write(f"{str(element)}\n")  # Fixed: was \\n, now actual newline
        except Exception as e:
            self.logger.error(f"Error writing to {self.path}: {e}")
            raise


class ConsoleSink(Sink[Any]):
    """Writes elements to stdout."""
    
    def __init__(self, prefix: str = ""):
        super().__init__("ConsoleSink")
        self.prefix = prefix

    async def _process_captured(self, element: Any) -> None:
        """Print element to console."""
        print(f"{self.prefix}{element}")
