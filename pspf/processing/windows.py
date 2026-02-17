from abc import ABC, abstractmethod
from typing import List, Tuple, Any
from datetime import datetime, timezone

class Window(ABC):
    @abstractmethod
    def assign_windows(self, timestamp: float) -> List[Tuple[float, float]]:
        """
        Assigns a timestamp to one or more windows.
        Returns list of (start, end) tuples.
        """
        pass

class TumblingWindow(Window):
    """
    Fixed-size, non-overlapping windows.
    """
    def __init__(self, size_ms: int):
        self.size_ms = size_ms

    def assign_windows(self, timestamp: float) -> List[Tuple[float, float]]:
        # timestamp is in seconds, convert to ms
        ts_ms = int(timestamp * 1000)
        start = ts_ms - (ts_ms % self.size_ms)
        end = start + self.size_ms
        return [(start / 1000.0, end / 1000.0)]

class SlidingWindow(Window):
    """
    Fixed-size, overlapping windows.
    """
    def __init__(self, size_ms: int, slide_ms: int):
        self.size_ms = size_ms
        self.slide_ms = slide_ms

    def assign_windows(self, timestamp: float) -> List[Tuple[float, float]]:
        ts_ms = int(timestamp * 1000)
        last_start = ts_ms - (ts_ms % self.slide_ms)
        windows = []
        # Backtrack to find all windows that overlap this timestamp
        current_start = last_start
        while current_start + self.size_ms > ts_ms:
             windows.append((current_start / 1000.0, (current_start + self.size_ms) / 1000.0))
             current_start -= self.slide_ms
        return windows
