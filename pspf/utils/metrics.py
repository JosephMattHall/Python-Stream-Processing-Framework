from datetime import datetime
from typing import Dict, Any

class Counter:
    def __init__(self, name: str):
        self.name = name
        self.value = 0

    def inc(self, amount: int = 1):
        self.value += amount

class Gauge:
    def __init__(self, name: str):
        self.name = name
        self.value = 0.0

    def set(self, value: float):
        self.value = value

class MetricsManager:
    """Registry for framework metrics."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MetricsManager, cls).__new__(cls)
            cls._instance.metrics: Dict[str, Any] = {}
        return cls._instance

    def counter(self, name: str) -> Counter:
        if name not in self.metrics:
            self.metrics[name] = Counter(name)
        return self.metrics[name]

    def gauge(self, name: str) -> Gauge:
        if name not in self.metrics:
            self.metrics[name] = Gauge(name)
        return self.metrics[name]

    def get_all(self) -> Dict[str, Any]:
        return {k: v.value for k, v in self.metrics.items()}
