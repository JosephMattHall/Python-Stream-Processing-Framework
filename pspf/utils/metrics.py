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
    """Registry for framework metrics. Supports Prometheus if installed."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MetricsManager, cls).__new__(cls)
            cls._instance.metrics: Dict[str, Any] = {}
            try:
                from prometheus_client import CollectorRegistry, Counter as PromCounter, Gauge as PromGauge
                cls._instance.prom_registry = CollectorRegistry()
                cls._instance._has_prom = True
            except ImportError:
                cls._instance._has_prom = False
        return cls._instance

    def counter(self, name: str) -> Counter:
        if name not in self.metrics:
            if self._has_prom:
                from prometheus_client import Counter as PromCounter
                self.metrics[name] = PromCounter(name, f"Counter for {name}", registry=self.prom_registry)
            else:
                self.metrics[name] = Counter(name)
        return self.metrics[name]

    def gauge(self, name: str) -> Any:
        if name not in self.metrics:
            if self._has_prom:
                from prometheus_client import Gauge as PromGauge
                self.metrics[name] = PromGauge(name, f"Gauge for {name}", registry=self.prom_registry)
            else:
                self.metrics[name] = Gauge(name)
        return self.metrics[name]

    def set_lag(self, partition: int, lag: int) -> None:
        """Helper to set partition-specific lag metrics."""
        # Using a structured name to avoid multidimensional metric complexity in simple mode
        g = self.gauge(f"pspf_partition_lag_{partition}")
        g.set(float(lag))

    def get_all(self) -> Dict[str, Any]:
        res = {}
        for k, v in self.metrics.items():
            if self._has_prom:
                # Prometheus counters/gauges have internal state
                res[k] = v._value.get()
            else:
                res[k] = v.value
        return res
