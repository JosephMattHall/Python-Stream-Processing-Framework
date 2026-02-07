import logging
import time
from typing import Dict, Any, Optional

# Telemetry libraries
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.propagate import extract, inject
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from pspf.settings import settings

logger = logging.getLogger("pspf.telemetry")

class MetricsCollector:
    """
    Prometheus Metrics definition.
    """
    def __init__(self):
        # Counters
        self.messages_processed = Counter(
            'stream_messages_processed_total', 
            'Total messages processed',
            ['stream', 'status']
        )
        
        # Histograms
        self.processing_latency = Histogram(
            'stream_processing_seconds',
            'Time taken to process message',
            ['stream'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        
        # Gauges
        self.lag = Gauge(
            'stream_lag',
            'Estimated consumer group lag',
            ['stream', 'group']
        )
        self.active_consumers = Gauge(
            'stream_active_consumers',
            'Number of active consumers',
            ['stream', 'group']
        )

class TelemetryManager:
    """
    Centralized manager for Observability.
    Singleton pattern usage recommended.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelemetryManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.enabled = settings.OTEL_ENABLED
        self.metrics = MetricsCollector()
        self.tracer = None
        
        if self.enabled:
            self._setup_otel()
            logger.info("OpenTelemetry enabled.")
        else:
            logger.info("OpenTelemetry disabled via config.")

        # Start Prometheus Endpoint
        try:
            start_http_server(settings.PROMETHEUS_PORT)
            logger.info(f"Prometheus metrics exposed on port {settings.PROMETHEUS_PORT}")
        except Exception as e:
            logger.warning(f"Could not start Prometheus server (maybe already running?): {e}")

        self._initialized = True

    def _setup_otel(self):
        """
        Configure OTel Provider.
        For production, you'd likely Config OTLP Exporter here.
        For now, we use Console or NoOp.
        """
        provider = TracerProvider()
        # Simple console exporter for demo purposes
        processor = SimpleSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer("pspf", "0.1.0")

    def get_tracer(self):
        if not self.enabled:
            return trace.NoOpTracerProvider().get_tracer("noop")
        return self.tracer

    def inject_context(self, carrier: Dict[str, Any]):
        """Injects current Trace Context into a dictionary."""
        if not self.enabled:
            return
        inject(carrier)

    def extract_context(self, carrier: Dict[str, Any]):
        """Returns a Context object extracted from the carrier."""
        if not self.enabled:
            return None
        return extract(carrier)

# Usage: telemetry = TelemetryManager()
