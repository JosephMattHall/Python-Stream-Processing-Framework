from typing import Optional
import os
from pspf.utils.logging import get_logger

logger = get_logger("tracing")

# Optional dependency
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False

def init_tracer(service_name: str) -> None:
    if not HAS_OTEL:
        return
    
    provider = TracerProvider()
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    logger.info(f"Initialized tracer for {service_name}")

def get_tracer(name: str):
    if HAS_OTEL:
        return trace.get_tracer(name)
    return None

class TraceStub:
    """Fallback when OTel is not installed."""
    def start_as_current_span(self, name: str):
        class DummySpan:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def set_attribute(self, key, value): pass
        return DummySpan()

def start_span(tracer, name: str):
    if tracer:
        return tracer.start_as_current_span(name)
    return TraceStub().start_as_current_span(name)
