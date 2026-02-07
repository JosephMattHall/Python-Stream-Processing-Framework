import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pydantic import BaseModel
from pspf.schema import SchemaRegistry, BaseEvent
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.telemetry import TelemetryManager

# --- Schema Unit Tests ---
class TestSchema(BaseModel):
    name: str

def test_schema_registry_registration():
    SchemaRegistry.register("TestType", TestSchema)
    assert SchemaRegistry.get_model("TestType") == TestSchema

def test_schema_validation_success():
    SchemaRegistry.register("TestType", TestSchema)
    data = {"event_type": "TestType", "name": "foo"}
    model = SchemaRegistry.validate(data)
    assert isinstance(model, TestSchema)
    assert model.name == "foo"

def test_schema_validation_fallback():
    data = {"event_type": "UnknownType", "some_field": 123}
    model = SchemaRegistry.validate(data)
    assert isinstance(model, BaseEvent)
    assert model.event_type == "UnknownType"
    # extra="allow" puts fields as attributes
    assert getattr(model, "some_field") == 123

# --- Serialization Unit Tests ---
@pytest.mark.asyncio
async def test_valkey_serialization_logic():
    # We mock the connector to inspect what gets passed to xadd
    mock_connector = MagicMock()
    mock_client = AsyncMock()
    mock_connector.get_client.return_value = mock_client
    
    backend = ValkeyStreamBackend(connector=mock_connector, stream_key="s", group_name="g", consumer_name="c")
    
    complex_data = {
        "event_type": "Complex",
        "nested": {"a": 1},
        "list": [1, 2],
        "simple": "string"
    }
    
    await backend.add_event(complex_data)
    
    # Verify xadd called with serialized JSON strings for complex types
    args, kwargs = mock_client.xadd.call_args
    stream_key, data = args[0], args[1]
    
    assert stream_key == "s"
    assert data["simple"] == "string"
    assert isinstance(data["nested"], str)
    assert json.loads(data["nested"]) == {"a": 1}
    assert isinstance(data["list"], str)
    assert json.loads(data["list"]) == [1, 2]

@pytest.mark.asyncio
async def test_valkey_deserialization_logic():
    # We mock read_batch response
    mock_connector = MagicMock()
    mock_client = AsyncMock()
    mock_connector.get_client.return_value = mock_client
    
    backend = ValkeyStreamBackend(connector=mock_connector, stream_key="s", group_name="g", consumer_name="c")
    
    # Simulate data stored as JSON strings
    mock_response = [
        [
            b"stream_key", 
            [
                (b"1-0", {
                    "event_type": "Complex",
                    "nested": '{"a": 1}',
                    "list": '[1, 2]',
                    "simple": "string"
                })
            ]
        ]
    ]
    mock_client.xreadgroup.return_value = mock_response
    
    messages = await backend.read_batch()
    
    assert len(messages) == 1
    msg_id, data = messages[0]
    
    assert data["simple"] == "string"
    assert data["nested"] == {"a": 1} # Deserialized back to dict
    assert data["list"] == [1, 2]     # Deserialized back to list

# --- Telemetry Tests ---
def test_telemetry_singleton():
    t1 = TelemetryManager()
    t2 = TelemetryManager()
    assert t1 is t2

def test_telemetry_disabled_by_default():
    # Assuming .env doesn't set OTEL_ENABLED=true or defaults are False
    # If environment sets it, this might flap, but default in settings.py is False
    with patch('pspf.settings.settings.OTEL_ENABLED', False), \
         patch('pspf.telemetry.MetricsCollector') as MockCollector:
        # We need to reset the singleton to test init logic effectively
        TelemetryManager._instance = None
        t = TelemetryManager()
        assert not t.enabled
        # Ensure MetricsCollector was still instantiated (as per code it is)
        MockCollector.assert_called() 
        tracer = t.get_tracer()
        # Verify no-op behavior via type check or behavior
        # In opentelemetry, NoOpTracer is returned
        assert "noop" in str(type(tracer)).lower() or "noop" in str(tracer).lower()
