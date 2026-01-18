import json
import uuid
import valkey
from typing import Any
from pspf.connectors.base import Sink

class ValkeyDeadLetterSink(Sink[Any]):
    """
    Saves failed records to a Valkey list for later inspection.
    """
    def __init__(self, host: str = "localhost", port: int = 6379, queue_key: str = "pspf:dlq"):
        super().__init__(name="ValkeyDeadLetterSink")
        self.client = valkey.Valkey(host=host, port=port, decode_responses=False)
        self.queue_key = queue_key

    async def _process_captured(self, element: Any) -> None:
        """
        Pushes the failed element to the Valkey list.
        Serializes to JSON if possible.
        """
        try:
            # If it's a StreamRecord, we might want to preserve its structure
            from pspf.models import StreamRecord
            if isinstance(element, StreamRecord):
                payload = {
                    "id": element.id,
                    "key": element.key,
                    "event_type": element.event_type,
                    "timestamp": element.timestamp.isoformat(),
                    # Value might be bytes (Protobuf)
                    "value_hex": element.value.hex() if isinstance(element.value, bytes) else str(element.value)
                }
                data = json.dumps(payload).encode('utf-8')
            else:
                data = str(element).encode('utf-8')
            
            self.client.rpush(self.queue_key, data)
        except Exception as e:
            self.logger.error(f"Failed to write to DLQ: {e}")
