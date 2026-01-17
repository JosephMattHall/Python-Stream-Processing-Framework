from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

@dataclass
class StreamRecord:
    """
    Canonical record for the stream processing framework.
    Used universally across logs, sources, and operators.
    """
    id: str                  # Unique Event ID (UUID)
    key: str                 # Partition key
    value: Any               # Event payload
    event_type: str          # Event Type (e.g. "ItemCreated")
    timestamp: datetime      # Event time
    partition: Optional[int] = None
    offset: Optional[int] = None
