from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

class StreamRecord(BaseModel):
    """
    Internal representation of a record in the Stream Log.
    """
    id: str
    key: str
    value: Dict[str, Any]
    event_type: str = Field(default="")
    timestamp: datetime
    partition: Optional[int] = None
    offset: Optional[int] = None
    topic: Optional[str] = None
