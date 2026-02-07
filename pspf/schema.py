import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Generic, TypeVar, Type
from pydantic import BaseModel, Field, field_validator

T = TypeVar("T", bound=BaseModel)

class BaseEvent(BaseModel):
    """
    Base Pydantic model for all stream events.
    Ensures every event has a unique ID and timestamp.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = Field(default_factory=dict)
    
    # Metadata fields that typically aren't part of the business logic payload
    partition: Optional[int] = None
    offset: Optional[str] = None  # String to accommodate Valkey/Redis IDs (12345-0)

    model_config = {
        "extra": "allow",  # Allow extra fields for flexibility
        "arbitrary_types_allowed": True,
        "from_attributes": True # V2 alias for orm_mode
    }

class SchemaRegistry:
    """
    Simple registry to map event types to Pydantic models.
    """
    _registry: Dict[str, Type[BaseModel]] = {}

    @classmethod
    def register(cls, event_type: str, model: Type[BaseModel]):
        cls._registry[event_type] = model

    @classmethod
    def get_model(cls, event_type: str) -> Optional[Type[BaseModel]]:
        return cls._registry.get(event_type)

    @classmethod
    def validate(cls, event_dict: Dict[str, Any]) -> BaseModel:
        """
        Validate a raw dictionary against the registered schema for its event_type.
        Falls back to BaseEvent if no specific schema is registered.
        """
        event_type = event_dict.get("event_type")
        if not event_type:
             # If no event type, try to coerce to BaseEvent generic
             return BaseEvent(**event_dict)
        
        model_class = cls.get_model(event_type)
        if model_class:
            return model_class.model_validate(event_dict)
        
        return BaseEvent(**event_dict)
