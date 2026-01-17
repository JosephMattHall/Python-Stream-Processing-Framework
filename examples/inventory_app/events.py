from dataclasses import dataclass
from typing import Literal, Union
from datetime import datetime

@dataclass
class BaseInventoryEvent:
    event_type: str
    item_id: str
    timestamp: str 

@dataclass
class ItemCreated(BaseInventoryEvent):
    name: str
    initial_qty: int

@dataclass
class ItemCheckedIn(BaseInventoryEvent):
    qty: int
    user_id: str

@dataclass
class ItemRecounted(BaseInventoryEvent):
    """Admin force-override of quantity."""
    new_qty: int
    reason: str

# Commands / Requests
@dataclass
class CheckoutRequested(BaseInventoryEvent):
    qty: int
    user_id: str

# Outcomes
@dataclass
class ItemCheckedOut(BaseInventoryEvent):
    qty: int
    user_id: str
    request_id: str  # Link back to the request event ID

@dataclass
class CheckoutRejected(BaseInventoryEvent):
    qty: int
    user_id: str
    reason: str
    request_id: str

# Union type for easy typing
InventoryEvent = Union[
    ItemCreated, 
    ItemCheckedIn, 
    ItemRecounted, 
    CheckoutRequested, 
    ItemCheckedOut, 
    CheckoutRejected
]
