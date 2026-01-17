from typing import Dict, Optional
from .events import (
    InventoryEvent, ItemCreated, ItemCheckedIn, ItemRecounted, 
    ItemCheckedOut, CheckoutRejected, CheckoutRequested
)

class ItemState:
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.name: Optional[str] = None
        self.qty: int = 0
        self.created: bool = False

    def apply(self, event: InventoryEvent) -> None:
        """
        Apply an event to update the state.
        This is used for both projection and validating new commands.
        """
        if event.item_id != self.item_id:
            return

        if isinstance(event, ItemCreated):
            self.created = True
            self.name = event.name
            self.qty = event.initial_qty
        
        elif isinstance(event, ItemCheckedIn):
            self.qty += event.qty

        elif isinstance(event, ItemRecounted):
            self.qty = event.new_qty
            
        elif isinstance(event, ItemCheckedOut):
            self.qty -= event.qty

        elif isinstance(event, CheckoutRejected):
            self.qty += event.qty

class InventoryStateStore:
    """
    In-memory store of all items.
    """
    def __init__(self):
        self.items: Dict[str, ItemState] = {}

    def get_item(self, item_id: str) -> ItemState:
        if item_id not in self.items:
            self.items[item_id] = ItemState(item_id)
        return self.items[item_id]

    def apply_event(self, event: InventoryEvent) -> None:
        item = self.get_item(event.item_id)
        item.apply(event)
