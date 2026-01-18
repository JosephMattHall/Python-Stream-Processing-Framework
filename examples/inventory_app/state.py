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

    async def get_item(self, item_id: str) -> ItemState:
        if item_id not in self.items:
            self.items[item_id] = ItemState(item_id)
        return self.items[item_id]

    async def apply_event(self, event: InventoryEvent) -> None:
        item = await self.get_item(event.item_id)
        item.apply(event)
        await self.save_item(item)

    async def save_item(self, item: ItemState) -> None:
        # Default: No-op for memory store
        pass


class ValkeyInventoryStateStore(InventoryStateStore):
    """
    Durable State Store backed by Valkey.
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, prefix: str = 'pspf:inventory'):
        super().__init__()
        import valkey.asyncio as valkey
        self.valkey = valkey.Valkey(host=host, port=port, decode_responses=True)
        self.prefix = prefix

    async def get_item(self, item_id: str) -> ItemState:
        # Check memory first
        if item_id in self.items:
            return self.items[item_id]
        
        # Load from Valkey
        import json
        key = f"{self.prefix}:{item_id}"
        data = await self.valkey.get(key)
        
        item = ItemState(item_id)
        if data:
            state_dict = json.loads(data)
            item.name = state_dict.get("name")
            item.qty = state_dict.get("qty", 0)
            item.created = state_dict.get("created", False)
        
        self.items[item_id] = item
        return item

    async def save_item(self, item: ItemState) -> None:
        import json
        key = f"{self.prefix}:{item.item_id}"
        state_dict = {
            "name": item.name,
            "qty": item.qty,
            "created": item.created
        }
        await self.valkey.set(key, json.dumps(state_dict))

