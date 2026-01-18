import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional

from pspf.models import StreamRecord
from pspf.log.interfaces import Log
from pspf.runtime.dedup import DeduplicationStore
from .events import (
    InventoryEvent, ItemCreated, ItemCheckedIn, ItemCheckedOut, CheckoutRejected
)
from .state import InventoryStateStore
from pspf.utils.metrics import MetricsManager

class InventoryProcessor:
    """
    Business logic for inventory events. 
    Handles state updates and invariant checks (like preventing negative stock).
    """
    def __init__(self, 
                 state_store: InventoryStateStore, 
                 log: Log, 
                 dedup_store: DeduplicationStore):
        self.state_store = state_store
        self.log = log
        self.dedup_store = dedup_store
        self.metrics = MetricsManager()
        self.events_processed = self.metrics.counter("inventory_events_total")
        self.rejections = self.metrics.counter("inventory_rejections_total")

    async def process(self, record: StreamRecord) -> None:
        if await self.dedup_store.has_processed(record.id):
            return

        self.events_processed.inc()

        try:
            event = self._parse_event(record)
        except Exception as e:
            print(f"Failed to parse event {record.id}: {e}")
            await self.dedup_store.mark_processed(record.id)
            return

        if not event:
            await self.dedup_store.mark_processed(record.id)
            return

        # Apply event and check for business rule violations
        await self.state_store.apply_event(event)
        
        item_state = await self.state_store.get_item(event.item_id)
        
        if item_state.qty < 0:
            # We don't allow negative stock. 
            # If a checkout caused this, we emit a compensation event.
            if isinstance(event, ItemCheckedOut):
                rejection = CheckoutRejected(
                    event_type="CHECKOUT_REJECTED",
                    item_id=event.item_id,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    qty=event.qty,
                    user_id=event.user_id,
                    reason="Insufficient stock",
                    request_id=record.id
                )
                
                rejection_id = str(uuid.uuid4())
                rejection_record = StreamRecord(
                    id=rejection_id,
                    key=rejection.item_id,
                    value=self._serialize_event(rejection),
                    event_type="CheckoutRejected",
                    timestamp=datetime.now(timezone.utc)
                )
                
                await self.log.append(rejection_record)
                await self.state_store.apply_event(rejection)
                self.rejections.inc()
                
                # Pre-mark as processed so we don't double-count the compensation on replay
                await self.dedup_store.mark_processed(rejection_id)
            
        await self.dedup_store.mark_processed(record.id)

    def _parse_event(self, record: StreamRecord) -> Optional[InventoryEvent]:
        from protos import inventory_pb2
        
        event_type = getattr(record, "event_type", "")
        if not event_type:
             return None
             
        try:
            if event_type == "ItemCreated":
                proto = inventory_pb2.ItemCreated()
                proto.ParseFromString(record.value)
                return ItemCreated(
                    event_type="ITEM_CREATED",
                    item_id=proto.item_id,
                    timestamp=record.timestamp.isoformat(),
                    name=proto.name,
                    initial_qty=0
                )
                
            elif event_type == "ItemCheckedIn":
                proto = inventory_pb2.ItemCheckedIn()
                proto.ParseFromString(record.value)
                return ItemCheckedIn(
                    event_type="ITEM_CHECKED_IN",
                    item_id=proto.item_id,
                    timestamp=record.timestamp.isoformat(),
                    qty=proto.qty,
                    user_id=proto.user_id
                )
                
            elif event_type == "ItemCheckedOut":
                proto = inventory_pb2.ItemCheckedOut()
                proto.ParseFromString(record.value)
                return ItemCheckedOut(
                    event_type="ITEM_CHECKED_OUT",
                    item_id=proto.item_id,
                    timestamp=record.timestamp.isoformat(),
                    qty=proto.qty,
                    user_id=proto.user_id,
                    request_id=record.id
                )
                
            elif event_type == "CheckoutRejected":
                proto = inventory_pb2.CheckoutRejected()
                proto.ParseFromString(record.value)
                return CheckoutRejected(
                    event_type="CHECKOUT_REJECTED",
                    item_id=proto.item_id,
                    timestamp=record.timestamp.isoformat(),
                    qty=proto.requested_qty,
                    user_id="system",
                    reason=proto.reason,
                    request_id=record.id
                )
                
        except Exception as e:
            print(f"Protobuf Parse Error ({event_type}): {e}")
            return None
            
        return None

    def _serialize_event(self, event: InventoryEvent) -> bytes:
        from protos import inventory_pb2
        
        if isinstance(event, CheckoutRejected):
            proto = inventory_pb2.CheckoutRejected(
                item_id=event.item_id,
                requested_qty=event.qty,
                available_qty=0,
                reason=event.reason
            )
            return proto.SerializeToString()
        
        return b""
