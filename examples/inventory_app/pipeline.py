import asyncio
import uuid
import json
from datetime import datetime, timezone
from typing import Optional

from pspf.models import StreamRecord
from pspf.log.interfaces import Log
from pspf.runtime.dedup import DeduplicationStore
from .state import InventoryStateStore
from .events import (
    InventoryEvent, ItemCreated, ItemCheckedIn, ItemCheckedOut, CheckoutRejected,
    ItemRecounted, CheckoutRequested
)

class InventoryProcessor:
    """
    Processes inventory events, updates state, and enforces business rules.
    Implements optimistic concurrency with compensating transactions.
    """
    def __init__(self, 
                 state_store: InventoryStateStore, 
                 log: Log, 
                 dedup_store: DeduplicationStore):
        self.state_store = state_store
        self.log = log
        self.dedup_store = dedup_store

    async def process(self, record: StreamRecord) -> None:
        """
        Process a single record from the stream.
        """
        # 1. Deduplication check
        if await self.dedup_store.has_processed(record.id):
            return

        # 2. Parse Event
        try:
            event = self._parse_event(record)
        except Exception as e:
            # Log error and skip bad records
            print(f"Failed to parse event {record.id}: {e}")
            await self.dedup_store.mark_processed(record.id)
            return

        if not event:
            await self.dedup_store.mark_processed(record.id)
            return

        # 3. Apply to State
        self.state_store.apply_event(event)
        
        # 4. Check Invariants
        item_state = self.state_store.get_item(event.item_id)
        
        # Rule: No negative stock
        if item_state.qty < 0:
            # Invariant violated! We must compensate.
            # Assuming the violation was caused by the current event (ItemCheckedOut).
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
                
                # Create ID for rejection
                rejection_id = str(uuid.uuid4())
                
                # Create Record
                rejection_record = StreamRecord(
                    id=rejection_id,
                    key=rejection.item_id,
                    value=self._serialize_event(rejection),
                    event_type="CheckoutRejected",
                    timestamp=datetime.now(timezone.utc)
                )
                
                # Emit to Log
                await self.log.append(rejection_record)
                
                # Apply Immediate Compensation to State
                self.state_store.apply_event(rejection)
                
                # Mark Rejection ID as processed so we don't re-apply when we read it back
                await self.dedup_store.mark_processed(rejection_id)
            
            else:
                # Other events causing negative stock? (e.g. Recount).
                # For now, allow or log warning.
                pass

        # 5. Mark Processed
        await self.dedup_store.mark_processed(record.id)

    def _parse_event(self, record: StreamRecord) -> Optional[InventoryEvent]:
        # Map event_type string to Protobuf Class
        from protos import inventory_pb2
        
        event_type = getattr(record, "event_type", "")
        if not event_type:
             # Fallback for old records without type?
             return None
             
        try:
            if event_type == "ItemCreated":
                proto = inventory_pb2.ItemCreated()
                proto.ParseFromString(record.value)
                # Map to Domain Class
                return ItemCreated(
                    event_type="ITEM_CREATED",
                    item_id=proto.item_id,
                    timestamp=record.timestamp.isoformat(),
                    name=proto.name,
                    initial_qty=0 # Handled by separate checkin if needed
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
                    request_id=record.id # Use record ID as request ID
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
        # Inverse: Domain -> Protobuf -> Bytes
        # Used for emitting compensation events (CheckoutRejected)
        from protos import inventory_pb2
        
        if isinstance(event, CheckoutRejected):
            proto = inventory_pb2.CheckoutRejected(
                item_id=event.item_id,
                requested_qty=event.qty,
                available_qty=0, # TODO: Pass available
                reason=event.reason
            )
            return proto.SerializeToString()
        
        # Add others if pipeline needs to emit them
        return b""
