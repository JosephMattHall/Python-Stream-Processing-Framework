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
            event_data = record.value
            event_type = event_data.get("event_type")
            event = self._parse_event(event_data)
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

    def _parse_event(self, data: dict) -> Optional[InventoryEvent]:
        # Simple parser mapping
        mapping = {
            "ITEM_CREATED": ItemCreated,
            "ITEM_CHECKED_IN": ItemCheckedIn,
            "ITEM_CHECKED_OUT": ItemCheckedOut,
            "CHECKOUT_REJECTED": CheckoutRejected
        }
        cls = mapping.get(data.get("event_type"))
        if cls:
            # filter args based on cls (simple dict unpacking might fail if extra fields)
            # kept simple for MVP
            return cls(**data)
        return None

    def _serialize_event(self, event: InventoryEvent) -> dict:
        return event.__dict__
