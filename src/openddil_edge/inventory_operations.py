# =============================================================================
# OpenDDIL Edge SDK (Python) — Inventory Operations
# =============================================================================
# High-level wrapper methods for UI developers. These methods construct the
# domain event, enqueue it in the Outbox, and return immediately — no network
# call required. The Relay handles flushing when connectivity allows.
# =============================================================================

import uuid
from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

# Generated from openddil-contracts: make python
from openddil.inventory.v1.inventory_events_pb2 import ItemAllocatedEvent

from openddil_edge.outbox_manager import OpenDdilOutboxManager


class InventoryOperations:
    """
    High-level inventory operations for Edge UI developers.
    All operations are local-first: they write to the SQLite Outbox and
    return immediately, even when fully disconnected.
    """

    def __init__(self, outbox: OpenDdilOutboxManager, user_id: str) -> None:
        self._outbox = outbox
        self._user_id = user_id

    def allocate_inventory(
        self,
        item_id: str,
        quantity: int,
        current_available_count: int,
    ) -> str:
        """
        Allocate inventory at the Edge. Creates an ItemAllocatedEvent and
        spools it to the local Outbox for eventual sync to HQ.

        This method works fully offline. The event will be flushed to HQ
        via Redpanda when connectivity is restored.

        Args:
            item_id: The inventory item ID to allocate from.
            quantity: Number of units to allocate.
            current_available_count: The available_count currently shown in the
                local SQLite read-cache (synced via ElectricSQL). Used for
                conflict detection at HQ.

        Returns:
            The event ID for tracking/correlation.
        """
        now = Timestamp()
        now.FromDatetime(datetime.now(timezone.utc))

        # Build the domain event
        evt = ItemAllocatedEvent(
            event_id=str(uuid.uuid4()),
            item_id=item_id,
            quantity=quantity,
            original_count_at_action=current_available_count,
            timestamp=now,
            user_id=self._user_id,
        )

        # Enqueue into the local Outbox (wrapped in CloudEvent envelope)
        event_id = self._outbox.enqueue(
            domain_event=evt,
            event_type="openddil.inventory.v1.ItemAllocatedEvent",
            user_id=self._user_id,
            subject=item_id,
        )

        print(
            f"[OUTBOX] Allocated {quantity} units from item {item_id} "
            f"(was {current_available_count}). Event {event_id} queued."
        )

        return event_id


# =============================================================================
# Usage Example
# =============================================================================
#
# with OpenDdilOutboxManager("openddil_edge.db", "edge-node-42") as outbox:
#     ops = InventoryOperations(outbox, "operator-jane")
#
#     # This works fully offline — writes to local SQLite only
#     ops.allocate_inventory(
#         item_id="550e8400-e29b-41d4-a716-446655440000",
#         quantity=5,
#         current_available_count=100,
#     )
#
#     # Check how many events are waiting to sync
#     print(f"Pending sync: {outbox.get_pending_count()} events")
# =============================================================================
