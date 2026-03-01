# =============================================================================
# OpenDDIL Edge SDK (Python) — Outbox Manager
# =============================================================================
# Manages the local SQLite Outbox for spooling events in DDIL environments.
# Events are serialized as Protobuf binaries and inserted into the Outbox.
# The Relay (separate component) flushes pending events to HQ when connected.
# =============================================================================

import sqlite3
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp

# Generated from openddil-contracts: make python
from openddil.events.v1.cloud_event_pb2 import CloudEvent


class OpenDdilOutboxManager:
    """
    Manages the local SQLite Outbox for spooling CloudEvent-wrapped domain
    events. Designed for DDIL environments where connectivity is unreliable.

    Usage:
        outbox = OpenDdilOutboxManager("openddil_edge.db", "edge-node-42")
        outbox.enqueue(domain_event, "openddil.inventory.v1.ItemAllocatedEvent", "user-1")
    """

    _SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS outbox_events (
        id              TEXT    PRIMARY KEY,
        event_type      TEXT    NOT NULL,
        payload_bytes   BLOB    NOT NULL,
        created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        is_processed    INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_outbox_pending
        ON outbox_events (is_processed, created_at)
        WHERE is_processed = 0;
    """

    def __init__(self, db_path: str, edge_node_id: str) -> None:
        """
        Initialize the Outbox Manager, creating the Outbox table if needed.

        Args:
            db_path: Path to the local SQLite database file.
            edge_node_id: Unique identifier for this Edge node.
        """
        self._edge_node_id = edge_node_id
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
        self._initialize_schema()
        self._sequence = self._get_max_sequence()

    def enqueue(
        self,
        domain_event: Message,
        event_type: str,
        user_id: str,
        subject: Optional[str] = None,
    ) -> str:
        """
        Enqueue a domain event into the local Outbox, wrapped in a CloudEvent.
        The event will be flushed to HQ by the Relay when connectivity allows.

        Args:
            domain_event: A Protobuf message (e.g., ItemAllocatedEvent).
            event_type: CloudEvent type URI (e.g., "openddil.inventory.v1.ItemAllocatedEvent").
            user_id: The operator who triggered this event.
            subject: The entity ID this event is about (optional).

        Returns:
            The event ID (UUID) assigned to this event.
        """
        event_id = str(uuid.uuid4())
        now = Timestamp()
        now.FromDatetime(datetime.now(timezone.utc))
        self._sequence += 1

        # Pack the domain event into google.protobuf.Any
        any_payload = Any()
        any_payload.Pack(domain_event)

        # Wrap in CloudEvent envelope
        cloud_event = CloudEvent(
            id=event_id,
            source=f"openddil://edge/{self._edge_node_id}",
            spec_version="1.0",
            type=event_type,
            time=now,
            data_content_type="application/protobuf",
            subject=subject or "",
            user_id=user_id,
            edge_node_id=self._edge_node_id,
            sequence=self._sequence,
            data=any_payload,
        )

        # Serialize the full CloudEvent envelope to Protobuf binary
        payload_bytes = cloud_event.SerializeToString()

        # Insert into the local SQLite Outbox
        self._conn.execute(
            "INSERT INTO outbox_events (id, event_type, payload_bytes) VALUES (?, ?, ?)",
            (event_id, event_type, payload_bytes),
        )
        self._conn.commit()

        return event_id

    def get_pending_events(self, batch_size: int = 100) -> List[Tuple[str, bytes]]:
        """
        Retrieve all pending (unflushed) events from the Outbox.
        Used by the Relay to flush events to HQ.

        Args:
            batch_size: Maximum number of events to retrieve.

        Returns:
            List of (id, payload_bytes) tuples.
        """
        cursor = self._conn.execute(
            """
            SELECT id, payload_bytes
            FROM outbox_events
            WHERE is_processed = 0
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (batch_size,),
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]

    def mark_as_processed(self, event_ids: List[str]) -> None:
        """
        Mark events as flushed after the Relay has successfully sent them to HQ.

        Args:
            event_ids: The IDs of successfully flushed events.
        """
        self._conn.execute("BEGIN")
        try:
            for event_id in event_ids:
                self._conn.execute(
                    "UPDATE outbox_events SET is_processed = 1 WHERE id = ?",
                    (event_id,),
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def get_pending_count(self) -> int:
        """
        Returns the count of pending (unflushed) events in the Outbox.
        Useful for UI status indicators.
        """
        cursor = self._conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE is_processed = 0"
        )
        return cursor.fetchone()[0]

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _initialize_schema(self) -> None:
        self._conn.executescript(self._SCHEMA_SQL)

    def _get_max_sequence(self) -> int:
        cursor = self._conn.execute("SELECT COUNT(*) FROM outbox_events")
        return cursor.fetchone()[0]
