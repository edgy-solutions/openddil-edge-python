# OpenDDIL Edge SDK (Python)

Python client SDK for OpenDDIL — manages the local SQLite **Outbox** for spooling events in DDIL (Denied, Degraded, Intermittent, Limited) environments.

## Architecture

```mermaid
graph LR
    UI["Edge UI / App"] --> OPS["InventoryOperations"]
    OPS --> OM["OpenDdilOutboxManager"]
    OM --> DB["SQLite Outbox"]
    DB --> RELAY["Relay (flush)"]
    RELAY --> RP["Redpanda (HQ)"]

    ESQL["ElectricSQL"] --> RC["SQLite Read-Cache"]
    RC --> UI
```

## How It Works

1. **UI calls `allocate_inventory()`** — constructs a Protobuf event
2. **OutboxManager wraps it** in a CloudEvent envelope and serializes to binary
3. **Inserts into local SQLite** `outbox_events` table — works fully offline
4. **Relay (separate component)** queries pending events and flushes to Redpanda when connected
5. **ElectricSQL** syncs the updated Postgres state back to the local read-cache

## Key Files

| File | Purpose |
|---|---|
| `src/openddil_edge/outbox_manager.py` | Core Outbox manager — enqueue, get pending, mark processed |
| `src/openddil_edge/inventory_operations.py` | High-level `allocate_inventory()` wrapper for UI developers |
| `src/openddil_edge/relay_agent.py` | Async background agent that flushes Outbox → Redpanda with exponential backoff |
| `sql/outbox_schema.sql` | SQLite schema for the `outbox_events` table |

## Quick Start

```python
from openddil_edge.outbox_manager import OpenDdilOutboxManager
from openddil_edge.inventory_operations import InventoryOperations

with OpenDdilOutboxManager("openddil_edge.db", "edge-node-42") as outbox:
    ops = InventoryOperations(outbox, "operator-jane")

    # Works fully offline — writes to local SQLite only
    ops.allocate_inventory(
        item_id="550e8400-e29b-41d4-a716-446655440000",
        quantity=5,
        current_available_count=100,
    )

    # Check sync status
    print(f"Pending sync: {outbox.get_pending_count()} events")
```

## Dependencies

- `sqlite3` (stdlib) — SQLite access
- `protobuf` — Protobuf serialization (`pip install protobuf`)
- `aiokafka` — Async Kafka/Redpanda producer (`pip install aiokafka`)
- Generated classes from `openddil-contracts` (`make python`)

## AI Documentation

| File | Purpose |
|---|---|
| [`llms.txt`](llms.txt) | Structured project summary for LLM discovery |
| [`.cursorrules`](.cursorrules) | Python coding style and SDK conventions |
| [`AGENTS.md`](AGENTS.md) | AI agent safety guidelines |
