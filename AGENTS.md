# AGENTS.md — OpenDDIL Edge SDK (Python)

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains the **Python Edge SDK** — the client-side code that runs on disconnected Edge devices. It manages the local SQLite Outbox for spooling events.

## What You CAN Do

- **Add new operations modules** for new bounded contexts (e.g., `logistics_operations.py`).
- **Add new methods** to existing operations classes for new event types.
- **Modify `OpenDdilOutboxManager`** to add utility methods (e.g., purge processed events).
- **Update documentation** (README, llms.txt, .cursorrules, this file).

## What You MUST NOT Do

- ❌ **Never make network calls**. This SDK is local-only. The Relay handles network I/O.
- ❌ **Never define Protobuf messages here**. All contracts live in `openddil-contracts`.
- ❌ **Never modify the Outbox schema** without making the same change in `openddil-edge-dotnet`.
- ❌ **Never store raw JSON** in payload_bytes. Always use Protobuf binary serialization.
- ❌ **Never skip the CloudEvent envelope**. HQ processors expect every event to be wrapped.
- ❌ **Never delete or modify the `id` column semantics**. UUIDs are used for end-to-end idempotency.
- ❌ **Never use f-strings or % formatting for SQL**. Always use parameterized queries (?).

## Adding a New Event Type

1. Ensure the Protobuf message exists in `openddil-contracts`.
2. Run `make python` in `openddil-contracts` to regenerate Python classes.
3. Create or update an operations module (e.g., `inventory_operations.py`).
4. The new method should:
   - Construct the domain event with all fields
   - Accept the current read-cache values for conflict detection
   - Call `self._outbox.enqueue()` with the correct `event_type` URI
   - Return the event ID
5. Update README and llms.txt.

## Symmetry Rule

> ⚠️ The Python and .NET Edge SDKs MUST remain symmetric. Any change to the Outbox schema, CloudEvent wrapping logic, or operations API surface MUST be mirrored in `openddil-edge-dotnet`.
