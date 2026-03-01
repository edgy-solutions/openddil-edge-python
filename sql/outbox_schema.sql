-- =============================================================================
-- OpenDDIL Edge — SQLite Outbox Schema
-- =============================================================================
-- Identical to openddil-edge-dotnet/sql/outbox_schema.sql.
-- Local write-side event spool for DDIL environments.
-- =============================================================================

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
