CREATE TABLE account (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    account_key TEXT NOT NULL,
    period_id BLOB NOT NULL CHECK(length(period_id) = 16),
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    state INTEGER NOT NULL CHECK(state IN (0, 1, 2)),
    close_snapshot BLOB
) STRICT;
CREATE TABLE usage_events (
    event_id BLOB PRIMARY KEY CHECK(length(event_id) = 16),
    event_bytes BLOB NOT NULL
) STRICT;
