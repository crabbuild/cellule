CREATE TABLE period (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    spec BLOB NOT NULL,
    state INTEGER NOT NULL CHECK(state IN (0, 1, 2, 3)),
    sealed_report BLOB
) STRICT;
CREATE TABLE period_members (
    account_key TEXT PRIMARY KEY,
    ready INTEGER NOT NULL CHECK(ready IN (0, 1)),
    reconciled BLOB
) STRICT;
CREATE TABLE projected_events (
    account_key TEXT NOT NULL,
    event_id BLOB NOT NULL CHECK(length(event_id) = 16),
    event_bytes BLOB NOT NULL,
    PRIMARY KEY(account_key, event_id)
) STRICT;
