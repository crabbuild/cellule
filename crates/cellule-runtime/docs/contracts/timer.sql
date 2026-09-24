CREATE TABLE timer_entries (
    timer_id BLOB PRIMARY KEY CHECK (length(timer_id) = 16),
    target_index INTEGER NOT NULL CHECK (target_index >= 0),
    target_partition BLOB NOT NULL CHECK (length(target_partition) <= 1024),
    payload BLOB NOT NULL CHECK (length(payload) <= 262144),
    due_at_ms INTEGER NOT NULL CHECK (due_at_ms >= 0),
    generation INTEGER NOT NULL CHECK (generation >= 1),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0)
) STRICT, WITHOUT ROWID;
CREATE INDEX timer_due ON timer_entries(due_at_ms, timer_id);
