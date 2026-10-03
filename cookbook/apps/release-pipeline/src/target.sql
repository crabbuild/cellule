CREATE TABLE target_slots (
    name TEXT PRIMARY KEY CHECK(length(name) BETWEEN 1 AND 32),
    record BLOB NOT NULL CHECK(length(record) <= 32768)
) WITHOUT ROWID;
CREATE TABLE target_operations (
    release_id BLOB PRIMARY KEY CHECK(length(release_id) = 16),
    record BLOB NOT NULL CHECK(length(record) <= 32768)
) WITHOUT ROWID;
CREATE TABLE target_artifacts (
    release_id BLOB PRIMARY KEY CHECK(length(release_id) = 16),
    payload BLOB NOT NULL CHECK(length(payload) BETWEEN 37 AND 4132)
) WITHOUT ROWID;
