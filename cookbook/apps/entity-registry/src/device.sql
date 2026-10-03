CREATE TABLE device (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    device_key TEXT NOT NULL UNIQUE CHECK (length(device_key) BETWEEN 1 AND 64),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    location TEXT NOT NULL CHECK (length(location) BETWEEN 1 AND 120),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    revision INTEGER NOT NULL CHECK (revision > 0),
    effect_id BLOB NOT NULL CHECK (length(effect_id) = 32)
) STRICT;
