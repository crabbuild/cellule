CREATE TABLE project (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 128),
    description TEXT NOT NULL CHECK (length(description) <= 512),
    revision INTEGER NOT NULL CHECK (revision > 0),
    updated_by TEXT NOT NULL CHECK (length(updated_by) BETWEEN 1 AND 48)
);
