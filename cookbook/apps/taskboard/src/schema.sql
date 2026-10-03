CREATE TABLE tasks (
    id INTEGER PRIMARY KEY CHECK (id > 0),
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
    assignee TEXT CHECK (assignee IS NULL OR length(assignee) BETWEEN 1 AND 80),
    closed INTEGER NOT NULL DEFAULT 0 CHECK (closed IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0)
);
