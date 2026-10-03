CREATE TABLE dashboard (
    project_key TEXT PRIMARY KEY CHECK(length(project_key) BETWEEN 1 AND 48),
    name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 120),
    revision INTEGER NOT NULL CHECK(revision > 0),
    issues INTEGER NOT NULL CHECK(issues BETWEEN 0 AND 32),
    open INTEGER NOT NULL CHECK(open BETWEEN 0 AND issues),
    attachments INTEGER NOT NULL CHECK(attachments BETWEEN 0 AND 64),
    digest BLOB NOT NULL CHECK(length(digest) = 32)
) WITHOUT ROWID;
