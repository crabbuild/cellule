CREATE TABLE project (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    project_key TEXT NOT NULL UNIQUE CHECK(length(project_key) BETWEEN 1 AND 48),
    name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 120),
    revision INTEGER NOT NULL CHECK(revision > 0),
    effect_id BLOB NOT NULL CHECK(length(effect_id) = 32)
);
CREATE TABLE issues (
    issue_id TEXT PRIMARY KEY CHECK(length(issue_id) BETWEEN 1 AND 48),
    title TEXT NOT NULL CHECK(length(CAST(title AS BLOB)) BETWEEN 1 AND 120),
    description TEXT NOT NULL CHECK(length(CAST(description AS BLOB)) BETWEEN 1 AND 512),
    assignee TEXT CHECK(assignee IS NULL OR length(assignee) BETWEEN 1 AND 48),
    status INTEGER NOT NULL CHECK(status IN (1, 2)),
    revision INTEGER NOT NULL CHECK(revision > 0)
) WITHOUT ROWID;
CREATE TABLE attachments (
    issue_id TEXT NOT NULL REFERENCES issues(issue_id),
    attachment_id TEXT NOT NULL CHECK(length(attachment_id) BETWEEN 1 AND 48),
    publication BLOB NOT NULL CHECK(length(publication) BETWEEN 1 AND 1024),
    PRIMARY KEY(issue_id, attachment_id)
) WITHOUT ROWID;
