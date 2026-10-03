-- Embedding-owned reference schema. Runtime wire bytes remain canonical.
CREATE TABLE IF NOT EXISTS state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format INTEGER NOT NULL CHECK (format = 1),
    scope BLOB NOT NULL CHECK (length(scope) = 48),
    profile BLOB NOT NULL CHECK (length(profile) = 32),
    head BLOB NOT NULL CHECK (length(head) <= 65536),
    registry BLOB NOT NULL CHECK (length(registry) <= 65536)
);
CREATE TABLE IF NOT EXISTS intents (key BLOB PRIMARY KEY CHECK (length(key)=16), body BLOB NOT NULL CHECK (length(body)<=65536));
CREATE TABLE IF NOT EXISTS operations (key BLOB PRIMARY KEY CHECK (length(key)=16), request BLOB NOT NULL CHECK (length(request)<=65536), body BLOB NOT NULL CHECK (length(body)<=65536));
CREATE TABLE IF NOT EXISTS progress (key BLOB PRIMARY KEY CHECK (length(key)=32), body BLOB NOT NULL CHECK (length(body)<=1048576));
CREATE TABLE IF NOT EXISTS enrollments (key BLOB PRIMARY KEY CHECK (length(key)=32), body BLOB NOT NULL CHECK (length(body)<=65536));
CREATE TABLE IF NOT EXISTS actions (
    key BLOB NOT NULL CHECK (length(key)=32), node BLOB NOT NULL CHECK (length(node)=16), session BLOB NOT NULL CHECK (length(session)=16),
    operation BLOB NOT NULL CHECK (length(operation)=16), sequence BLOB NOT NULL CHECK (length(sequence) IN (0,8)), effect INTEGER NOT NULL,
    accepted BLOB NOT NULL CHECK (length(accepted)<=65536), result BLOB CHECK (length(result)<=65536),
    PRIMARY KEY(key,node,session)
);
CREATE UNIQUE INDEX IF NOT EXISTS movement_index ON actions(operation,sequence,effect,node,session) WHERE length(sequence)=8;
CREATE TABLE IF NOT EXISTS bases (
    key BLOB NOT NULL, node BLOB NOT NULL, session BLOB NOT NULL, kind INTEGER NOT NULL CHECK (kind IN (1,2,3)),
    body BLOB NOT NULL CHECK (length(body)<=65536), PRIMARY KEY(key,node,session,kind),
    FOREIGN KEY(key,node,session) REFERENCES actions(key,node,session)
);

-- Role evidence shares registry revisions and the same accepted backend jobs.
CREATE TABLE IF NOT EXISTS reader_evacuations (
    key BLOB PRIMARY KEY CHECK(length(key)=32), body BLOB NOT NULL CHECK(length(body)<=1048576)
);
CREATE TABLE IF NOT EXISTS reader_evacuation_pages (
    key BLOB PRIMARY KEY CHECK(length(key)=32), body BLOB NOT NULL CHECK(length(body)<=65536)
);
CREATE TABLE IF NOT EXISTS latest_reader_evacuations (
    operation BLOB NOT NULL CHECK(length(operation)=16), original BLOB NOT NULL CHECK(length(original)=32),
    witness BLOB NOT NULL REFERENCES reader_evacuations(key), PRIMARY KEY(operation,original)
);
CREATE TABLE IF NOT EXISTS follower_policy (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    body BLOB NOT NULL CHECK(length(body) <= 65536)
);
CREATE TABLE IF NOT EXISTS follower_evacuations (
    key BLOB PRIMARY KEY CHECK(length(key) = 32),
    body BLOB NOT NULL CHECK(length(body) <= 1048576)
);
CREATE TABLE IF NOT EXISTS latest_follower_evacuations (
    operation BLOB NOT NULL CHECK(length(operation) = 16),
    original BLOB NOT NULL CHECK(length(original) = 32),
    witness BLOB NOT NULL REFERENCES follower_evacuations(key),
    PRIMARY KEY(operation, original)
);
