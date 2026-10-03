CREATE TABLE ticket (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    ticket_key TEXT NOT NULL UNIQUE CHECK(length(ticket_key) BETWEEN 1 AND 48),
    state BLOB NOT NULL CHECK(length(state) BETWEEN 1 AND 131072)
) STRICT;
CREATE TABLE opening (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    ticket BLOB NOT NULL CHECK(length(ticket) BETWEEN 1 AND 4096)
) STRICT;
CREATE TABLE messages (
    sequence INTEGER PRIMARY KEY CHECK(sequence BETWEEN 1 AND 64),
    message_id TEXT NOT NULL UNIQUE CHECK(length(message_id) BETWEEN 1 AND 48),
    body BLOB NOT NULL CHECK(length(body) BETWEEN 1 AND 16384)
) STRICT;
