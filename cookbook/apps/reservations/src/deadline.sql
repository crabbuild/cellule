CREATE TABLE deadline_bindings(
    workflow_key BLOB PRIMARY KEY CHECK(length(workflow_key)=32),
    ticket BLOB NOT NULL CHECK(length(ticket) BETWEEN 1 AND 1024)
) STRICT;
