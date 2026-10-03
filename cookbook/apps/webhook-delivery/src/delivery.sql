-- Application-owned binding survives native terminal Workflow history retention.
CREATE TABLE delivery_bindings(
    delivery_key BLOB PRIMARY KEY CHECK(length(delivery_key)=32),
    ticket BLOB NOT NULL CHECK(length(ticket) BETWEEN 1 AND 4096)
) STRICT;
