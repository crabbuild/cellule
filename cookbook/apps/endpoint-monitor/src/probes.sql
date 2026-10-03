CREATE TABLE probe_bindings(check_key BLOB PRIMARY KEY CHECK(length(check_key)=32),ticket BLOB NOT NULL) STRICT;
