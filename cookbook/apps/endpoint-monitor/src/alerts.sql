CREATE TABLE alerts(row_id INTEGER PRIMARY KEY,edge_key BLOB NOT NULL UNIQUE CHECK(length(edge_key)=32),monitor BLOB NOT NULL,definition BLOB NOT NULL,edge BLOB NOT NULL) STRICT;
CREATE INDEX alerts_by_definition ON alerts(monitor,definition,row_id);
