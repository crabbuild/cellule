CREATE TABLE checks(row_id INTEGER PRIMARY KEY,check_key BLOB NOT NULL UNIQUE CHECK(length(check_key)=32),monitor BLOB NOT NULL CHECK(length(monitor)=16),definition BLOB NOT NULL CHECK(length(definition)=16),check_bytes BLOB NOT NULL,outcome BLOB NOT NULL) STRICT;
CREATE INDEX checks_by_definition ON checks(monitor,definition,row_id);
CREATE TABLE monitor_state(monitor BLOB NOT NULL,definition BLOB NOT NULL,state BLOB NOT NULL,PRIMARY KEY(monitor,definition)) STRICT;
CREATE TABLE incidents(incident BLOB PRIMARY KEY CHECK(length(incident)=32),opening BLOB NOT NULL CHECK(length(opening)=32),closing BLOB CHECK(closing IS NULL OR length(closing)=32)) STRICT;
CREATE TABLE edges(edge_key BLOB PRIMARY KEY CHECK(length(edge_key)=32),edge BLOB NOT NULL,effect_id BLOB NOT NULL CHECK(length(effect_id)=32)) STRICT;
