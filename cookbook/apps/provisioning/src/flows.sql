CREATE TABLE flow_bindings(resource_id BLOB PRIMARY KEY CHECK(length(resource_id)=16),spec BLOB NOT NULL CHECK(length(spec)<=2048),run_id BLOB NOT NULL CHECK(length(run_id)=16),terminal INTEGER NOT NULL CHECK(terminal IN (0,1))) STRICT;
CREATE TABLE deletion_bindings(resource_id BLOB PRIMARY KEY CHECK(length(resource_id)=16),spec BLOB NOT NULL CHECK(length(spec)<=2048)) STRICT;
CREATE TABLE flow_replies(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),reply BLOB NOT NULL CHECK(length(reply)<=4096)) STRICT;
CREATE TABLE reconciliations(resource_id BLOB NOT NULL CHECK(length(resource_id)=16),token BLOB NOT NULL CHECK(length(token)=16),PRIMARY KEY(resource_id,token)) STRICT;
