CREATE TABLE release_bindings(release_id BLOB PRIMARY KEY CHECK(length(release_id)=16),spec BLOB NOT NULL CHECK(length(spec)<=32768),run_id BLOB NOT NULL CHECK(length(run_id)=16),terminal INTEGER NOT NULL CHECK(terminal IN (0,1))) STRICT;
CREATE TABLE rollback_bindings(release_id BLOB PRIMARY KEY CHECK(length(release_id)=16),spec BLOB NOT NULL CHECK(length(spec)<=32768)) STRICT;
CREATE TABLE approval_bindings(release_id BLOB PRIMARY KEY CHECK(length(release_id)=16),vote BLOB NOT NULL CHECK(length(vote)<=512)) STRICT;
CREATE TABLE release_replies(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),reply BLOB NOT NULL CHECK(length(reply)<=8192)) STRICT;
CREATE TABLE release_reconciliations(release_id BLOB NOT NULL CHECK(length(release_id)=16),token BLOB NOT NULL CHECK(length(token)=16),PRIMARY KEY(release_id,token)) STRICT;
