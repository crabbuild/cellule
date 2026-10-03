CREATE TABLE release_records(release_id BLOB PRIMARY KEY CHECK(length(release_id)=16),record BLOB NOT NULL CHECK(length(record)<=8192)) STRICT;
CREATE TABLE release_messages(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),projection BLOB NOT NULL CHECK(length(projection)<=8192)) STRICT;
