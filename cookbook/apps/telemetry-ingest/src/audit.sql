CREATE TABLE batches (
 message_id BLOB PRIMARY KEY CHECK(length(message_id)=16),
 completion BLOB NOT NULL CHECK(length(completion)<=65536)
) STRICT;
