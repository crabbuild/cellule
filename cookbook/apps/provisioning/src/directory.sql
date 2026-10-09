CREATE TABLE resources(row_id INTEGER PRIMARY KEY,resource_id BLOB UNIQUE NOT NULL CHECK(length(resource_id)=16),record BLOB NOT NULL CHECK(length(record)<=4096)) STRICT;
CREATE TABLE directory_messages(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),call_bytes BLOB NOT NULL CHECK(length(call_bytes)<=4096),reply_bytes BLOB NOT NULL CHECK(length(reply_bytes)<=4096)) STRICT;
