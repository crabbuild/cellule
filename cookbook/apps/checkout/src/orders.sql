CREATE TABLE orders(row_id INTEGER PRIMARY KEY,order_id BLOB UNIQUE NOT NULL CHECK(length(order_id)=16),order_bytes BLOB NOT NULL CHECK(length(order_bytes)<=4096)) STRICT;
CREATE TABLE order_messages(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),call_bytes BLOB NOT NULL CHECK(length(call_bytes)<=4096),reply_bytes BLOB NOT NULL CHECK(length(reply_bytes)<=4096)) STRICT;
