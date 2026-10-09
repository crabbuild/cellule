CREATE TABLE saga_bindings(order_id BLOB PRIMARY KEY CHECK(length(order_id)=16),spec BLOB NOT NULL CHECK(length(spec)<=2048),run_id BLOB NOT NULL CHECK(length(run_id)=16)) STRICT;
CREATE TABLE saga_replies(message_key BLOB PRIMARY KEY CHECK(length(message_key)=32),reply BLOB NOT NULL CHECK(length(reply)<=4096)) STRICT;
CREATE TABLE reconciliations(order_id BLOB NOT NULL CHECK(length(order_id)=16),token BLOB NOT NULL CHECK(length(token)=16),PRIMARY KEY(order_id,token)) STRICT;
