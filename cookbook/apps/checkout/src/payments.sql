CREATE TABLE payments(order_id BLOB PRIMARY KEY CHECK(length(order_id)=16),payment BLOB NOT NULL CHECK(length(payment)<=4096)) STRICT;
