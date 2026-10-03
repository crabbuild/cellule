CREATE TABLE receiver_policies(
    subscription_key TEXT PRIMARY KEY,
    mode INTEGER NOT NULL CHECK(mode BETWEEN 0 AND 3)
) STRICT;
CREATE TABLE received_deliveries(
    delivery_key BLOB PRIMARY KEY CHECK(length(delivery_key)=32),
    ticket BLOB NOT NULL CHECK(length(ticket) BETWEEN 1 AND 4096),
    requests INTEGER NOT NULL CHECK(requests BETWEEN 1 AND 20),
    applied INTEGER NOT NULL CHECK(applied IN(0,1))
) STRICT;
