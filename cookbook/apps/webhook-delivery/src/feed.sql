CREATE TABLE subscriptions(
    subscription_key TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),
    revision INTEGER NOT NULL CHECK(revision>0)
) STRICT;
CREATE INDEX subscription_topic ON subscriptions(topic,enabled,subscription_key);
CREATE TABLE published_events(
    event_id BLOB PRIMARY KEY CHECK(length(event_id)=16),
    record BLOB NOT NULL CHECK(length(record) BETWEEN 1 AND 32768)
) STRICT;
