CREATE TABLE monitor_ids(monitor BLOB PRIMARY KEY CHECK(length(monitor)=16)) STRICT;
CREATE TABLE definitions(version BLOB PRIMARY KEY CHECK(length(version)=16),change BLOB NOT NULL,generation INTEGER NOT NULL CHECK(generation>0)) STRICT;
