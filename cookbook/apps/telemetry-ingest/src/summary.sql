CREATE TABLE devices (
 device_key TEXT PRIMARY KEY,
 revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 129),
 accepted INTEGER NOT NULL CHECK(accepted BETWEEN 0 AND 128),
 snapshot BLOB NOT NULL CHECK(length(snapshot)<=4096)
) STRICT;
CREATE TABLE buckets (
 device_key TEXT NOT NULL REFERENCES devices(device_key),
 start_ms INTEGER NOT NULL CHECK(start_ms>=60000 AND start_ms%60000=0),
 count INTEGER NOT NULL CHECK(count BETWEEN 0 AND 128),
 sum_milli INTEGER NOT NULL CHECK(sum_milli BETWEEN -128000000 AND 128000000),
 PRIMARY KEY(device_key,start_ms)
) STRICT;
CREATE INDEX bucket_time ON buckets(start_ms);
