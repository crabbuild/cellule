CREATE TABLE provider_resources(resource_id BLOB PRIMARY KEY CHECK(length(resource_id)=16),record BLOB NOT NULL CHECK(length(record)<=4096),phase INTEGER NOT NULL CHECK(phase IN (0,1,2,3)),due_at_ms INTEGER,CHECK((phase IN (0,2) AND due_at_ms>0) OR (phase IN (1,3) AND due_at_ms IS NULL))) STRICT;
CREATE INDEX provider_due ON provider_resources(due_at_ms,resource_id) WHERE phase IN (0,2);
