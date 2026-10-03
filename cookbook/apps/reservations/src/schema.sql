CREATE TABLE event (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    event_key TEXT NOT NULL,
    seat_count INTEGER NOT NULL CHECK(seat_count BETWEEN 1 AND 100),
    revision INTEGER NOT NULL CHECK(revision > 0)
) STRICT;
CREATE TABLE seats (
    number INTEGER PRIMARY KEY CHECK(number BETWEEN 1 AND 100),
    generation INTEGER NOT NULL CHECK(generation >= 0),
    event_id INTEGER NOT NULL DEFAULT 1 CHECK(event_id=1) REFERENCES event(singleton)
) STRICT;
CREATE TABLE holds (
    id BLOB PRIMARY KEY CHECK(length(id)=16),
    seat INTEGER NOT NULL REFERENCES seats(number),
    generation INTEGER NOT NULL CHECK(generation > 0),
    buyer TEXT NOT NULL,
    deadline_ms INTEGER NOT NULL CHECK(deadline_ms > 0),
    state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 3),
    start_effect BLOB NOT NULL CHECK(length(start_effect)=32),
    UNIQUE(seat, generation)
) STRICT;
-- Scarce inventory and all competing transitions share this transaction domain.
CREATE UNIQUE INDEX occupied_seat ON holds(seat) WHERE state IN (0, 1);
