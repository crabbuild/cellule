CREATE TABLE quota_account (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    customer TEXT NOT NULL UNIQUE CHECK (length(customer) BETWEEN 1 AND 64),
    allowance INTEGER NOT NULL CHECK (allowance BETWEEN 0 AND 1000000000000),
    consumed INTEGER NOT NULL CHECK (consumed >= 0),
    reserved INTEGER NOT NULL CHECK (reserved >= 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    reservation_count INTEGER NOT NULL CHECK (reservation_count BETWEEN 0 AND 1024),
    CHECK (consumed <= allowance AND reserved <= allowance - consumed)
) STRICT;
CREATE TABLE reservations (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    account_id INTEGER NOT NULL DEFAULT 1 CHECK (account_id = 1) REFERENCES quota_account(singleton),
    credits INTEGER NOT NULL CHECK (credits BETWEEN 1 AND 1000000000000),
    state INTEGER NOT NULL CHECK (state IN (0, 1, 2))
) STRICT;
