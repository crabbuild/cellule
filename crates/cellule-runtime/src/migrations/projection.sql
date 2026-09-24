CREATE TABLE projection_watermarks (
    source_cell BLOB PRIMARY KEY CHECK (length(source_cell) = 32),
    applied_through INTEGER NOT NULL CHECK (applied_through >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0)
) STRICT, WITHOUT ROWID;
