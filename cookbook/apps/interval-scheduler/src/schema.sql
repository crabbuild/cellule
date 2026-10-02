CREATE TABLE reminders (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule TEXT NOT NULL, definition TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation>0),
    occurrence INTEGER NOT NULL CHECK(occurrence>0),
    scheduled_at_ms INTEGER NOT NULL CHECK(scheduled_at_ms>=0),
    title TEXT NOT NULL, message TEXT NOT NULL,
    UNIQUE(schedule,definition,generation,occurrence)
);
CREATE INDEX reminder_pages ON reminders(schedule,row_id);
