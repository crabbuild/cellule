CREATE TABLE availability (id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL CHECK(enabled IN(0,1)));
INSERT INTO availability VALUES (1,1);
CREATE TABLE deliveries (
    job TEXT NOT NULL, dead INTEGER NOT NULL CHECK(dead IN(0,1)), body TEXT NOT NULL,
    PRIMARY KEY(job,dead)
);
