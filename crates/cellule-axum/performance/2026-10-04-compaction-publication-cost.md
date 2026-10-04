# Intermediate compaction publication cost

An isolated probe of the real runtime pressure-append path finds redundant
metadata uploads. [Dataset](2026-10-04-compaction-publication-cost.json).
Production sources match merged main `4c1c9826322b154a4dd2af6f86b89cd586e871a7`.

| Operation | PUTs | Metadata HEADs | New objects | New root metadata outside final inventory | Restored rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native append after compaction | 10 | 4 | 9 | 2 | 39 |
| Schema migration after compaction | 10 | 4 | 9 | 2 | 39 |

Both probes verify the final dependency inventory at origin, retain authority
and schema assertions, and restore 39 rows into a fresh SQLite file. They measure
a valid append without first preparing the fixture's intentional invalid sequence.
Provider counters cover the whole preparation; the replica ledger is drained
mid-operation by compaction telemetry and reports only the final five-object append.

Two new root metadata objects and one new directory object are absent from the
final inventory. The next implementation should construct the final append root
from verified compaction state, avoiding upload and reopening of intermediate
root metadata. Persisted formats and the original authority predecessor must
remain unchanged. Directory relocation must stay bounded; compacted directory
nodes can remain necessary when an append changes only some pages.

This is an operation-count diagnostic, not timed HTTP evidence. No removal or
speedup is established yet; both require regression tests and sustained paired runs.
