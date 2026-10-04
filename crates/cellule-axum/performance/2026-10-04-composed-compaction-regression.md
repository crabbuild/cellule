# Composed compaction publication regression

The runtime pressure-append path constructs the final root directly from verified
compaction state. [Dataset](2026-10-04-composed-compaction-regression.json).
Baseline `4c1c982`; implementation `d5aebe3`.

| Pressure preparation | Baseline PUTs / HEADs | Candidate PUTs / HEADs | Exact restored rows |
| --- | ---: | ---: | ---: |
| Native append | 10 / 4 | 8 / 2 | 32 |
| Schema migration | 10 / 4 | 8 / 2 | 32 |

The same real publisher fixture publishes commands 1–31 and measures preparation
of command 32. Counters stop before authority CAS and verification. Both variants
verify the origin inventory and restore ordered rows before the reference path
can upload anything. Final roots match that path byte for byte; the original
authority predecessor and fenced publication remain intact.

Additional guards cover missing/corrupt origin objects, strict ceiling fallback,
partial directory updates, dependency upload failures, and cancellation through
scratch cleanup. Runtime/LTX tests, targeted Clippy, local LTX without replication,
API docs, architecture, and document/SQL/peer checks pass. The dataset retains
source and local log hashes; committed tests provide reproducible CI checks.

After integration with main's required root-lineage retention (`fa548bb`,
implementation `2ffe5bd`), the same fixture measures **11 PUTs / 4 HEADs**
on the baseline and **9 PUTs / 2 HEADs** on the candidate for both schemas.
These counts include the required lineage PUT. The lineage is confirmed with
only the original authority predecessor before CAS. The integrated runtime/LTX
suite passes 1,271 tests; metadata failure preserves its source and cannot return
a proposal. Both comparisons remain separately identified in the dataset.

These are operation counts. Sustained HTTP TPS and latency gains remain unverified.
Ordinary append costs are unchanged; larger representations may retain descriptor
pages or relocated directory nodes. Composed work/admission phases include
compaction/recovery: their overlapping populations differ from standalone append.
