# Quiet compaction admission

The retained [2,000-Cell r4 run](2026-10-05-node-lease-renewals.md) reports
117.36 durable writes/s and write HTTP p50/p95/p99 of 117.2/2,036/9,322.8 ms.
Its quiet-compaction mean is 11,625.6 ms; root admission averages 3,408.6 ms
versus 45.9 ms of admitted root work. These populations have different
boundaries; their means cannot be added as latency shares. That run failed
cold recovery and does not qualify node capacity.

The actor previously claimed every eligible quiet Cell's publisher and marked
it busy before waiting for shared compaction memory admission. With two
recovery slots, many independent Cells could wait without executing compaction
while their foreground queries and writes queued. The existing LTX pair gate
already releases partial pairs; the reproduced defect is early Cell exclusion.

A public runtime regression holds both recovery slots, acknowledges eight
writes on each of four independent Cells, waits for quiet-compaction eligibility,
then requires a read and ninth durable write on every Cell within one second.
The old production source fails that deadline. The fixed source passes, drains
all Cells to Idle, and restores their exact sequence-nine roots with value nine.
The final fixture isolates its dirty/IO/blocking pools at the same existing
capacities (8/32/8); recovery remains two and the deadline is unchanged.

The actor now tries shared admission synchronously before taking the publisher
or spawning a compaction task. Unavailable capacity leaves the Cell available
and uses the existing one-second retry interval. Fresh compactions yield to
queued pair negotiations. Admitted work retains both reservations through the
existing preparation, lease check and exact-root publication path; it remains
exclusive on its Cell. No resource ceiling or response gate changes.

Isolated checks pass: 124 LTX unit tests, 75 Cell tests, 73 host tests, 47
local-only unit tests, all runtime suites (619 unit and 235 runtime integration
tests), strict LTX/runtime/Axum Clippy and API docs. Cancellation coverage checks
both ordinary and pre-admitted preparation across six paused native operations
and two composition modes: reservations and scratch survive dispatched work
and release after cleanup. Existing ignored tests and qualification gates remain.

The [dataset](2026-10-05-compaction-admission.json) retains source hashes and
red/green evidence references. Post-fix RustFS TPS, latency, all-Cell cold audit
and owner-loss qualification are unverified while Colima is stopped. Compaction
timing now starts after admission, so it cannot be compared directly with the
old admission-inclusive duration. Compare HTTP journals under the same workload
and budgets; deferred attempts are not completed compactions. The full
[node capacity target](node-capacity.md) remains open.
