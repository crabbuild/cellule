# Shared directory cache and cold-start pressure

A diagnostic replay of the retained 2,000-Cell r4 dataset reproduces
`Capacity("node pressure")` with 993 active Cells. The runtime reports
89,481,216 resident bytes, zero retained bytes and zero active jobs, but
706,099,184 reserved disk bytes against the unchanged 1 GiB envelope. Local
admission is Constrained. These counters identify disk pressure; they are not
RSS or completed full-fleet restore evidence.

Each activation previously opened the same persistent directory cache again.
Every live owner reloaded its membership and reserved the same files against
the shared disk ledger. The canonical host regression demonstrates the cause:
21 cloned hosts charge 168 bytes for one physical 8-byte cache entry.

Clones now reuse one live cache for the same directory, filesystem and disk
budget. They join accepted fills before opening, preserving the tested ordering.
Weak registration retains no cache or disk reservation after the last owner
drops. Reopening then reconstructs persistent membership through admitted
blocking work. Existing authentication, cache bounds and response gates remain.

The regression now charges eight bytes once, shares subsequent membership,
releases the charge after the final owner drops, and reaccounts both entries
on reopening. Separate tests cover 32 concurrent opens and distinct selected
disk budgets. Isolated macOS checks pass: 123 LTX unit tests, 75 Cell tests,
72 host tests, 47 local-only unit tests, all runtime suites (including 619 unit
and 234 runtime integration tests), and strict LTX/runtime/Axum Clippy.
Existing ignored tests and qualification expectations remain unchanged.

The [dataset](2026-10-05-shared-directory-cache.json) records the exact counters,
probe binary hash and external evidence references. The diagnostic moved into
the runtime because modifying the example changes its persisted application
code identity. An earlier probe was correctly rejected with CatalogCollision;
it supplies no recovery evidence.

Colima stopped before the fixed RustFS replay. Post-fix throughput, latency,
all-Cell cold audit and owner-loss qualification remain unverified. The
[node capacity target](node-capacity.md) remains open. Publication admission
and compaction waits from the [previous report](2026-10-05-node-lease-renewals.md)
still need controlled steady-state measurements after recovery is repaired.
