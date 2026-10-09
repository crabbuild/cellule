# Follower validation reuse: retry integrity

The validation-reuse candidate exposed two retry defects: after rejecting a
missing open-log prefix or a valid substituted sealed frame, clearing the
in-memory index allowed a later retry to accept the changed history. Both
regressions reproduced against the committed candidate through public
`FollowerStore` append and whole/paged tail operations.

The fix retains original validated digest/length witnesses after errors and
reconciles disk locations before granting new durability credit. Reconciliation
persists valid records left by an interrupted append before indexing or sealing
them. Exact byte repair permits retry; authority-verified object coverage can
release witnesses only for its covered prefix. Successful retirement and store
shutdown still release index reservations.

The isolated snapshot matches all 1,233 tracked Rust/Cargo inputs. All 20
follower unit tests pass, including repeated rejection, exact repair, partial
append followed by seal/cold recovery, and covered-prefix release. The full
runtime suite passes: 646 unit tests and 408 integration tests, including 241
public runtime scenarios; the 10 existing ignored tests remain unchanged.
Strict all-targets/all-features runtime Clippy and API docs pass, as do format,
boundary/layout, Markdown links, Rust fences and SQL/peer contract checks.
External raw evidence uses
the `follower-retry-integrity-*-v3` prefix; the original failing test log and
input hashes remain preserved separately.

This records a correctness prerequisite for the next performance run. It does
not establish an HTTP throughput gain or target capacity. The previous
[failed balanced control](2026-10-05-follower-verified-r12-failed-control.md)
remains a failed control, and the previous
[native validation-cost comparison](2026-10-05-follower-verified-reuse.md)
uses its original frozen sources. The dedicated-node target and its original
qualification gates remain open.

The preceding PR head (`edd5ccb`) also failed its
[object-only routing CI gate](https://github.com/crabbuild/cellule/actions/runs/37411495723/job/112102808138).
All four measurement pairs executed successfully, but the single-client local
command median comparison had throughput ratio 0.827 and p99 ratio 2.395.
The original minimum throughput ratio 0.90 and maximum p99 ratio 2.0 remain
unchanged. Its frozen baseline and candidate merge revisions, binary/source
hashes and comparison values are recorded in the adjacent JSON; the complete
artifact and logs remain external. Causality is still under investigation;
these measurements precede this retry fix.
