# Write optimization implementation record

This branch implements the [write performance design](../../cellule-runtime/docs/write-performance-design.md)
on Cellule base `e07670e2348231ed401cc7280a47e3ab97596ffe`. It adds bounded
execution, follower correctness fixes, phase attribution and runnable Docker
diagnostics. **Celld parity and the simultaneous node capacity target remain
unqualified.** Later changes on `main` require integration and new qualification.

This record keeps implementation decisions, representative results and their
limits. Raw CSVs, container logs, binaries, frozen sources and complete receipts
remain external benchmark artifacts; they are not repository contents.

## Measurement implementation

Runtime observations distinguish follower accounting/lane waits, append and sync
barriers, publication, actor/worker dispatch, Cell-control CAS purposes, and the
five phases before node-log enqueue. Disabled new CAS/submission probes avoid
timing allocation and clock reads. Cancellation remains distinct from success
and failure. Successful enqueue is not a durability receipt.

The application fixture owns bounded observation export and reports all trace
counts at shutdown, including shutdown errors. Verification rejects missing,
dropped or buffered observations. Provider events remain aggregate; marginal
phase percentiles cannot be added or causally joined to an unrelated command.

## Durability prerequisite and workload core

The follower path now syncs a recovered open prefix and its directory before
acknowledging a duplicate retry. Existing seal/retirement markers still complete
the required barriers. Startup finds prune temporaries in the actual producer
directory and discards only the uncommitted temporary after directory validation.
Partial pruning reserves simultaneous retained-copy bytes before rewrite;
uncertain cleanup remains charged. Regressions failed before these fixes.

The primary workload performs a seeded upsert and atomic application command
audit through the public SQL author API, alongside the framework outcome ledger.
It verifies duplicate replay, conflicting-audit rollback, exact payload/digest
readback, every committed command and final root coverage. These extra records
must be matched when comparing another system's application workload.

## D1 warm byte deltas and coverage-gated pruning

Warm follower accounting applies verified byte deltas rather than repeatedly
recounting the entire store. Pruning is skipped only when verified coverage is
unchanged; advancing coverage retains reconciliation. Accounting reservations
remain paired with physical files and survive ambiguous failures. Public
receipt formats, selected-member proof requirements and recovery are unchanged.

Lane-scoped recount and fully covered inode-reuse experiments remain isolated.
Synthetic append improvements had excessive repeat variability and did not
qualify application TPS. The inode-reuse candidate is not part of this branch.

## SQL mutation preparation

The generic SQL primitive executes the statement it already prepared and
authorized. It removes a second prepare through `Connection::execute` while
preserving parameter/access checks, RETURNING rejection, affected-row counts,
rollback and original errors. No durable-TPS gain is claimed from source alone.

## R1 schema and fixed statement reuse

A schema-cookie cache shares verified primitive capabilities across executor,
scheduler and capacity checks. Fixed runtime/KV/deadline SQL uses bounded
statement reuse. Schema changes invalidate capabilities; generic SQL continues
to use its authorizer. Tests exercise schema changes, indirect access, missing
primitive tables and rollback. Sustained CPU/rate improvement remains unqualified.

## R2 owner-read connection

Managed databases retain four connections: writer, protected owner reader,
capture control and capture read-lock. Each owner-read callback runs in a fresh
read-only transaction on the same serialized SQL worker and ends that snapshot
before returning. The managed VFS still authenticates sparse pages. Managed
interrupts reach executing reads; callbacks cannot end the snapshot unnoticed
or make a successful protected read writable.

Owner reads therefore preserve writer statement reuse without allowing a
second concurrent writer. Read consistency and response gates remain unchanged.

## Resident population probe

The four-connection diagnostic verified 2,000 sparse resident Cells at roughly
671 MiB process RSS and 972 MiB cgroup memory, with 20,000 named SQLite descriptors
and complete drain. Admission reserves 128 KiB native memory, a separately
charged 256 KiB page-cache target and eleven descriptors per Cell. These charges
are budgets, not loaded RSS estimates. The embedding process must provision its
OS file limit; the diagnostic verifies 65,536 soft/hard limits.

This proves sparse residency, not the full dataset or 10K-write/50K-read target.

## R0 owner-read attribution and owned export

Read observations separate actor admission, worker acquisition/dequeue,
execution and terminal reply. Absent deadline boundaries stay absent. A bounded
owner-process exporter removes per-row mount writes from dispatch and fails
qualification if its queue loses evidence. Tests preserve one terminal reply
under cancellation and failure.

### SQL slot attribution

Finite job kinds and slot acquire/start/release boundaries distinguish native
worker ownership from execution and control work. Dispatched work retains its
admission after caller cancellation until it finishes.

### SQL slot Docker result

One sparse-read diagnostic audited 2.7 million arrivals without trace loss.
Measured 5,000/s and 10,000/s points passed; 25,000/s failed. Actor waiting
dominated selected tails. This is diagnostic evidence, not a controlled gain.

### Actor phase Docker result

Ingress, Cell FIFO selection and spawned task start are observed separately.
The observations motivated native queue dispatch rather than submitter-dependent
worker handoff. Percentiles describe the selected run, not a universal limit.

### R3 native queue dispatch

Native dispatch now queues before execution admission. Native/control capacity,
shutdown ownership and cancellation remain bounded. The deterministic handoff
regression failed before the fix; control progress and terminal resource release
remain tested.

### R3 Docker dispatch result

The R3 sparse-read ramp audited 1.2 million arrivals. Measured 5,000/s passed;
10,000/s failed on 261 client-full arrivals during a server stall. Final roots
and resources drained. The 50K-read target remains unmet.

### R0 Docker owner-read result

Earlier owner-read probes verified sparse residency, renewal and complete
export. They do not qualify a populated workload, mixed reads/writes, telemetry
overhead or the simultaneous node target. Additional read-only optimization is
deferred while write parity is the priority.

## D2 follower fixture connection reuse

The signed TCP test fixture reuses one connection per selected member. Its
serialized request boundary drops connections on interrupted I/O, preserves
fresh enrollment checks and signed receipts, and owns receiver tasks through
shutdown. Five transport regressions pass on Mac and Linux. Production mTLS
adoption and ordered shipping overlap remain outstanding.

## Small-KV fleet write diagnostic

The finite profiles use 1,000 Cells, 96-byte values, a bounded keyspace, three
owners and two selected followers. Owners have 8-vCPU/16-GiB container caps and
4-GiB tmpfs each; the driver has a separate 4-vCPU/4-GiB cap. All services share
one actual 8-vCPU/16-GiB Docker VM. The external RustFS service has a two-CPU,
two-GiB cap and persistent volume. Other host work remained active.

The ramp retains 60-second windows, absolute arrivals, a 1,024-request bound,
50-ms scheduled write p99, 5-ms generator p99, complete audit, exact roots,
all-selected follower proofs and final drain. It stops at the first failing
measured point. Tmpfs qualifies process-loss behavior only; this simulation does
not establish dedicated hardware capacity or power-loss durability.

### Corrected write diagnostic result

A host-admission regression previously released a semaphore slot before its
ledger charge. The paired permit now releases the charge first and retains both
through dispatched blocking work. Two regressions failed before the fix; the
resulting diagnostic reconciled all 4,600 commands but still failed write p99 at
30/s. That result does not establish parity or an optimization-gain baseline.

## Write maintenance attribution and failed drain

An idle control after seeding distinguishes background maintenance from new
application SQL. The first attribution runs failed final log closure; one also
lost final observation counts. They remain failed runs. Their maintenance
counts motivated further investigation but cannot supply a qualified gain.

### Backend descriptor exhaustion reproduced

RustFS emitted thousands of descriptor-exhaustion errors with the inherited
file limit. Transient PUT failures preceded lease deadlines and fenced shutdowns.
The separate `small-kv-attribution-v2` profile explicitly provisions and verifies
65,536 soft/hard file limits while preserving all traffic and acceptance gates.
It records actual process limits and cgroup counters before/after load and
rejects OOM, quota mismatch and descriptor exhaustion.

A preflight parser initially rejected Linux's trailing spaces after the `files`
unit, before any application arrivals. Realistic-format and failure-retention
regressions failed before the fix. Raw process/cgroup evidence is now retained
before validation. Correcting the provider environment is not a framework gain.

The corrected `write-attribution-v7` run passed the full integrity audit:

| Measurement | Result |
| --- | --- |
| Unique Cells / audited commands | 1,000 / 4,600 |
| Measured planned writes | 1,800 at 30/s; all eventually succeeded |
| Acknowledgements inside the 60-second window | 28.85/s |
| Scheduled latency p50 / p95 / p99 | 675.129 / 4,579.914 / 6,300.832 ms |
| Generator delay p99 | 39.975 ms; fails the 5-ms gate |
| Healthy Fleet proofs / total commands | 4,583 / 4,600 |
| Trace loss, final publication debt and retained jobs | Zero; all three logs closed and sessions withdrew |
| Shutdown | All three succeeded in about 2.84 seconds |
| RustFS observed descriptors before / after | 17 / 29; these are endpoint samples, not a peak |
| RustFS memory peak | 1,690,591,232 bytes; no OOM or descriptor-exhaustion errors |

**No measured rate passed the existing latency/generator gates.** The ramp
stopped before 60/s. The result is not a hardware capacity ceiling or a matched
celld comparison.

The 60-second idle interval completed 18,872 renewal CAS operations against
18,918 aggregate PUT completions, without new application SQL. Two late seed
publications remained visible. Idle renewal p99 was 520–549 ms. In the measured
window, the fleet completed about 405 PUTs/s; renewal p99 was 4.11–4.62 seconds.
Pre-enqueue submission p99 was 29–40 ms and ticket-order p99 at most one
microsecond. Owner CPU averaged about 0.20 cores each. These observations select
maintenance/provider cost for the next controlled experiment; they do not prove
individual-request causality or an optimization gain.

Selected external artifact identities:

| Artifact | SHA256 |
| --- | --- |
| Frozen diagnostic source | `80abeff288070673191d46346422852647b2f4f60c0c50638513ff02d298677c` |
| Linux benchmark binary | `32f5511ac804ad9f3f0f940a4b01e09000c4bd0cf1949efc6f3b71832e385386` |
| Pre-benchmark verification receipt | `b7963a83cafae46ec8a8624f7d2d110116f6285b549ba6f32ef775f2460da274` |
| Revalidated diagnostic summary | `40107a31c7d58d283fe3452b0935c01ab22ff17b34366686dadd229673e3cbf9` |

These identify the tested snapshot before this concise documentation rewrite,
not the eventual PR commit. Its Rust/binary inputs are unchanged from the fully
verified preceding snapshot; the inheritance receipt records that fact.

## Verification environment

The frozen Rust changes passed 1,272 workspace tests with 40 intentionally
ignored tests, 51 local LTX tests, feature/target checks, Clippy and API docs with
warnings denied, format, boundaries, module layout, document syntax/links and
SQL/peer contract validation. Linux authority, shipper, transport and workload
checks passed with the pinned release binary. The qualification parser/retention
fix passed all 113 Python tests. Tests ran before benchmark arrivals.

An earlier suite network-tail test failed under shared-host contention, then
passed unchanged alone and in the full suite with four test threads. Original
deadlines and assertions were retained. These receipts concern this branch's
base; they do not validate integration with later `main` changes.

## Outstanding delivery

- Integrate this branch with subsequent `main` changes and rerun verification.
- Establish repeatable A/A controls and a passing scheduled-latency baseline.
- Evaluate node-bound fresh ownership reads at the existing cadence; preserve
  node lease fencing, protected-field checks, deadlines and unbound CAS renewal.
- Complete production mTLS adoption and bounded ordered shipping experiments.
- Qualify sustained publication debt, persistent storage and owner-loss recovery.
- Run a matched celld benchmark before claiming parity or the later 25% gain.
- Qualify 2,000 owned Cells with simultaneous 10K writes/s and 50K reads/s for
  30 minutes on one 8-vCPU/16-GiB owner node.
