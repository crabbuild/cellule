# Publication-progress measurements

October 11, 2026. These fresh public-application diagnostics use Node Bundle v1,
two required followers, and the existing 64-MiB retained-work budget. They establish
a smaller 500-write/s acceptance milestone; the separate 2K-write/20K-read
qualification remains open.

## Workload and resources

| Setting | Actual configuration |
| --- | --- |
| Workload | 2,000 uniformly active Cells; distinct 96-byte SQL ledger INSERT plus SELECT; durable outcome retained for two hours |
| Requests | 30-second warmup; 128 in-flight requests and 128 queued offers; no timed-request retry |
| Serving owner | 8 vCPUs, 16 GiB; pinned CPUs 0–7 |
| Each follower | 1 vCPU, 2 GiB |
| Object provider | RustFS, 2 vCPUs, 2 GiB; fresh provider volume |
| Client/auditor | 4 vCPUs, 2 GiB |
| Linux VM | 12 vCPUs, 24 GiB; support roles share CPUs 8–11 |
| Host | Mac14,13; 12 physical/logical CPUs; 32 GiB RAM |
| Local database/follower storage | tmpfs; provider data on the VM disk |
| Evidence | Every ACK is read back and retried with the original request before and after joined drain/cold restore |

The v6 and v7 write-only VM disks were under `/Users/haipingfu/.colima`, on the internal host volume.
The installed Colima CLI ignored the attempted `COLIMA_HOME` override; no
external-to-internal disk relocation occurred. The earlier failing profile and
its disk images remain preserved. Host process samples retain competing compiler
load. This shared development host is not a dedicated capacity-qualification
machine; these runs do not establish causal speedup percentages, physical disk
survival, maximum TPS, or celld parity. The runtime reads in these diagnostics
use the current live-owner lease mode; they do not implement a new per-read
follower barrier protocol.

## Five-minute 500-write/s milestone

Source `4ee7cf523e8b28f62c97601074db2752e42cf763`; all eight framework packages
were cleaned before building with pinned Rust 1.98.1. The SQL binary SHA-256 is
`5299a7dc2ce33b1cca3992f7f2e0001e5f2dd5ab14dec6f9f3617b4c88460dee`.

| Measurement | Result |
| --- | ---: |
| Planned/generated writes | 150,000 / 150,000 |
| Completed within 300 seconds | 149,997 (499.99/s) |
| Completed including final in-flight requests | 150,000 |
| Errors / queue drops / unissued offers | 0 / 0 / 0 |
| Warmup errors / drops | 0 / 0 |
| Scheduled latency p50 / p95 / p99 | 6.6 / 10.3 / 16.1 ms |
| Request latency p50 / p95 / p99 | 5.8 / 9.1 / 13.9 ms |
| ACKs checked warm and cold, including setup/warmup/probe | 167,001 |
| Warm/cold readback or exact-retry failures | 0 / 0 |
| Joined owner/follower drain | 47.28 s |
| Cold startup | 11.84 s |

Three offers complete just after the measurement boundary; the delivery test
counts them separately. Minute snapshots report zero admission refusals.

| Window sample | Retained reservation bytes | Oldest root age | Issued minus bucket-covered frames | Pending root publications |
| --- | ---: | ---: | ---: | ---: |
| Start | 32,664,702 | 45.019 s | 8 | 1,902 |
| Minute 1 | 28,860,412 | 41.999 s | 6 | 1,905 |
| Minute 2 | 29,162,452 | 44.588 s | 8 | 1,980 |
| Minute 3 | 29,281,360 | 43.721 s | 25 | 1,860 |
| Minute 4 | 29,607,202 | 42.818 s | 9 | 1,851 |
| End | 39,664,838 | 46.113 s | 14 | 1,969 |

Root age and population remain bounded around the existing 45-second policy
trigger. The end reservation includes 11,305,976 bytes of active materialization;
selection retains 5,242,400 bytes, and the publisher reserves 23,068,672 bytes.
These are ledger reservations, not a measurement of total process heap.

| Completed phase | Count | Mean duration |
| --- | ---: | ---: |
| Preparation | 18,683 | 5.04 ms |
| Upload | 18,683 | 3.97 ms |
| Selection | 18,682 | 4.42 ms |
| Fresh verification inside selection | 18,682 | 1.51 ms |
| Authority CAS inside selection | 18,682 | 2.55 ms |
| Root materialization | 12,397 | 17.70 ms |
| Checkpoint notification wait | 12,395 | 32.64 ms |

Every completed phase in this window reports zero failure/cancellation.
Nested and overlapping durations cannot be summed into request latency. Serial
fallback counts are 12,525 for foreground headroom, 6,127 for checkpoints and
31 for capacity; fallback preserves progress rather than rejecting the request.

Raw artifacts are outside Git under
`cellule-publication-progress-20261011-memory-v6`, case
`cellule-fleet-parity-goal-memory-v6-internal-r1`. Build/source manifests,
request journals, minute snapshots, independent audits, process exits and
`internal-publication-analysis.json` are retained together.

## Repeat after joining activation lifetimes

Source `03d2eba21c64105841c5ff0d2cb671c4148edf53`, with SQL binary SHA-256
`902ed4cd327ff514f79f5e6a47c2798a75a6894a5245015625823f79fb2fac70`.
The same five-minute workload and limits pass again after the authority activation
callback moves outside proof-waiter lifetimes. All eight framework packages were
cleaned for this fresh binary; its source manifest has no Rust/Cargo differences
from the committed implementation.

| Measurement | Result |
| --- | ---: |
| Planned/generated/completed writes | 150,000 / 150,000 / 150,000 |
| Completed within the window | 149,990 (499.97/s) |
| Errors / queue drops / unissued offers | 0 / 0 / 0 |
| Warmup errors / drops | 0 / 0 |
| Scheduled latency p50 / p95 / p99 | 6.3 / 9.7 / 16.0 ms |
| Request latency p50 / p95 / p99 | 5.5 / 8.6 / 13.5 ms |
| All-ACK warm/cold readback and original retry checks | 167,001 each; zero failures |
| Joined fleet drain | 39.25 s |
| Cold startup | 9.73 s |

Ten writes finish just after the window. Minute snapshots show zero admission
refusals, root age 42.0–48.9 seconds and 1,858–1,954 pending roots; the end age is
46.015 seconds. Retained reservations start at 28,399,209 bytes and end at
39,737,580 bytes, including root materialization in progress. Bucket frame lag
ranges from 6 to 54 in these samples, independently of root age.

Completed selection averages 3.94 ms across 21,272 rounds; preparation 4.20 ms,
upload 3.44 ms, materialization 15.52 ms and checkpoint wait 29.16 ms. These are
nested/overlapping phase observations, not an additive response-time model or
an attributable speedup comparison. Host disk headroom decreased under swap
during the run. The next workload was held until the completed task-owned VM
could be stopped and its disks copied and verified on Workspace.

Artifacts are under `cellule-publication-progress-20261011-activation-v7`, case
`cellule-fleet-parity-goal-activation-v7-corrected-r1`;
`acceptance-publication-analysis.json` pins the raw summary and snapshot hashes.
The first v7 attempt stopped before timed work because the copied harness
omitted its unchanged seed helper. Its logs and provider volume remain retained;
the corrected run uses the v6 helper byte-for-byte.

## Fresh mixed and read-only windows

The same v7 binary runs after the completed task-owned VM is stopped and both
images are copied byte-for-byte onto Workspace. `vm-relocation.json` records
matching pre-restart source/destination SHA-256 hashes, actual paths and the
unchanged 12-vCPU/24-GiB VM. Original provider volumes remain retained in that
image; the new case uses a fresh provider volume. The primary Colima profile is
untouched. This physical move occurred after the write-only repeat; it must not
be confused with the earlier ignored `COLIMA_HOME` override.

Each window has a 30-second warmup and 60-second measurement with the same
2,000 Cells, SQL ledger, 64-MiB budget and 128/128 client limits.

| Window | Offered/s | Successful/s in window | Returned errors | Dropped offers | Request p50 / p95 / p99 | Scheduled p50 / p95 / p99 |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| Mixed writes | 2,000 | 971.25 | 53,037 | 8,662 | 6.2 / 18.5 / 36.6 ms | 7.7 / 20.9 / 39.3 ms |
| Mixed reads | 20,000 | 18,529.95 | 0 | 88,152 | 0.6 / 3.6 / 9.5 ms | 2.0 / 6.5 / 12.7 ms |
| Read-only reads | 20,000 | 15,806.40 | 0 | 251,484 | 0.5 / 4.2 / 17.0 ms | 1.9 / 10.8 / 38.5 ms |

Every planned offer is generated: 120,000 mixed writes and 1,200,000 reads per
window. Mixed completions including in-flight drain are 58,301 writes and
1,111,848 reads; read-only completions are 948,516. Warmup mixed writes have
23,837 errors and 5,903 drops, mixed reads 61,229 drops, and read-only reads
54,460 drops. Warmup read errors are zero. These windows fail delivery at the
offered targets; successful-request latency alone does not establish capacity.

All 90,562 ACKs, including setup/warmup/probes, pass both warm and cold readback
and original-request retry checks with zero errors. Joined fleet drain takes
38.77 seconds and cold startup 7.48 seconds. Correct ACK recovery and sustained
offered throughput remain separate observations.

### Where the mixed window stalls

Window telemetry classifies 53,035 new write failures as publication-backlog
refusals, with no new failures in the other finite classes. Client counts include
the final in-flight completions beyond the snapshot boundary. Retained
reservations stay near the 48-MiB foreground threshold:

| Stage | Start | End |
| --- | ---: | ---: |
| Capture | 20,676,012 B | 13,796,358 B |
| Selection | 6,131,764 B | 8,023,906 B |
| Materialization | 455,328 B | 5,319,898 B |
| Publisher | 23,068,672 B | 23,068,672 B |
| Total, including small request/other reservations | 50,331,776 B | 50,244,114 B |

Oldest root age grows 57.700→88.718 seconds. Issued-minus-bucket-covered frame
lag is 6,154→4,046, while follower proof ends only 21 frames behind issuance at
the end. The publisher completes 1,022 cohorts, including 60,234 captures and
63,892 frames: about 58.9 captures and 62.5 frames per cohort, near the existing
64-frame bound. Of these rounds, 1,019 use serial fallback for checkpoints.
There are two capacity fallbacks and no headroom fallbacks in the measured delta.

| Completed phase | Mean duration |
| --- | ---: |
| Preparation | 24.52 ms |
| Upload | 22.06 ms |
| Selection, including fresh verification and CAS | 11.13 ms |
| Fresh verification | 4.91 ms |
| Authority CAS | 5.56 ms |
| Root materialization | 31.99 ms |
| Checkpoint notification wait | 100.02 ms |

All completed phases have zero failure/cancellation. This points to shared
publication and checkpoint service under retained-byte pressure rather than
follower lag. It does not justify raising the budget or removing the checkpoint
ordering guard. The following read-only start/end snapshots show no pending
publication, no capture credit and zero bucket lag.

The read-only window contains a sampled competing compiler load of 335.1% CPU;
the two mixed-window samples report zero. Read-only request maximum is 2.52
seconds. The slower read-only result cannot establish a repeatable regression,
maximum throughput or a causal explanation; isolate generator/host stalls from
server execution in a controlled repeat.

Artifacts: case `cellule-fleet-parity-read-mixed-activation-v7-workspace-r1`
under the v7 directory, with `mixed-publication-analysis.json`,
`read-only-publication-analysis.json` and `host-load-window-analysis.json`.

## Next capacity work

1. Qualify the checkpoint-overlap candidate below on an idle host. The earlier
   mixed path fell back to serial publication in 1,019 of 1,022 rounds; the
   candidate removes that fallback while preserving exact dependency verification,
   ordered root credit and the authority CAS. Its first fresh 750-write/s run
   failed delivery and debt gates under competing compiler load.
2. Reduce preparation/verification work and bytes per cohort. Use the new phase
   and stage measurements to check actual allocation bounds and repeated catalog
   work. Ramp fresh five-minute offers through 750, 1,000, 1,500 and 2,000 writes/s,
   requiring zero refusals/drops, stable debt and every-ACK warm/cold checks under
   the same 64-MiB budget. Smaller component timings are insufficient.
3. Repeat read and mixed windows on a controlled host, separating client schedule
   stalls, synchronous SQL execution and support-role contention. Qualify hot-Cell
   fairness and long-query isolation rather than extrapolating uniform warm reads.
4. Extend physical-media, crash/partition and maintenance qualification. The
   activation-cancellation regression and process audits cover concrete runtime
   lifetimes; tmpfs tests and joined cold restore cannot certify device loss.

The hosted workspace run also retained two `Fenced` follower-maintenance example
failures. Both exact cases pass all-feature local execution and five repeated
runs. The full isolated public controller suite passes all 382 cases, including
both original failures. The original CI logs remain and the job is retried
without weaker assertions or longer deadlines; a passing repeat does not identify
or fix the intermittent failure's cause.

### Checkpoint-overlap candidate

The next candidate composes checkpoints in either of its two ordered cohorts,
releases duplicate preparation snapshots after encoding, and shares heavy
preparation/verification scratch. It preserves the existing 64-MiB budget,
foreground headroom, fresh dependency checks and authority CAS. An accepted
checkpoint joins its original callback to preserve upload source errors even
when a sibling upload delays worker termination.

The focused bundle suite passes 150 cases; the subsequent six-case checkpoint
suite also covers a checkpoint in each cohort and independently ordered credit.
Exact encoding comparisons retain complete sibling catalogs and require
byte/digest equality. These are correctness and bounded-lifetime regressions;
the candidate's fresh application capacity ramp has not yet completed. The
500-write/s and mixed/read-only tables above still describe source `03d2eba`,
not this candidate. See the [managed protocol](managed-bundle-pipeline.md).

The maintenance investigation also reproduces a distinct capture race: a real
same-boot heartbeat between two directory observations caused `Fenced` through
full-advertisement equality. Capture now retains the exact Open epoch, membership
and signed boot identity while reobserving monotone generation, activation,
coverage and lease progress. Its original heartbeat regression fails before the
change and passes after it; a held-read closure case still rejects. The original
CI failures and 50 passing local repeats remain retained. Hosted workspace run
38181130233 subsequently passed, including the public controller models. This
concrete fix and passing run do not establish that the heartbeat race caused
the original hosted failures.

An isolated controller repeat passed 383 of 384 cases. The remaining failure
was fixture construction: separate clock reads could sign a 30,001-ms
advertisement against the 30,000-ms maximum. Deriving both timestamps from one
reading preserves the lifetime contract; all three inherited-recovery cases
then pass with their original assertions.

The device-backed sparse-owner process test also passes against fresh ext4
SQLite and RustFS volumes. After the child acknowledges one mutation, the test
kills its process, deletes its local writer files, restores the authoritative
root, and retries the original request with a handler that fails if invoked.
The retry returns the original result and sequence; the next mutation verifies
the recovered state. This qualifies that published-root process-loss path,
not unpublished follower tails, network partitions or physical power loss.
The initial missing-import build failure and corrected passing run are both
retained under the v9 artifact directory.

Two further ignored integration cases passed against fresh ext4 follower,
SQLite and RustFS volumes. The exact two-follower ensemble is authoritative
before the child can acknowledge. The first case holds the original data PUT,
verifies a follower-based success while the authoritative root remains at
sequence zero, kills the child and removes its local writer files. Recovery
reopens both native follower logs, verifies their exact retained frame, restores
the acknowledged state and returns the original retry without running its
mutation again.

The second case forwards the child's real RustFS traffic through a test-owned
TCP proxy, publishes one mutation, then closes existing connections and rejects
new connections. The next mutation receives no success before its client
deadline. Its captured state is nevertheless present on both follower disks;
recovery retains that uncertain mutation as well as the earlier ACK. Both
original request IDs return their stored outcomes without reexecution. The
proxy recorded 49 rejected or closed connections in the passing run.

The native followers share the killed process and use separate device-backed
directories. These cases qualify process loss, native follower persistence and
an owner-to-object-store TCP partition, not remote peer transport, follower
network partitions or power loss. Initial setup and incorrect uncertain-tail
expectation failures remain retained alongside the passing `device-crash-r5`
logs. Strict runtime test Clippy and the isolated shared-store integration suite
pass: 246 cases, with six environment-dependent cases ignored.

### Fresh 750-write/s checkpoint-overlap run

Source `da8887d`, five-minute offered window, 2,000 Cells, unchanged 64-MiB
budget and the same client/auditor binaries:

| Measurement | Result |
| --- | --- |
| Offered writes | 225,000 |
| Successful writes within the window | 210,347; 701.16/s |
| Returned errors / dropped offers | 11,915 / 2,731 |
| Scheduled p50 / p95 / p99 | 9.7 / 50.1 / 318.4 ms |
| Warmup errors / drops | 0 / 42 |
| Every-ACK warm and cold readback and original retries | All 234,813; zero errors |
| Joined fleet drain / cold startup | 108.56 / 29.75 seconds |
| Final three-minute debt / age slopes | +866,204 bytes/s / +393 ms/s; failed |
| Pipeline checkpoint / headroom fallbacks | 0 / 3,579 |

The case completed its audits but failed the capacity gate. Host samples inside
the measured window show competing compiler activity in 19 of 20 observations,
averaging 349% sampled CPU and peaking at 696%. It does not establish idle-host
capacity or isolate a causal throughput effect of the candidate. Retain this
failed attempt and repeat on an idle host before advancing the ramp.

Within this window, materialization averaged 44.17 ms and checkpoint waiting
93.83 ms. Preparation, upload and selection averaged 20.62, 20.31 and 12.34 ms.
The publisher retains its original 22-MiB scratch/cache reservation; sampled
root I/O concurrency falls from two to one as retained selection metadata and
captures grow. These observations identify admission and root work to investigate;
they do not justify changing the budget or weakening the debt gate.

Artifacts: `cellule-checkpoint-overlap-20261011-v9`, case
`cellule-fleet-parity-checkpoint-overlap-v9-750-r1`, with
`capacity-750-derived.json`, `capacity-750-host-window.json` and the case's
`publication-analysis.json`. The 1,000/1,500/2,000 ramp and fresh read/mixed
qualification remain outstanding.

### Removing a duplicate LTX scan

A diagnostic captured an actual SQLite mutation and encoded 64 distinct Cell
rows from the same pinned 203-byte LTX segment. The in-memory CRB1 constructor
verified each segment before copying it, then verified it again through the
external decoder. Temporary counters in an isolated snapshot measured 128 full
segment validations per encoding. The candidate performs 64, retaining complete
footer/layout checks and exact comparisons with the original verified rows.
Its independent decoder still performs all 64 validations. The complete
40,144-byte envelope and digest match exactly before and after.

Eight debug-build samples of 100 encodings had median durations of 1,041.49 ms
before and 636.36 ms after. These shared-host component timings do not establish
application throughput or a causal fleet speedup. Instrumentation was removed
before verification: the full LTX suite passed 323 tests/doctests with seven
ignored, the runtime suite passed 1,316 with thirteen ignored, local-only LTX
passed 66, and strict workspace Clippy passed. Existing origin verification,
formats and resource bounds remain intact. Artifacts are in the v9
`bundle-encode-diagnostic` directory, including the pinned input, raw samples,
counter comparison and contract results.

### Requested three-node resource profile

The `three-node-1cpu` profile uses one owner and two required followers, each
limited to 1 vCPU and 2 GiB memory with swap disabled. Their guest CPU sets are
0, 1 and 2; RustFS occupies 3–4 and the client 5–8. A live Docker probe confirmed
the configured limits and effective cgroup quota, memory and zero-swap limits
for all five containers. This establishes resource enforcement, not application
capacity. The original queued 8-CPU-owner repeat was stopped before startup in
response to this resource request; its deferral and observations remain retained.

The preliminary 2,000-Cell diagnostic used 30 seconds warmup and 60-second
measurement windows. At 100 offered writes/s plus 1,000 reads/s, it delivered
99.20 writes/s and 992.18 reads/s, with zero returned errors but 47 write drops
and 468 read drops; one read also dropped during warmup. Read-only offered
1,000/s delivered 999.98/s with zero errors or drops. All 10,954 acknowledged
mutations passed warm readback and exact retry, but the owner exceeded the
original 120-second drain deadline and the cold audit did not run. The case
failed; these short, busy-host measurements do not qualify capacity.
Artifacts are in `cellule-three-small-nodes-20261011-v10`, including
`setup-verification-result.json`, raw windows and failed lifecycle evidence.

The user clarified that the required topology is **three separate VMs**. The
`three-vm-1cpu` profile routes each node to its own Docker host and retains
fresh node volumes on those VMs' disks. Hypervisor configurations allocate
one vCPU and 2 GiB each; distinct guest boot IDs and Docker host identities,
usable memory, disabled guest/container swap and OOM counters are recorded.
RustFS and the client run in a separate 6-vCPU, 8-GiB support VM. Loopback TCP
routes use SSH tunnels while retaining end-to-end pinned peer mTLS; their
transport overhead and shared Mac host belong to the measurement scope.
The 2,000-Cell, 64-MiB, five-minute capacity run still requires an idle host,
every-ACK warm/cold audits, stable debt and the original drain deadline.
New topology artifacts are in `cellule-three-vms-20261011-v11`.

A fresh 16-Cell smoke run completed on the separate VMs. Each window used
30 seconds warmup and 60 seconds measurement, with the unchanged 96-byte value
and 64-MiB budget:

| Window | Delivered writes/s | Delivered reads/s | Returned errors / drops | Scheduled p99, write / read |
| --- | ---: | ---: | --- | --- |
| 10 offered writes/s + 100 reads/s | 10.00 | 100.00 | 0 / 0 | 2,058.6 / 1,885.5 ms |
| Read-only, 100 offered reads/s | 0 | 100.00 | 0 / 0 | — / 44.7 ms |

All 917 ACKs passed warm and cold readback and exact retry. Fleet drain took
8.28 seconds; fresh bucket-only startup took 1.10 seconds. No guest or container
OOM occurred. Mixed response-source deltas were 134 follower proofs and 466
bundle proofs; each follower recorded four failed append attempts and its log
reported `Fenced`. These observations do not identify the earlier CI failure.
Host samples showed competing work, with mixed-window CPU idle values of
38.81%, 38.34%, 0% and 61.90%. The latency target failed, and a 16-Cell smoke
cannot qualify 2,000-Cell capacity. Clean shutdown and cold bucket restore also
do not substitute for remote-VM crash recovery of an unpublished follower tail.
Exact resource identities, metrics, hashes and raw evidence are retained in
`three-vm-smoke-results.json` and `cellule-fleet-parity-three-vms-smoke-r1`.
The fresh 2,000-Cell, five-minute 750-write/s case waits for the original idle-host
start check; higher ramp points remain pending.

A separate 120-second diagnostic on the same three VMs traced all 41 rejected
append attempts to grant expiry **after native fsync**. The lane remained open
and the token digest matched; the original post-write check refused the proof.
Failed attempts spent 22.2–872.0 ms in fsync. Error-only instrumentation retained
the original errors and authority checks. The window delivered 9.88 writes/s
and 98.81 reads/s, with zero returned errors but 14 write and 143 read drops;
scheduled write/read p99 was 3,363.3/3,011.9 ms. All 1,503 ACKs passed warm/cold
readback and exact retry; drain took 18.10 seconds. This busy-host diagnostic
does not qualify capacity. Evidence is in `cellule-grant-fenced-20261011-v12`.

A one-second early-renewal prototype then failed for a separate reason: a signed
grant request exceeded the follower's unchanged ten-second horizon by 1 ms.
The owner entered rotation and its Cells stopped serving. All 1,200 measured
write and 12,000 read attempts failed; the warm audit could not read the 140
earlier ACKs, and the cold audit did not run. Owner drain exceeded 120 seconds
and cleanup stopped it; followers stopped normally, with no container OOM.
That failed early-renewal prototype was not promoted.
`cellule-grant-renewal-20261011-v13` retains the failed attempt and exact deadline
samples. These append/grant observations do not establish the cause of the
earlier maintenance CI failures, whose fixture uses local follower transport.

The client now leaves one second below the receiver's existing ten-second
horizon; earlier grant deadlines still take precedence. Five signed-transport
tests passed, including the reproduced 1-ms lag, the headroom boundary, expired
grant deadlines and overflow. A fresh 120-second run with this deadline change
and the original grant reuse completed: 9.46 writes/s and 94.58 reads/s, zero
returned errors, 65 write drops and 650 read drops. Scheduled write/read p99 was
6,866.7/6,842.5 ms. All 1,452 ACKs passed warm/cold readback and exact retry;
drain took 10.27 seconds. There were 39 post-fsync grant-expiry rejections, with
failed fsyncs spanning 15.3–1,469.7 ms. The deadline fix preserves serving across
the observed clock difference; this busy-host diagnostic still fails delivery
and latency qualification. Evidence is in `cellule-request-deadline-20261011-v14`.

The committed revision `d821b6e` was then rebuilt without diagnostic patches and
verified on the three separate VMs. Both windows used 16 Cells, 96-byte values,
the unchanged 64-MiB budget, 30 seconds warmup and 120 seconds measurement:

| Window / request | Offered/s | Delivered/s | Scheduled p50 / p95 / p99 | Errors / drops |
| --- | ---: | ---: | --- | --- |
| Mixed writes | 10 | 9.04 | 34.3 / 2,650.2 / 10,284.6 ms | 0 / 115 |
| Mixed reads | 100 | 90.51 | 4.9 / 2,313.0 / 10,217.1 ms | 0 / 1,139 |
| Read-only | 100 | 100.00 | 4.7 / 13.1 / 53.7 ms | 0 / 0 |

The mixed scheduled histogram overflowed for 13 write and 127 read samples,
so its table percentiles use nearest-rank quantiles from all attempted measured
request journals. Dropped offers have no latency sample; the original driver's
overflow and failed qualification are retained. All 1,402 ACKs passed warm/cold
readback and exact retry. Drain took 15.78 seconds; cold startup took 2.18 seconds.
All node processes stopped normally; guest swap and OOM counters stayed zero.
Mixed response-source deltas were 86 follower proofs and 999 bundle proofs;
the followers recorded 13 and 12 failed appends. Their failures grant no Fleet
credit. Start, minute and end samples in both windows showed active Fleet in
epoch 1 without fencing or rotation.

Host CPU idle ranged from 3.16–52.17% in mixed samples and 0.17–66.64% in
read-only samples. This verifies functional behavior at the stated workload;
it does not qualify capacity. `cellule-three-vms-current-20261011-v15` retains
the source/binary identity, raw windows, `three-vm-current-results.json` and
`three-vm-current-results.md`. The waiting high-rate run now uses this committed
build, the original 2,000-Cell/five-minute/64-MiB profile, unchanged timed driver
and previously verified setup-only pacing. Earlier waiting queues were joined
before any high-rate case was admitted.

After the deadline fix, a fresh uninstrumented early-renewal probe used the
same three VMs, 16 Cells and 120-second mixed window. The client renews cached
grants when one second or less remains. Signed five-second expiry, native
pre/post-fsync authority checks and the bounded exact-frame retry stay unchanged.
Both followers recorded zero failed appends and neither peer log contained
`Fenced`, compared with 13 and 12 measured failures in the preceding separate
run. The renewal change is now applied to the SQL example transport; it leaves
time for transport and fsync but cannot guarantee an append finishes before
expiry.

The probe delivered 9.51 writes/s and 95.18 reads/s, with zero returned errors
and 58 write / 578 read drops. Scheduled write/read p99 was 4,897.4/4,747.7 ms.
One write completed after the window and is excluded from delivered throughput.
All 1,459 ACKs passed warm/cold readback and exact retry; joined drain took
11.01 seconds and cold startup took 2.24 seconds. Node processes stopped
normally, guest OOM/swap counters remained zero, and boot IDs stayed stable.
Every mixed-window host sample reported 0% CPU idle. These separate busy-host
runs do not establish a throughput gain; dropped offers and seconds-long tails
still fail capacity and latency qualification. The explicit patch, source and
binary hashes, raw windows and `three-vm-headroom-results.json` are retained in
`cellule-grant-headroom-20261011-v16`. A fresh committed build and the unchanged
idle-host ramp remain required.

The four ext4-backed process/partition cases were also repeated with fresh
volumes after a prior cleanup observation was interrupted. All four passed in
51.36 seconds; the repeat recorded both provider and worker stopped without
OOM. This is device-backed process evidence, separate from remote-node VM
failure or power-loss qualification. Both attempts remain retained.

## Retained failed attempts

| Attempt | Outcome |
| --- | --- |
| Working admission v2, 500/s for five minutes | Zero returned errors but 144 drops; root age grew; not accepted |
| Ordered frame reads v3, two five-minute repeats | Errors/drops remained under competing compiler load; both completed all-ACK warm/cold audits and drained within 120 seconds |
| Working admission v5, 500/s for five minutes | 482.84/s; 3,746 errors and 1,398 drops; growing root age; all 161,857 ACKs passed warm/cold checks and drain took 51.92 seconds |
| Earlier profile/provider filesystem failure | Guest-wide I/O errors before timed work; no throughput result; stopped disks and failure artifacts preserved |

No budget, qualification profile, evidence requirement, fault assertion or
deadline was increased to make these failures pass. Correct readback of ACKs
and sustainable offered throughput are separate requirements.
