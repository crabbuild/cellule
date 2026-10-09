# PR 67: native admission and ordered follower pipeline

**Performance parity remains unmet; PR #67 stays a draft.** Native admission no
longer waits for a publication slot under the global ordering lock. The first
pipeline candidate regresses throughput. Preserving a short commit window and
crediting ready rounds during the next batch assembly restores throughput in a
later diagnostic. Availability, publication cost and complete draining still
fail qualification.

## Implemented behavior

- The original native byte window owns publication FIFO entries, capture control
  allocations and member frame vectors. Capacity waits and local verification
  precede ordered issuance; synchronous enqueue preserves the assigned range.
- Eight original rounds progress through independent ordered member lanes.
  Adjacent delivered work uses the canonical follower append and group commit.
  Credits stay ordered, require every member's exact coverage, and cannot jump
  to a grouped receipt's later frontier. Accepted member I/O retains credit and
  shutdown joins the original tasks even after a joining waiter is cancelled.
- The four-millisecond assembly window preserves batching. Ready follower rounds
  receive ordered credit during the next assembly, without waiting for its
  deadline. Frames, rounds and byte limits remain unchanged.
- Selection freshly reads and matches the complete uploaded bundle, then reuses
  its private encoder-checked binding metadata within that operation. Fresh
  base/root/history verification still precedes the canonical CAS; there is no
  availability cache between selections.

Celld provides the comparison for [ordered shipping](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4530),
[follower group commit](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L179)
and [new-entry bundle upload](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L6628).
Its shipping concurrency is 64; this candidate retains eight rounds and one
ordered RPC at a time per member. The publication protocols remain different.

Baseline `10370d20f52c0b2c6103b0df61c56a1252238d33` has unchanged production
`4a5b001` bytes. First candidate `a42d344ae44cadd93c320724acb3e9a149ba9935`
contains native admission, the pipeline and metadata reuse. Corrected candidate
`d5cec4885e97228efcf5c4013cd70ebc5eb79af8` adds the assembly window and prompt
round credit. The intervening `3cad11e` has no completed TPS measurement.

## Matched write diagnostics

Each case has 2,000 uniform Cells, 96-byte SQL values, the same request/result
ledger, 128 clients/queue slots, one owner and two followers, WAL NORMAL and
tmpfs. It offers 2,000 writes/s, with 30-second warmup and a 60-second measured
window. The pinned builder establishes distinct serving binaries and identical
driver, auditor, fixture bytes, images and runner. No build, contributor suite
or independent replay overlaps a timed window.

Baseline, first candidate and celld run in the initial comparison. The corrected
candidate runs later against the same profile; its replay includes those
original baseline and celld journals. They are reference runs, not newly repeated
baseline/celld runs. These observations establish no repeatable causal gain.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Cellule baseline | 468.38 | 574.13 | 52,505 | 39,266 |
| Cellule first pipeline candidate | 404.68 | 302.98 | 70,538 | 25,181 |
| Cellule corrected candidate | 553.53 | 321.43 | 60,319 | 26,404 |
| celld pinned reference | 1,993.08 | 67.96 | 0 | 403 |

TPS counts only successful completions inside the measured window. Successful
scheduled p99 includes measured successful offers through client drain and
excludes fast failures. Successful request p99 is 381.28, 174.22, 173.65 and
48.91 ms respectively. The first candidate loses 13.60% TPS. The corrected
candidate completes 18.18% more TPS and has 44.01% lower scheduled p99 than the
baseline, but 6.09% higher p99 than the first candidate. The same baseline
binary's earlier 579.10/s result illustrates run variability.

Independent replay reconciles every offer, attempt, original successful output,
payload byte count, per-Cell count, trailing completion and complete ACK
provenance. All 2,000 Cells have measured successes. The corrected candidate has
33,212 in-window successes, 65 trailing successes and a complete cohort of
61,233 ACKs including setup and warmup.

| Arm | Complete ACK cohort | Warm audit | Cold audit / joined drain |
| --- | ---: | --- | --- |
| Baseline | 47,855 | 2,804 HTTP 503s | Not reached |
| First candidate | 47,222 | 17 HTTP 503s | Not reached |
| Corrected candidate | 61,233 | 32 HTTP 503s; 61,201 retries checked | Not reached; owner exit exceeds 120 s |
| celld | 181,598 | All mutations and original retries pass | All pass; drain 16.85 s |

The corrected candidate's first recorded errors refer to Cell 243; the owner logs
`PendingPublication` during drain and the original process wait times out.
HTTP 503s fail availability qualification and do not establish lost data.
Celld's dropped offers and scheduled p99 also fail this run's unchanged
performance gates. No case qualifies.

## What telemetry establishes

Completed submission phase cohorts are 28,167, 24,303 and 33,277 respectively.
Each cohort's seven phase totals reconcile exactly with native submission time.

| Mean window observation | Baseline | First candidate | Corrected candidate |
| --- | ---: | ---: | ---: |
| Ordered-lock wait, ms | 120.613 | 0.0011 | 0.00045 |
| Publication-slot wait, ms | 1.530 | 0.00038 | 0.00029 |
| Complete native submission, ms | 122.313 | 0.214 | 0.141 |
| Follower frames per sync | 27.64 / 27.64 | 6.25 / 6.22 | 11.39 / 11.40 |
| Follower sync calls | 1,088 / 1,088 | 4,161 / 4,177 | 3,090 / 3,088 |
| Follower-proof wait, ms, separate cohort | 23.84 | 52.37 | 51.93 |

Caller waits overlap and are not serial TPS service intervals. Pipelining
removed incidental batching behind a preceding RPC. The short assembly window
increases group size relative to the first candidate, but RPC/sync work per
frame remains higher than the baseline. The two corrective changes are measured
together; this run cannot attribute their separate effects.

Corrected-candidate publication debt starts/ends at 4,439/4,402 pending entries,
48.43/45.07 seconds oldest age and 6.23/9.50 MB retained captures. Unpublished
native-log bytes grow 32.36→48.05 MB. Active Cells fall 2,000→1,999. Boundary
states and a short window do not prove stable sustained debt. The measured
window has 113,890 immutable GETs and 103,097 node-authority range starts.
Selection still verifies previous roots/history and serializes with checkpoint
work. Metadata reuse removes a repeat decode; it does not implement celld's
new-entry-only publication protocol.

## Verification and limits

The held-publication 513-capture, independently progressing member and cancelled
shutdown-join regressions fail on unchanged production and pass on the first
candidate. Its 17 native shipping and 91 bundle tests pass; all 13 contributor
routes pass with 1,988 workspace tests/doctests, 60 local LTX tests and both Rust
1.97/1.99 Clippy checks. The 38 environment-dependent tests remain ignored.

The ready-credit regression fails three times on `3cad11e` and passes three
times on `d5cec48`; all 18 native shipping tests pass. The lost-ACK fixture now
waits for its first successful follower append before issuing the command whose
ACK is lost. Previously nine of ten repetitions grouped both commands into the
successful first append and never injected the intended fault. The corrected
fixture passes ten repetitions with its original recovery assertions.

The corrected candidate's first full suite fails a two-second Fleet-ACK wait
while root CAS is held. Ten isolated repetitions pass with an actual follower
receipt and zero selected publication, supporting a load-dependent timing
hypothesis without proving its cause. The one-thread controlled rerun exposes a
separate child protocol bug: libtest prefixes `LTX-CUT` on its test-name line,
so the parent misses the marker and times out. A fresh-line marker fixes the
fixture without changing its kill, restore assertions or 15-second deadline.
The corrected marker reproduces the original timeout and passes three fixed
repetitions. Final controlled verification uses one test thread and private
workstation scratch; all 13 contributor routes pass, with 1,989 passing
workspace test/doctest executions, 38 ignored environment tests, 60 local LTX
tests and both Clippy versions. The serving code is unchanged from measured
`d5cec48`; the later change is test-only. All failed runs remain part of the
evidence; no qualification expectation or original deadline changes.

The shared VM has eight CPUs and 8,306,286,592 bytes of total memory across all
roles, with oversubscribed container ceilings. It does not qualify a dedicated
8-vCPU/16-GiB serving node. Cellule's original 64-MiB retained, 1-GiB disk and
20-MiB publication-working policies are unchanged; celld has no equivalent
fixture admission settings. No new Bucket, read-only, mixed or physical-media
results are supplied here. The three five-minute repetitions remain required.

Full source/build identities, journals, audits, telemetry, valid and failed
attempts are outside Git under `/Volumes/Workspace/crabbuild-target/`:

- `native-pipeline-20261009`: frozen initial comparison and contributor evidence;
  10,814 files, 1,041,121,919 bytes verified against its SHA-256 index.
- `native-batch-window-20261009`: frozen intervening failed suite and lost-ACK
  injection diagnosis; 3,925 files, 54,464,448 bytes verified.
- `native-round-credit-20261009`: corrective regressions, pinned diagnostic,
  independent journal replay, telemetry and contributor attempts.

Final source/build hashes and qualification status are in each evidence index.
No raw journals or bulk artifacts are added to the repository.
