# Bounded follower log: native R14 diagnostic

Rotating the live follower chunk at 1 MiB instead of 64 MiB reduces repeated
scanning and prefix rewriting when retained history is large. The record format,
durability gate and recovery checks remain unchanged. Historical larger chunks
and individual larger valid frames remain accepted.

The same frozen frame corpus was appended through the public follower API in
64-frame batches. Each case has three repetitions per version; the table gives
medians. Seal and cold checks run after the measured append interval. All 30
points recovered every retained frame byte-for-byte, including the complete
uncovered suffix.

| Frames / coverage lag | Control time | Candidate time | Process read bytes, control → candidate | Process write bytes, control → candidate |
| --- | --- | --- | --- | --- |
| 1,024 / no coverage | 302.60 ms | 377.85 ms | 4.77 MB → 4.77 MB | 0.67 MB → 0.67 MB |
| 1,024 / 256 | 397.67 ms | 541.64 ms | 6.57 MB → 6.57 MB | 2.16 MB → 2.16 MB |
| 4,096 / no coverage | 4,150.84 ms | 1,943.33 ms | 101.10 MB → 29.81 MB | 3.55 MB → 3.55 MB |
| 4,096 / 256 | 2,334.83 ms | 2,482.21 ms | 43.15 MB → 43.15 MB | 13.60 MB → 13.60 MB |
| 4,096 / 2,048 | 8,741.20 ms | 1,796.41 ms | 246.29 MB → 32.67 MB | 60.20 MB → 3.55 MB |

The last case's median process CPU fell from 8,300 to 1,660 ms; median append
p50/p95 fell from 54.67/259.00 to 24.21/49.64 ms. Retained storage increased
from 2,041,419 to 2,495,995 bytes because an immutable chunk retains its covered
prefix until its final sequence is covered. All retained bytes were checked.

Short-history cases have essentially identical I/O counts and slower timings.
These sequential debug-build diagnostics ran on shared Colima, where other
workloads can affect timing. The result establishes reduced history
amplification, not a universal speedup. Process read/write counters include
cached and other process I/O; they are not physical disk throughput.

Smaller chunks also exposed a multi-rotation batch bug: later rotations moved
earlier pending records' indexed locations to the newest chunk. A regression
failed through warm tail reading before the fix. The fix relocates only records
from the current open chunk. Tests cover warm/cold tails, partial and complete
coverage, interrupted batches and historical larger chunks.

The isolated candidate passed 650 runtime unit tests and 408 integration tests,
with the original ten environment-dependent ignores. Strict all-target Clippy,
API docs, formatting, module/boundary checks and document/SQL protocol gates
passed. All 1,234 Rust/Cargo inputs matched the tested snapshot; the temporary
native diagnostic was removed before these checks.

The subsequent x86 workspace CI run failed two legacy fault-injection tests:
each selected an arbitrary `.log` directory entry, which can belong to frame 1
after rotation while the assertion requests frame 2. The correction explicitly
damages the live chunk containing the last appended frame, retaining the
original rejection and scan-count assertions. Production code is unchanged;
corrected tests passed the `ca2f8f8` workspace CI Test step; the full workflow
remains in progress. The HTTP input stays frozen at
`464dad2`. The adjacent JSON preserves the CI failure and log hash.

The [adjacent JSON](2026-10-05-follower-bounded-r14.json) records exact medians,
source/corpus hashes and external raw evidence. The initially captured control
manifest read mutable main-source hashes; corrected hashes were taken from the
unchanged isolated control snapshot, and the original capture is preserved.
The control diagnostic executable was not frozen before candidate overwrite,
so no hash for that executable is claimed.

An HTTP comparison and dedicated-node qualification remain pending. This does
not establish the 2,000-Cell 10K write / 50K owner-ordered read TPS target, the
original three 30-minute windows, or owner-loss/follower-only recovery.
