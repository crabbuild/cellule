# Writable entity capacity: measurement status, 2026-09-29

The existing ignored `entity_process_scaling` integration workload schedules
uniform, hot, and skewed traffic at 1, 4, and 16 actions per node per second
across 3, 5, 10, and 20 constrained nodes. Its independent parser checks all
arrivals, stable mutation identities, acknowledged receipts, readback values,
owner records, and final published roots. An integrity pass means that the
reported results are internally consistent; it does not certify a supported
rate when arrivals were missed.

This revision adds separate raw command-response proof and object-publication
completion records per node. `node-N-responses.tsv` identifies Recorded,
Fleet, or Object as the single proof that released each successful runtime
command response. `node-N-publications.tsv` records queue wait, root
preparation, authority/confirmation time, and total background publication
time per commit sequence. The parser reports their distributions and published
roots/s separately from completed client actions/s. It also reports an
arrival-latency distribution over every scheduled action, including rejected
and late arrivals. The earlier resource, logical object-operation, receipt,
and readback files remain required.

The new `--workload capacity` selector fixes the fleet at three owners and
12 writable Cells. The initial CI run scheduled 10-second points at 2, 4, 16,
64, 256, and 1024 actions per node per second. The next run adds 24, 32, 48,
96, 128, and 192 to narrow the overload interval. Each shape stops at its first overloaded
point. A point is fully served only when every scheduled action succeeds and
admitted work drains within 12 seconds of its 10-second arrival window. The
verifier requires at least one fully served point and one overloaded
point for uniform, hot, and skewed traffic; it rejects missing arrivals,
misstated success, incomplete readback, or an incomplete rate ramp. It reports
the last fully served logical write rate separately from the first overloaded
rate. The first isolated object-proof result is summarized below.

The dedicated `Cell write capacity qualification` GitHub Actions workflow
builds one release binary and runs three object-proof repeats on a fresh
runner. Each repeat has its own Compose project, RustFS volume, object prefix,
and evidence directory. It uploads raw samples, logs, the binary digest, and
provider details even if a repeat fails. Its output requires review before a
capacity claim; the follower-enabled comparison remains separate.

## First isolated object-proof result

[CI run 36649534205](https://github.com/crabbuild/cellule/actions/runs/36649534205)
passed three fresh-provider repeats on the same binary, with 12 Cells and
readback verification in every run. The source snapshot was
`4837fc823c198f51a85b01528a6c7aaf6b1f4470`; the release integration
binary SHA-256 was
`2b3aa0b36df5d2c278fe2f3e524a6258c5f886ddedab927057bd4c8db76834a3`.
The RustFS image was pinned at
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
All response proofs were Object; each verified window reported zero root
sequence lag at its end.

| Shape | Highest fully served offered rate | Fully served logical writes/s | First overloaded offered rate | Fully served action p95 / p99 across repeats |
| --- | ---: | ---: | ---: | ---: |
| Uniform writes | 16 actions/node/s | 47.99–48.00 | 64 actions/node/s | 17.68–32.05 / 120.89–161.14 ms |
| One hot writable Cell | 16 actions/node/s | 47.99–48.00 | 64 actions/node/s | 40.84–52.06 / 56.45–76.39 ms |
| Skewed, 20% writes | 64 actions/node/s | 38.39–38.40 | 256 actions/node/s | 18.26–20.21 / 36.98–43.24 ms |

These are tested lower bounds, not precise saturation points. The overloaded
uniform windows had client concurrency rejection and owner capacity refusals;
hot windows had 976–982 owner capacity refusals plus 47–55 writes whose
receipt-bound read failed; skewed windows had 1,911–2,227 scheduler-late
arrivals and 469–667 read failures. The upper rate cannot be reported as
sustainable even though some writes completed. No node showed cgroup CPU
throttling. Capture p95 stayed below 2 ms in overloaded windows, whereas
root preparation and response waits were much longer. The current aggregate
provider counters cannot isolate GET/HEAD/PUT time inside preparation or
separate owner admission queueing from provider wait. A second run with raw
per-operation provider timing is needed before choosing one limiter.

The three `verification.json` files have SHA-256 digests
`35b859c9f9f66aa46da1c797fae5d691a52af907760cf3c4bacc7d79ec809fe2`,
`8a3b951a95a1b9e2b2b00ab8954df53c2783acf352cf529f684fc00be2cd6111`,
and `9d13723e28d4989a87dc60d88b7c84780c125d716ef1d3b1504eb1895fd9efc7`
for repeats 1–3 respectively. Each report contains hashes of its raw TSVs.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36649534205`.

After preparing a fresh source/binary snapshot with the Compose qualification
guide, run each repeat with a new state directory and Compose project:

```sh
python3 "$CELLULE_REFERENCE_STATE/source/crates/cellule-app/qualification/scale.py" \
  --state "$CELLULE_REFERENCE_STATE" --project capacity-unique-run \
  --workload capacity
```

The object-proof-only Compose profile records LTX phases and capture timing,
publication objects and bytes, response proof, background publication,
per-Cell root sequence lag, and per-node resource and provider operations.
The resource stream also samples unpublished follower-log bytes from runtime
stats; it is expected to be zero in the object-only lane.
Root lag is derived from the latest acknowledged receipt and completed root
publication at each window's wall-clock end; clock adjustments limit its
precision. A scheduler-late or client-full
overload point identifies the harness admission limit until runtime and
provider phase evidence demonstrates a narrower Cell limiter.

The existing entity host does not enroll a follower durability lane, so this
profile cannot supply the separate follower-enabled comparison. The added
publication observation aggregates root preparation and provider I/O; it
cannot by itself distinguish predecessor GET/HEAD, immutable PUT, and provider
wait inside that phase. Those limits prevent selecting a safe implementation
change from this revision alone.

The 2026-09-29 workstation did not provide an isolated provider environment.
The Colima VM had multiple unrelated active RustFS workloads, several above
one CPU, while the Linux qualification binary was building. Running the
3/5/10/20-node profile three times there would mix provider and CPU contention
from those workloads into the Cellule curve. A raw Docker container snapshot
is retained under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/state-20260929/evidence/shared-host-docker-stats.tsv`.
The VM also could not bind-mount the mounted Workspace volume; a disposable
source snapshot was therefore moved under `$HOME/.codex/qualification/` for
the Linux build. No capacity claim or bottleneck classification is made from
that build.

The earlier disposable snapshot built the Linux `cellule-app` integration test in
release mode with `cargo test --release --locked -p cellule-app --test integration --no-run`.
Its binary SHA-256 is
`6cf7c3dc86ff673c86bcd82b2eedcafbbe2db358f4c16e280d41600476ea9983`.
The snapshot is under `$HOME/.codex/qualification/cellule-capacity-5ca5-state`;
the original source archive and host evidence are under the Workspace path
above. That binary predates the fixed-Cell capacity selector and cannot run
it; create a new source snapshot and release binary for the capacity runs.
This is a compile result, not an execution result.

Before choosing a write-path optimization, run the workload in a dedicated
provider environment with a new object prefix per repeat. Keep the same
source/binary digest, node limits, Cell count, and arrival schedule across
three repeats; add a separate follower-enabled lane and an offered rate above
the first overload point. Require readback for every acknowledged write and
compare response proof rates with eventual root drain. If root preparation
dominates, collect its GET/HEAD, PUT, provider wait, and CAS split before
proposing a code change. If the three repeats disagree, the result remains
inconclusive.
