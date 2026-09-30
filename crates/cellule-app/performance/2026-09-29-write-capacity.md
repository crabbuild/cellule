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

A subsequent instrumentation revision adds `node-N-executions.tsv` with actor
queue wait and SQL worker round trip per attempted command. This observation
ends before durability submission and proof. The verifier reports both
distributions per window, which helps distinguish owner admission and worker
occupancy from storage publication without treating either as a durable
response. The first CI result below predates this file.

The new `--workload capacity` selector fixes the fleet at three owners and
12 writable Cells. The initial CI run scheduled 10-second points at 2, 4, 16,
64, 256, and 1024 actions per node per second. The second run added 24, 32,
48, 96, 128, and 192 to narrow the overload interval. Each shape stops at its first overloaded
point. A point is fully served only when every scheduled action succeeds and
admitted work drains within 12 seconds of its 10-second arrival window. The
verifier requires at least one fully served point and one overloaded
point for uniform, hot, and skewed traffic; it rejects missing arrivals,
misstated success, incomplete readback, or an incomplete rate ramp. It reports
the last fully served logical write rate separately from the first overloaded
rate. The first and second isolated object-proof results are summarized below.

The dedicated `Cell write capacity qualification` GitHub Actions workflow
builds release binaries for separate object-proof and follower-proof jobs.
Each job runs three repeats; each repeat has its own Compose project, RustFS
volume, object prefix, and evidence directory. The workflow uploads raw
samples, logs, binary digests, and provider details even if a repeat fails.
Its output requires review before a capacity claim.

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
separate owner admission queueing from provider wait. The second run below
adds raw per-operation provider timing.

The three `verification.json` files have SHA-256 digests
`35b859c9f9f66aa46da1c797fae5d691a52af907760cf3c4bacc7d79ec809fe2`,
`8a3b951a95a1b9e2b2b00ab8954df53c2783acf352cf529f684fc00be2cd6111`,
and `9d13723e28d4989a87dc60d88b7c84780c125d716ef1d3b1504eb1895fd9efc7`
for repeats 1–3 respectively. Each report contains hashes of its raw TSVs.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36649534205`.

## Finer object-proof result

[CI run 36651191247](https://github.com/crabbuild/cellule/actions/runs/36651191247)
passed three fresh-provider repeats with all acknowledged writes verified by
readback. The source snapshot was
`f46c98c66a78c84bac2244eb739d7548d4ceb056`; the release binary SHA-256
was `b75352d08f379318f0a872fce7286d13590715eb49aecf7d53cc2cc7906bca7a`.
All response proofs were Object, every window ended with zero root sequence
lag, and no node reported CPU throttling.

| Shape | Highest fully served offered rate across repeats | Fully served logical writes/s | First overloaded offered rate |
| --- | --- | --- | --- |
| Uniform writes | 24, 32, 32 actions/node/s | 71.96, 95.89, 95.88 | 32, 48, 48 actions/node/s |
| One hot writable Cell | 24, 24, 16 actions/node/s | 71.99, 71.92, 47.99 | 32, 32, 24 actions/node/s |
| Skewed, 20% writes | 24, 24, 16 actions/node/s | 14.40, 14.40, 9.60 | 32, 32, 24 actions/node/s |

The threshold varies between repeats. Uniform overload mixed scheduler-late
arrivals with owner refusals in two repeats; hot overload mainly returned
owner `not_started` capacity refusals. Skewed overload was only 3–5
scheduler-late arrivals despite low CPU use, so it is a harness scheduling
limit, not evidence of a Cell write limit. At hot overload, owner 0 response
p95 was 77–169 ms while publication p95 was 36–42 ms and capture p95 stayed
under 1 ms. Its provider PUT p95 was 6.7–7.5 ms, GET p95 1.9–2.1 ms, and
HEAD p95 1.4–1.6 ms. Provider operations can overlap, so these percentiles
cannot be added to infer one command's critical path. The gap between owner
response and publication requires the actor queue and SQL worker timings added
after this run before choosing a write-path optimization.

The three `verification.json` SHA-256 digests are
`1624b007bd18b19b0e5b498a4cdc307fcfdc30a2320c2ebbd0271f097838c31f`,
`11d03fdc4835bd00a9fc0c90211cb9fd3a21d543ef4f91dc9abb4a64dd15d51c`,
and `d0e41bf722a143149742c47477885d7ea4442a7717d09f7cb6083c916df03e43`.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36651191247`.

## Actor and worker timing attempt

[CI run 36653277555, attempt 1](https://github.com/crabbuild/cellule/actions/runs/36653277555)
passed readback and evidence integrity in all three repeats. The source
snapshot was `7e1c89c20d62b403ebcace3d208381bcfdfd6485`; the binary SHA-256
was `d8778c143fe99b9ff5ff2a4c619bba8f7e8a3cdd7548d1d3a92d6fd64fe22880`.
All response proofs were Object and root lag was zero at window ends. The
fully served uniform rate varied from 2 to 16 actions/node/s, while the first
overloaded skewed rate ranged from 4 to 16. Some windows stopped after a
single scheduler-late arrival at 4 actions/node/s with 0.8–2.1% node CPU use
and no CPU throttling. Worker, publication, and provider tail timings also
spiked at these low rates. This attempt does not yield a stable saturation
point or one repeatable dominant phase. A same-revision rerun on a fresh CI
runner is required before selecting a write-path change.
Attempt 2 built the same revision but could not start the first repeat:
the pinned `bucket-init` image pull returned `toomanyrequests: Data limit
exceeded` from its public registry. It produced no capacity measurements.
The docs-only rerun on source `f9b918a` ([CI run
36657593245](https://github.com/crabbuild/cellule/actions/runs/36657593245),
attempts 1 and 2) failed at the same image pull before traffic. The
qualification fixture now pins the same AWS CLI 2.27.41 version from Docker
Hub at manifest digest
`sha256:bc6b7bba44ce38f9604ede49c584824af919047ea03fbcc7c7610671fdef95d8`;
the object-store image, resource limits, rate schedule, and verifier are
unchanged.
Its bucket-init completed against a fresh local RustFS Compose volume; the
three-repeat CI result is reported below.

The attempt-1 `verification.json` SHA-256 digests are
`b1282ef781b4ca9d962fb43dbf2949662e589ce7737f730fd276473689483673`,
`288e0b50d79780bf4540b9ca2d988880a8d494ca47285a151d5f91964fbd257f`,
and `d9abe2dc3ca698dda71e586da118bc8af0e7fdf2b97b982d963ff4946d108e18`.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36653277555`.

## Final hot Cell attribution

[CI run 36655966439](https://github.com/crabbuild/cellule/actions/runs/36655966439)
passed three fresh-provider repeats on source snapshot
`3d01ab9494f76cdf7cb249098ce1814e89f2b203` and binary SHA-256
`fe847a82c53e456b1f71c32d9309eeb6a6e8e864372a183230db9bedf86eb842`.
All acknowledged writes passed readback, all response proofs were Object,
every window ended at zero root lag, and no node reported CPU throttling.

| Shape | Fully served offered rate across repeats | Logical writes/s | First overloaded rate |
| --- | --- | --- | --- |
| One hot writable Cell | 24, 24, 24 actions/node/s | 71.96–71.99 | 32, 32, 32 actions/node/s |
| Uniform writes | 24, 32, 32 actions/node/s | 71.96–95.92 | 32, 48, 48 actions/node/s |
| Skewed, 20% writes | 96, 64, 96 actions/node/s | 38.40–57.59 | 128, 96, 128 actions/node/s |

At hot overload, owner 0 published 62.28–65.10 roots/s. Mean publication
time was 15.10–16.00 ms/root, with 11.64–11.69 provider requests per
acknowledged write. Actor queue p95 climbed from 49–83 ms at the fully served
point to 142–160 ms at overload, while SQL worker round-trip p95 was only
2.03–2.28 ms at overload. Publication p95 was 37–43 ms, of which root
preparation p95 was 33–37 ms. Supported hot action p95/p99 varied from
112–198/175–222 ms; overloaded action p95/p99 was 288–322/324–435 ms.
Owner `not_started` capacity refusals began at the 32 actions/node/s point.
Together these observations identify serialized object publication as the
hot Cell throughput limiter on this object-proof profile. They do not yet
distinguish predecessor verification, directory work, immutable uploads, or
compaction within preparation; that split is required before code changes.
Uniform and skewed thresholds varied between repeats, so this result is
specific to the hot shape.

The three `verification.json` SHA-256 digests are
`40e4908a169013a80fe873f4aaf0d6f355872545a72212addb0d712281724e31`,
`58740dbf1d5a57ed16b2138c011e2f694a4900b728967dee019b32a5eb187717`,
and `7d379162a637ba97dd197a8bc85c9d3abebe584454f9d7f65a683c065fb289be`.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36655966439`.

## Qualification image rerun

[CI run 36659182959](https://github.com/crabbuild/cellule/actions/runs/36659182959)
passed all three object-proof repeats after changing only the bucket-init
image registry. The PR test-merge source was
`4b52795bf23cd32b72c820269562b6e3e7d8d0fc`; the release integration
binary SHA-256 was
`8b23d9b7b88c082869a9bcab20e19ebbd345c8165f5c1ea5bb38d545d9d4d850`.
All three reports passed integrity and acknowledged-write readback checks,
every response winner was Object, root lag ended at zero, and the hot owner
reported zero CPU throttling.

| Shape | Fully served, repeats 1–3 | First overloaded, repeats 1–3 |
| --- | --- | --- |
| One hot writable Cell | 16, 16, 16 actions/node/s | 24, 24, 24 actions/node/s |
| Uniform | 24, 32, 32 actions/node/s | 32, 48, 48 actions/node/s |
| Skewed, 20% writes | 48, 48, 48 actions/node/s | 64, 64, 64 actions/node/s |

At hot overload, owner 0 published 57.84–59.05 roots/s. Its actor queue
p95 was 136–142 ms, SQL worker round-trip p95 2.53–2.75 ms, publication
p95 37–43 ms, and root preparation p95 32–38 ms. Provider requests per
acknowledged write were 11.66–11.73. This independently supports serial
object publication as the hot Cell limiter, while the lower hot rate interval
than run 36655966439 shows that exact throughput is sensitive to the shared
CI runner. A controlled A/A baseline is required before claiming a
code-change gain.

The `verification.json` SHA-256 digests for repeats 1–3 are
`5ff25de33a3c9edacd677b3e724b0003d28aceea1f0f2cb3e8f9b729e85d9086`,
`31035c59762a5d34eea412a52b412924f9fded01cca9085c05930a5ba347194c`,
and `bfbef7176b533f326d1e536122a814afbf6e2b7d8bbcfe3184574fcbac0b4940`.
The downloaded artifact is under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36659182959`.

## First networked follower-proof comparison

[CI run 36662251677](https://github.com/crabbuild/cellule/actions/runs/36662251677)
passed both capacity jobs, each with three fresh-provider repeats and the same
release binary (`69ad461750472d181e1a326a2d4f5e6ef8ade22ca14920e9b4c4e40ad83bf45f`).
The test-merge source was `f2a5db377cbc11da01278914bc6fa1996b4b4565`.
The follower lane used a signed TCP endpoint, two private-disk followers per
owner, and the object-only lane's 12 Cells, arrivals, resource limits, and
readback rules. Every acknowledged write passed readback, and final published
roots covered all receipts. Follower response winners were Fleet/Object
2,245/24, 3,740/7, and 4,657/24 across repeats 1–3. The object lane used
only Object proofs.

| Shape | Object fully served offered rate, repeats 1–3 | Follower fully served offered rate, repeats 1–3 | Follower first overloaded rate |
| --- | --- | --- | --- |
| Uniform writes | 16, 32, 32 actions/node/s | 4, 24, 24 actions/node/s | 16, 32, 32 |
| One hot writable Cell | 24, 16, 24 | 16, 4, 16 | 24, 16, 24 |
| Skewed, 20% writes | 48, 64, 64 | 24, 32, 48 | 32, 48, 64 |

At the common hot rate of 16 actions/node/s, all object repeats were fully
served with action p95 of 48.06–56.90 ms. Follower repeats 1 and 3 were fully
served with p95 of 17.12–26.37 ms; repeat 2 missed four scheduled arrivals
despite p95 of 21.58 ms among successful actions. Hot follower overload at
24 actions/node/s returned 168–238 owner capacity refusals in repeats 1 and
3. The follower path reduces response latency at this matched point, but its
fully served throughput bound is lower and variable on these shared runners.
The two jobs used different runners, so repeat numbers are not paired host
measurements.

The response and publication capacities differ. A fully served hot follower
window in repeat 1 completed about 48 writes/s while publishing 46.56 roots/s
and ended 13 commits ahead of its root. Its final root did drain and cover
every acknowledged receipt. Uniform fully served windows in repeats 2 and 3
ended one commit ahead. These are response-supported windows; their published
roots/s and remaining lag must be read separately. The first overloaded
uniform and skewed follower points were mostly isolated scheduler-late
arrivals, so they do not establish storage saturation.

The three follower `verification.json` SHA-256 digests are
`c927b09f8f0200b05548d02827aba28434014b8fa1dc7cea0e983f0893001e6e`,
`2e2e9a657fc200103224f2830804f9f9d629b390b91500498800cb74f38ee8b7`,
and `12f93bc42bd015f081f528504ff82f580edfe8db3c78eef7b5d1a52e53d6f75c`.
The corresponding object digests are
`7ddbbf67f22b8554738c73b73d2be76338f52deb46bb9223f1cda83dca22edca`,
`2cf37c2a34158f9e79a58850fc64873924642149fdd83b7e2d9d326adc90a24f`,
and `64c9182aa10c691a8ae32da608b9533f8a0384b02f08f6c9e8e0d3e438f83ac9`.
Raw logs, samples, provider digests, and these reports are under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36662251677`.
The enclosing PR check failed in its separate reader smoke because that
fixture still used a shared signing key after advertisements switched to
per-node keys. The next revision corrects that mismatch and adds network
append duration evidence; this capacity result remains valid for its pinned
binary and must be followed by a clean full rerun.

## Clean follower-proof rerun and variability

[CI run 36663653146](https://github.com/crabbuild/cellule/actions/runs/36663653146)
passed the workspace smoke and both three-repeat capacity jobs. The
test-merge source was `74933c331e477c36f01b0fb508fa555bf897d19f`, and both
capacity lanes used release binary SHA-256
`e4e48c7c50c8dc5a1a2c1f542fb3df6d38517e93fbc8926476c2024dcaf9c5bd`.
The source and binary were
pinned within the run; each repeat used a fresh provider volume and the same
12-Cell schedule. Every acknowledged write passed readback, every final root
covered its receipts, and no node reported CPU throttling.

| Shape | Object fully served offered rate, repeats 1–3 | Follower fully served offered rate, repeats 1–3 |
| --- | --- | --- |
| Uniform writes | 2, 4, 4 actions/node/s | 16, 4, 4 actions/node/s |
| One hot writable Cell | 4, 4, 16 | 4, 4, 2 |
| Skewed, 20% writes | 4, 4, 4 | 2, 4, 2 |

Most first-overload points in both lanes missed only 1–20 scheduled arrivals,
including at 4 actions/node/s. They cannot identify a stable storage throughput
limit. At the fully served hot point, object action p95 ranged from 50.42 to
147.86 ms; follower action p95 ranged from 58.33 to 155.84 ms. The new
end-to-end member append measurement had owner-0 p95 of 17.38–141.02 ms at
the follower hot fully served points. It includes member resolution, TCP,
authority lookup, and follower fsync; it does not yet attribute those phases.
The follower owner used 13.14–14.35 object-provider requests per acknowledged
hot write versus 11.12–11.69 in the object lane. This is consistent with
extra authority reads in the signed append path, but the current counters do
not prove which read or provider wait caused the tail. The first run's hot
latency gain is therefore an observed result on that pinned binary, not a
repeatable improvement across runners. A controlled A/A baseline and phase
split are needed before optimizing this transport path.

Follower `verification.json` SHA-256 digests are
`697bb3e5f88d90a9cf6fc802ebf025a643a3cbf479b3254838e5410ada557fb7`,
`f97b12e31794e9767c48e713010f1087c684a025ee44625272b1afb0bf0362c7`,
and `28c5fcfb130f23a2f4b2842804fd64dc99a62f46f9a5225677dd8bb762e462c6`.
Object digests are
`264f2b65f81fff66ba8d87b246b551fb2840bb8859bf74aa9de415c4a3afee7f`,
`cd37d078b95f4b870db37bb22cecd455b196c29b044b5b9bfc24c078f72add6c`,
and `93c6b119f3e76a0cf1a2319703c9f0956cb2e28970d4d28b84af2bf918b521b4`.
Raw evidence and provider image digests are under
`$HOME/Workspace/crabbuild-target/cellule-capacity-5ca5/ci-run-36663653146`.

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

The original object-only profile could not supply a follower-enabled
comparison. The later follower-proof lane above adds a networked node-log
transport and authority enrollment; a separate process test covers owner-loss
recovery. The added publication observation aggregates root preparation and
provider I/O; it cannot by itself distinguish predecessor
GET/HEAD, immutable PUT, and provider wait inside that phase. Those limits
prevent selecting a safe publication change from this evidence alone.

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

A publication optimization requires a dedicated provider environment and a
repeatable A/A rate interval before modifying publication. It then splits root
preparation into predecessor reads, directory work, uploads, provider wait,
and CAS. A follower-enabled lane runs separately, with recovery proof and
eventual root drain alongside response throughput.
