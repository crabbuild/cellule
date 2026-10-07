# Docker write verification

This harness runs the same SQL application on Cellule and pinned celld v0.6.1.
It uses 1,000 Cells, 96-byte values, INSERT plus SELECT in one transaction,
and a two-hour durable request/result ledger. It is **SQL application parity**;
it does not reproduce a bounded KV upsert benchmark.

Use a dedicated Linux Docker context. The current shared-VM profile gives
nodes an 8-CPU/16-GiB ceiling, tmpfs state of 4 GiB, a 2-CPU/2-GiB RustFS
provider, and a 4-CPU/4-GiB client. These ceilings exceed the shared VM's total
CPU; report contention. tmpfs does not qualify physical-device durability.
HTTP endpoints, certificates, credentials, and placement are fixture policy.
The credentials in these scripts are synthetic and used only by this fixture.

## Build and run

All artifacts, build caches, binaries, logs and audit journals go
outside the checkout. Each case gets a fresh labeled Docker volume for RustFS
data on the Linux filesystem, retained for investigation. Its name is recorded
in `store-data/volume.json`; it is never a host filesystem bind mount. The build records every exported source digest, the
adapted fixture source, immutable binary hashes and pinned image digests.
Never reuse an artifact directory for a new build.
Retained volumes share the Docker filesystem's byte and inode budgets. Startup
requires at least 10 GiB and one million free inodes. The runner records provider
filesystem counts before, during and after each case; falling below 2 GiB or
200,000 free inodes invalidates measurement. Archive and hash old test volumes
before removing their exact containers/volumes, or expand the dedicated test
disk. A fresh volume alone does not reset the shared filesystem's capacity.
Build caches are partitioned by every adapted source digest and the compiler
image. Before measurement, the runner reads a persisted root from the provider
and checks its codec version against the exported source; the small-root
candidate must also contain a packed dependency. This catches stale local Cargo
libraries that binary/source manifests alone would miss.

```sh
python3 scripts/perf/build.py --context dedicated-linux \
  --artifacts /absolute/external/candidate
python3 scripts/perf/run.py cellule-fleet celld-fleet \
  --context dedicated-linux --artifacts /absolute/external/candidate \
  --tag paired --seconds 300 --warmup 30 --repetitions 3 \
  --write-rates 100 200 500 15000
python3 scripts/perf/report.py /absolute/external/candidate/cellule-fleet-parity-paired-r1
```

Put case names before `--write-rates`, whose list extends until the next option.
Run sustainable-rate searches before target stress. A completed run is not a
qualified run. Failed runs are retained and the runner exits nonzero if any
case, audit or drain is incomplete. Overloaded completion rates are not capacity.

For latest main after PR 65, retain the same measurement hooks
and workload while leaving publication and storage codecs at the baseline:

```sh
python3 scripts/perf/build.py --context dedicated-linux \
  --artifacts /absolute/external/baseline \
  --framework-ref 18eff0f7af47fac09b993157bb444e582072d7cf \
  --measurement-overlay
```

The overlay is explicitly recorded and supported for that revision and the
audited PR 65 foundation `397f500a39d0cd3a84724a82fe66d6a56e76f726`.
Their production source is identical; main added three design documents.
The overlay build is not an unmodified main build. Client and auditor hashes must match across
comparisons. Serialize runs; a context lock prevents concurrent harness runs.
The harness owns only its named, labeled provider and client containers.
Its loopback ports must be free of other workloads.

For a read-only point, supply `--write-rates 0 --read-rate N`. For a mixed
point, use the same offered write rate on both systems and add `--read-rate N`.
Add `--hot-read-cells 10` for the 1% hot-read case. Record qualification of
read-only and mixed profiles separately; an aggregate read/write rate is not a
read-capacity result.

After establishing qualified capacity, add `--overload-capacity N`. The runner
then offers `1.5 × N` for 60 seconds and immediately `0.5 × N` for 30 seconds,
with no intervening warmup. Both cohorts enter the warm and cold audits.
The report separates these phases from steady capacity and records the total
fleet drain duration. The supplied reference rate still requires paired
qualification, and HTTP evidence alone does not prove every refusal preceded SQL.

## Evidence contract

Latency begins at scheduled arrival. Journals include every attempted request;
counts include unissued offers, client queue drops, HTTP errors and late
completions. The client samples raw cumulative histograms and provider counters
at window start, every minute, and window end; the report subtracts counters
rather than percentiles. Endpoint session, schema and counter resets invalidate
the metric window. All storage families sum to provider operation counters.
Reports include range GETs, copy and multipart operations separately; a full-GET
count alone hides compaction traffic. These observations count storage API calls.
Retries inside a provider SDK require provider-side telemetry for exact HTTP
attempt counts. Families distinguish immutable data, Cell authority, node authority, owner and
receiver enrollment, and other work. Enrollment scopes follow each GET through
stream completion. Compaction and conditional PUT roles need finer attribution;
all attempts and successful PUTs remain in the reported provider totals.
Schema 3 exports nonzero cumulative histogram buckets with their original
indices and fixed bound, preserving 100-us resolution and overflow. The client
primes exporters and sampling connections before warmup. Use `--telemetry off`
only to measure exporter overhead; that diagnostic cannot qualify. Compare
response, confirmation, fleet-proof, capture/checkpoint, and publication timings
separately instead of attributing publication time to the command ACK.

The producer emits every offer scheduled inside the window even if its final
wakeup is late. It preserves the original due time: lateness remains in the
scheduled latency and inside-window completion counts, and a full queue records
a drop. Overload and immediate recovery use explicit bounded phase metadata
with zero warmup; ordinary qualification still requires 30 seconds warmup.

Warm and bucket-only cold audits GET every acknowledged mutation and retry
**every original command**, checking its exact stored output and sequence.
Original owner and follower containers are removed before cold recovery.
The S3 origin remains in that case's Docker volume.
Provider/node failure and unsuccessful drain prevent a case from passing.
An independent-machine kill or power-loss test remains a separate profile.

New builds use `acknowledged.jsonl`, streamed by a 256-record producer queue
and at most 128 concurrent GET/retry pairs. Collection checks ID uniqueness
with a disposable disk index and reconciles seed, warmup, steady, trailing,
overload and recovery ACK counts against client totals. The retained
`acknowledged-manifest.json` hashes the stream and source journals; the runner
checks the stream before and after both audits. Missing successful records fail
the case. No multi-million-command response list is held in memory.
Build manifests pin the collector, control scripts and celld adapter bytes.
Cases preserve the exact loaded runner source. Rebuild old artifact directories
into fresh directories before using the current JSONL runner; retain their
original runner and JSON-array journals as historical evidence.

Individual reports expose achieved rate, failed delivery gates, window costs
and confirmed commands per selected root. They cannot establish the complete
[proposal qualification](../../docs/write-performance-proposal.md): that also
requires three matched repetitions, A/A variance, publication age/frontier
stability, read guardrails and the overload/recovery experiment. Missing evidence
stays explicitly unverified. Raw artifacts are never committed.
Cellule stability reports fit the debt and oldest-publication age over the last
three one-minute segments. A positive slope fails; missing, late, reset, or
fenced observations cannot pass. Native log frontiers remain observations and
grant no durability or collection authority.

For a machine-readable comparison, write an external JSON file with `baseline`,
`candidate` and `celld` lists of case directories, then run:

```sh
python3 scripts/perf/compare.py /absolute/external/matrix.json \
  --output /absolute/external/comparison.json
```

The comparison requires identical offered point sets, driver binaries, pinned
images, fixture bytes, loaded runner, Docker host, resources and point
distributions, including hot reads and preceding write offsets. Historical
cases lacking host/runner or fixture records are explicitly marked unverified
for those identities. Its read ratios describe each matched point;
they do not establish the baseline's highest qualified read capacity.
