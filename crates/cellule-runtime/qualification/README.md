# Cell-runtime qualification profiles

The JSON files in `profiles/` are canonical, versioned threshold inputs. The
receipt signer binds the profile digest; changing a threshold therefore makes
old evidence unusable. Profile schema 2 also binds workload thresholds to the
measured environment: protected profiles carry required provider and topology
labels, minimum throughput, peak RSS/local-disk/file-descriptor ceilings, and
an object-store call ceiling. Matrix verification requires the corresponding
`cells`, `operations`, `duration_secs`, `p99_latency_ms`,
`peak_local_disk_bytes`, and `peak_file_descriptors` metrics when a profile
sets those limits; a missing metric is a failed gate, not an assumed zero.
The release CLI additionally rejects protected receipts finished more than
seven days ago or more than five minutes ahead of its verifier clock. The
historical library verifier remains timestamp-neutral; use the fresh protected
matrix entry point for release decisions.

Run the commands below from this qualification directory. From the workspace
root, enter it first so `profiles/` resolves to the checked-in inputs:

```sh
cd crates/cellule-runtime/qualification
```

Generate and verify a deterministic mixed primitive workload in a temporary
directory, leaving the source tree untouched:

```sh
WORKLOAD_DIR="$(mktemp -d)"
cargo run --locked -p cellule-runtime --bin qualification_receipt -- \
  workload "$WORKLOAD_DIR/workload.json" profiles/pr-contract-v1.json 7
cargo run --locked -p cellule-runtime --bin qualification_receipt -- \
  verify-workload "$WORKLOAD_DIR/workload.json" profiles/pr-contract-v1.json
```

After a real harness has written one receipt and one or more raw artifacts for
each matrix workload, build the canonical manifest from that evidence tree:

```text
evidence/
├── receipts/<workload>.json
└── artifacts/<workload>/<artifact files>

cargo run --locked -p cellule-runtime --bin qualification_receipt -- \
  manifest evidence/qualification-matrix.json evidence/
```

The builder emits schema-2 rows in the required order and rejects missing,
symlinked, or non-file evidence entries. The manifest output must be directly
under the evidence directory so its relative paths remain verifiable. It only
indexes files; it does not create or sign receipts. Run `verify-matrix` with the
protected profile and pinned attestation key before treating the resulting
manifest as release evidence.

The protected release bundle has one canonical fresh-process verifier. It
requires all nine provider, scale, compatibility, and fault profiles, rejects
symlinks anywhere below the bundle, checks each profile name and protected
threshold contract, and verifies every matrix against the same source, image,
and pinned signer:

```text
cargo run --locked -p cellule-runtime --bin qualification_receipt -- \
  verify-protected-bundle protected/ <source-sha> <image-digest> \
  <trusted-signer-hex>
```

The workflow and release gate call this command directly; they only retain
their shell-side byte-for-byte comparison between each supplied profile and
the tracked profile from the exact source checkout.

Typed qualification adapters may use the bounded `QualificationWorkload::run_concurrent`
entry point when scheduled operations are independent or idempotent. The serial
`run` entry point remains the safe choice for workloads with application-level
ordering dependencies; both paths retain the same streaming schedule, counters,
latency histogram, and logical outcome digest. Protected adapters should use
`run_with_case_coverage` or `run_concurrent_with_case_coverage`, which require
each result to bind the lifecycle case it exercised.

After a protected adapter has captured a verified run artifact, write the
non-secret execution identity and fault/ownership observations with the
`QualificationExecutionEvidence` schema, then bind the receipt with the
trusted signing key held outside the evidence directory:

```text
cargo run --locked -p cellule-runtime --bin qualification_receipt -- \
  bind-protected receipt.json <source-sha> <image-digest> profile.json \
  execution-evidence.json /run/secrets/qualification-signing-key \
  run-artifact.json workload.json raw/provider-events.json
```

The command rejects local profiles, non-canonical evidence, mismatched
workload/run artifacts, missing protected resource measurements, and a signing
key symlink. It never generates a protected receipt from the synthetic
`emit` path; the resulting receipt must still pass `verify-matrix` with the
pinned public key before release packaging.

Named provider profiles also require one canonical
`QualificationProviderEvidence` raw artifact beside the run and workload
artifacts. It binds the provider/profile digest and workload seed, and records
successful conditional, range, and multipart checks. Missing, duplicated,
partial, or mismatched provider semantics are rejected by both the binder and
the fresh-process matrix verifier.

The default workload contains a deterministic, seed-bound case schedule for each
primitive: `happy`, `retry`, `duplicate`, `expiry`, `cancellation`, `owner-loss`,
and `recovery`. Adapters inspect `QualificationOperation::case()` (or its
bounded hint accessors) to drive the corresponding primitive-specific behavior;
the schedule is a case plan, not evidence that an external provider or owner
fault actually occurred. The workload's outcome counts are forecasts used to
describe that schedule. A measured run binds the scheduled attempt count for
each primitive and records actual acknowledgements, rejections, ambiguity,
retries, and verification independently; it need not reproduce those
forecasts. A PR wiring smoke marks only the primitive/case pairs it actually
exercises and checks; other scheduled pairs stay unmarked.

The measured summary also emits `throughput_ops_per_sec` using a conservative
rounded-up duration; protected profiles still verify the raw elapsed duration
and their independent resource counters. Protected `primitives` receipts must
also match the measured run artifact's cells, operations, duration, throughput,
and p50/p95/p99/max latency metrics in non-decreasing order; a signed receipt
with substituted threshold values is rejected.
Measured run artifacts use schema 4 and include a bounded primitive/case bitset;
named provider/topology profiles reject artifacts missing any lifecycle case.
Those protected profiles also require every acknowledged operation to have an
independent verification result; a partial verification count cannot be
promoted by the receipt binder. The local observed smoke remains allowed to
report partial coverage and is not release evidence.
The PR profile is a correctness gate.
`local-provider-v1`, `fault-v1`, `provider-v1`, `compatibility-v1`, and
`scale-v1` are release-candidate inputs only. The provider-specific
`provider-{s3,gcs,azure}-v1` and `fault-{s3,gcs,azure}-v1` profiles bind the
object-store provider and Kubernetes fault topology explicitly; they do not
claim provider or Kubernetes qualification until a protected run records
matching receipts and artifacts signed by the pinned qualification attestation
key. Profile verification also rejects those non-PR profiles when
the receipt is local, has no measured object-store/RSS counters, has an
environment label that does not match the profile, or has no ownership
watermark proof. The command-line `emit` helper only creates threshold metrics
for the PR correctness profile; protected evidence must come from the real
provider/fault/scale harness so it cannot be promoted from a synthetic local
receipt.

## Release handoff

A host release workflow must fetch protected evidence from a successful
protected run at the exact source revision being released. The run produces one
`protected/` directory with the tracked profiles, verified matrix manifests,
receipts, and raw artifacts written by an operator-installed executable that
owns the provider and Kubernetes boundary. That executable must run the real
workload, isolate provider prefixes or namespaces, inject the profile's faults,
and sample the profile's resources; a local, emulator, or synthetic fallback is
not release evidence.

The workflow must independently verify every required provider, scale,
compatibility, and fault matrix against the release image, the tracked profile,
and the pinned attestation public key in a fresh process, and it must check the
run identity, workflow name, event, attempt, and commit before accepting the
artifact. Missing, stale, symlinked, or malformed evidence fails closed. The
embedding product owns its CI artifact names, runner labels, executable paths,
secret names, deployment topology, and release inputs.
