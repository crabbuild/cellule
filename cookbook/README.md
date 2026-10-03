# Cellule runnable application cookbook

These application crates embed Cellule through its public Rust APIs. Each
reference application keeps its domain code in a reusable library and owns
its process, provider, and ingress separately. The
[catalog](../docs/cookbook.md) defines the complete 28-application scope and
acceptance criteria; implementation proceeds in complete vertical slices.

| Application | Current implementation | Run |
| --- | --- | --- |
| [Taskboard](apps/taskboard/README.md) | Typed task creation, assignment, closure, revision checks, durable rejections, outcome resolution, bounded reads, signed lease enrollment, and persistent restore. | `sh cookbook/scripts/taskboard.sh` from the repository root. |
| [Settings](apps/settings/README.md) | Conditional KV bundles, opaque versions, concurrent editors, delete/recreate checks, bounded prefix reads, supervised expiry, and persistent restore. | `sh cookbook/scripts/settings.sh` from the repository root. |
| [File vault](apps/file-vault/README.md) | Frozen multipart plans, staging across restarts, retained phase resolution, conditional publication/deletion, bounded verified downloads, and private artifact scope. | `sh cookbook/scripts/file-vault.sh` from the repository root. |
| [Work queue](apps/work-queue/README.md) | Leased consumers, permanent receiver idempotency, pause/resume, signed native dead-letter delivery, SQL inspection, redrive, crash recovery, and owned worker drain. | `sh cookbook/scripts/work-queue.sh` from the repository root. |
| [Interval scheduler](apps/interval-scheduler/README.md) | Durable Cron occurrences, pause/resume/delete, bounded catch-up, signed Effects, SQL inbox deduplication, lost-reply resolution, and recovery during delivery. | `sh cookbook/scripts/interval-scheduler.sh` from the repository root. |
| [Approvals](apps/approvals/README.md) | Human authorization, immutable votes, deadline precedence, run-scoped signals and history, requester controls, durable mailbox Activities, and recovery after external publication. | `sh cookbook/scripts/approvals.sh` from the repository root. |
| [Tenant workspace](apps/tenant-workspace/README.md) | Verified bearer ingress, pre-dispatch tenant and role authorization, conditional SQL project documents, KV preferences, bounded administration, retained resource-bound retries, and HTTP drain. | `sh cookbook/scripts/tenant-workspace.sh` from the repository root. |
| [Entity registry](apps/entity-registry/README.md) | Canonical device Cells, conditional attributes, atomic directory Effects, visible pending progress, monotonic projections, independent ownership transfer, and recovery during delivery. | `sh cookbook/scripts/entity-registry.sh` from the repository root. |
| [Credit quotas](apps/quotas/README.md) | Customer allowances, atomic holds, terminal consumption/release, permanent business identities, durable rejections, same-owner contention, bounded coherent reads, and persistent recovery. | `sh cookbook/scripts/quotas.sh` from the repository root. |
| [Event reservations](apps/reservations/README.md) | Event-local seats, permanent hold identities, buyer/generation checks, deadline precedence, autonomous Workflow timers, signed conditional expiration, bounded callback receivers, and persistent crash recovery. | `sh cookbook/scripts/reservations.sh` from the repository root. |
| [Webhook delivery](apps/webhook-delivery/README.md) | Immutable subscriber snapshots, signed native fan-out, bounded HTTP Activity retries, authenticated receiver admission, actual dropped replies, permanent business keys, recovery after receiver application, and owned HTTP drain. | `sh cookbook/scripts/webhook-delivery.sh` from the repository root. |
| [Media pipeline](apps/media-pipeline/README.md) | Immutable PNG Blob inputs/results, bounded deterministic processing, pinned Workflow links, authenticated Activity publication, native retry after published output, manifest reuse, and persistent crash recovery. | `sh cookbook/scripts/media-pipeline.sh` from the repository root. |
| [Report export](apps/report-export/README.md) | Immutable sealed SQL versions, bounded CSV page Activities, durable cursors, exact dataset verification, manifest reuse after publication, and byte-identical persistent crash recovery. | `sh cookbook/scripts/report-export.sh` from the repository root. |
| [Endpoint monitor](apps/endpoint-monitor/README.md) | Scheduled HTTP probe Workflows, immutable observations, incident watermarks, atomic notification intent, and a signed notification inbox; persistent restart, redelivery, and cold restore verified. | `sh cookbook/scripts/endpoint-monitor.sh` from the repository root. |
| [Checkout](apps/checkout/README.md) | Qualified order saga, atomic stock reservations, independent payment simulator, verified compensation, explicit review, signed callbacks, native action recovery, and cold restore. | `sh cookbook/scripts/checkout.sh` from the repository root. |
| [Resource provisioning](apps/provisioning/README.md) | Qualified provider lifecycle, stable external keys, signed callbacks, cancellation, retrying cleanup, read-only due hints, native recovery, and cold restore. | `sh cookbook/scripts/provisioning.sh` from the repository root. |
| [Release pipeline](apps/release-pipeline/README.md) | Qualified immutable Blob artifacts, human approval, independent HTTP deployment, conditional predecessor compensation, retained definition inventories, Activity reclamation, and interrupted-demo recovery. | `sh cookbook/scripts/release-pipeline.sh` from the repository root. |
| [Project tracker](apps/project-tracker/README.md) | Qualified project aggregates, immutable Blob attachments, original-input linking reconciliation, revisioned signed dashboard delivery, crash recovery, and interrupted-demo drain. | `sh cookbook/scripts/project-tracker.sh` from the repository root. |
| [Telemetry ingest](apps/telemetry-ingest/README.md) | Qualified durable Queue ingress, permanent device sequences, original batch audits, bounded signed summary shards, native crash recovery, and a retained interrupted demo. | `sh cookbook/scripts/telemetry-ingest.sh` from the repository root. |
| [Support desk](apps/support-desk/README.md) | Ticket conversations, immutable private attachments, generation-fenced deadline Workflows, signed escalation Effects, retryable HTTP Activities, authorized customer/agent ingress, and a persistent idempotent receiver. Local and CI quality/process/demo checks pass. | `sh cookbook/scripts/support-desk.sh` from the repository root. |
| [Usage ledger](apps/usage-ledger/README.md) | Permanent account events, signed period projections, account close fences, complete-set reconciliation, native close Workflow, and verified immutable CSV report. Package quality checks and the persistent crash/restart scenario pass locally; CI qualification is pending. | `sh cookbook/scripts/usage-ledger.sh` from the repository root. |
| Other catalog applications | 7 planned; no placeholder application crates. | See the catalog's implementation order. |

Twenty-one applications are implemented and runnable. Twenty are already
CI-qualified; Usage Ledger passes local package and process verification and
awaits CI qualification. Seven further applications remain planned.

The default command starts a pinned local RustFS container, initializes a
private bucket, runs the selected application scenario, and drains the node. Rust 1.97
or newer and Docker Compose are required; no cloud account is needed.
First-run image downloads and compilation take longer than subsequent runs.
Storage is exposed on loopback port 19000 and uses development credentials
only. SQLite working files live under `cookbook/.state/<application>`; authoritative
objects persist in the cookbook's Docker volume.

The shared launcher starts storage for serving commands; help and preparation
run without Docker. During serving, interrupt and Unix termination signals
stop the CLI operation and drain accepted work. Retained mutation files or
upload plans remain available to resolve interrupted outcomes.

```sh
sh cookbook/scripts/taskboard.sh
sh cookbook/scripts/taskboard.sh list cookbook/.state/taskboard release
sh cookbook/scripts/local-storage.sh down
```

`down` retains authoritative objects. The explicit `reset` command removes
the cookbook's Docker volume and all application objects in it. Delete local
working files only after the application drains; they can then be restored
from the retained authoritative objects. Do not reset storage while an app
is running.

## Workspace and verification

This is a separate workspace and lockfile. Framework packages continue to use
the root workspace and never depend on cookbook applications. Shared
[support](support/README.md) supplies exercised application infrastructure;
domain libraries own schemas, stable identities, codecs, and business rules.

```sh
cargo fmt --manifest-path cookbook/Cargo.toml --all --check
cargo check --manifest-path cookbook/Cargo.toml --workspace --all-targets --locked
cargo test --manifest-path cookbook/Cargo.toml -p cellule-cookbook-taskboard --test application --locked
cargo clippy --manifest-path cookbook/Cargo.toml --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --manifest-path cookbook/Cargo.toml --workspace --no-deps --locked
python3 cookbook/scripts/check-layout.py
```

The CI workflow first checks the complete workspace, then runs each application
in its own process-scenario job with private local storage. Its matrix comes
from Cargo membership; every application must supply a launcher and persistent
scenario. Each job records source and lockfile identity and retains its evidence.
Process qualification precedes the demo so versioned scenarios establish their
predecessor before a demo opens the current inventory in retained storage.

Use a checkout-specific target directory under the mounted Workspace build
volume. Broad suites and process scenarios must run in CI or an isolated
verification snapshot. The [taskboard process scenario](scenarios/taskboard.py)
runs an already-built binary against private local S3 storage and verifies
command outcomes across independent processes. It does not run automatically
as a credentialed or Docker-dependent Cargo test.

The current evidence covers local application behavior. Provider, scale,
fault-profile, and upgrade qualification remain governed by the framework's
[qualification contracts](../crates/cellule-runtime/qualification/README.md).
