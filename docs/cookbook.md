# Cellule application cookbook catalog

The cookbook should contain complete, runnable Rust applications that teach
reusable ways to build on Cellule. Each application owns its domain API,
authorization, storage configuration, background workers, and process lifecycle.
A reader should be able to run a meaningful scenario, inspect the durable state,
restart the application, and reuse the demonstrated pattern in another product.

This catalog defines **28 application crates** and their acceptance criteria.
Implementation has started in the separate [cookbook workspace](../cookbook/README.md):
Taskboard, Settings, File vault, Work queue, Interval scheduler, and Approvals
provide runnable journeys covering SQL, KV, Blob, Queue, Cron, Effects, Workflow,
and Activities, with shared persistent node assembly, supervised maintenance,
and owned workers. Tenant workspace adds authenticated HTTP ingress, tenant
isolation, role policy, and bounded administration. Entity registry adds stable
device Cells, explicit projection progress, monotonic signed directory delivery,
and independent ownership transfer. Credit quotas demonstrates atomic customer
allowances, permanent reservation identities, and idempotent consumption and
release. Event reservations adds scarce seat inventory, generation-checked
Workflow deadlines, signed conditional expiration, and bounded receivers for
callbacks to previously served events. [Webhook delivery](../cookbook/apps/webhook-delivery/README.md)
adds immutable subscriber fan-out, HTTP Activities, bounded retry classification,
permanent receiver idempotency, actual dropped replies, and recovery after external
application. [Media pipeline](../cookbook/apps/media-pipeline/README.md) adds
immutable PNG sources and thumbnails, bounded local computation, authenticated
Blob publication, pinned Workflow result links, and native retry after output
publication. [Report export](../cookbook/apps/report-export/README.md) adds atomic
sealed SQL versions, bounded CSV Activities, durable cursors, exact dataset
verification, and recovery after page publication while the live draft changes.
[Endpoint monitor](../cookbook/apps/endpoint-monitor/README.md) adds native
Cron probe Workflows, retained HTTP observations, atomic incident transitions,
and a signed notification inbox, with recovery after SQL publication before
source acknowledgment. [Checkout](../cookbook/apps/checkout/README.md) adds
atomic stock holds, a native order saga, permanent payment identities, verified
compensation, and explicit external review, with independent-process recovery
after authorization and cold restoration of both writer domains.
[Resource provisioning](../cookbook/apps/provisioning/README.md) adds permanent
provider keys, asynchronous lifecycle deadlines, verified resource details,
cancellation and retrying cleanup, read-only due hints, and explicit operator
review. Sixteen applications are qualified and runnable; 12 further applications
are planned.
The five existing
[primitive examples](../crates/cellule-app/docs/examples.md) remain runnable
introductions. The cookbook adds complete application journeys alongside those
examples and the [application integration guide](framework.md).

## Scope and organization

Use a separate `cookbook/` Cargo workspace with its own lockfile, depending on
the local framework crates. Keep the main workspace's seven framework packages
and dependency layers intact. The proposed layout is:

```text
cookbook/
  Cargo.toml
  Cargo.lock
  AGENTS.md
  README.md
  apps/
    taskboard/
      Cargo.toml
      README.md
      src/
      tests/
    ...
  support/             shared process plumbing extracted as apps need it
  scenarios/           process drivers and expected application outcomes
```

Package names use `cellule-cookbook-<slug>`. Each app has a library containing
its domain modules and typed client, and a binary that assembles and serves it.
Keep stable namespace IDs, migrations, operation IDs, codecs, and workflow
definitions with the domain code. Extract shared infrastructure only after
multiple applications need the same implementation; keep product policies in
each application. Framework crates must never depend on cookbook crates.

Start with a CLI or a small domain HTTP API. Add a browser UI only where it
helps explain the application, such as approvals or replica freshness. A
terminal scenario with observable outcomes is sufficient for an operational
application. Every service still supplies real admission, readiness, and
shutdown behavior.

## Application catalog

Each row describes a proposed application, its Cell boundaries, and the failure
scenario required before calling it a reference implementation. SQL Cells may
use entity partitions; KV, Blob, Queue, Cron, and Workflow capabilities use
their declared fixed shards and primitive routing rules. Separate namespaces
mean separate transaction domains even when they share a process.

### State and application boundaries

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `taskboard` | Create a project, add tasks, assign them, and close them; one SQL entity Cell per project. | Typed domain commands, relational constraints, bounded pagination, and reads requiring the write receipt. | Lose a mutation reply; resolve its original request identity and prove the task was changed once. |
| `entity-registry` | Register devices, update their attributes, and look them up; one SQL entity Cell per canonical device key, with a separate SQL directory updated through Effects. | Stable entity keys, typed clients, many independently owned Cells, and an eventually consistent directory. | Move ownership and restart; entity targets remain stable, and a delayed directory update is visible as pending. |
| `settings` | Edit organization preferences and feature flags; KV scopes select fixed shards. | Conditional atomic updates, version conflicts, expiry, and bounded prefix listing. | Race two editors, then delete and recreate a key; the losing or stale version cannot overwrite the new value. |
| `quotas` | Reserve, consume, and release simulated API credits; one SQL entity Cell per customer holding counters and reservation records. | Atomic local invariants, durable business rejection, and idempotent reservation release. | Concurrent reservations cannot exceed the allowance; retrying a release cannot credit it twice. |
| `reservations` | Hold and confirm seats for an event; one SQL entity Cell per event, with a Workflow for hold deadlines and Effects for expiration commands. | Put scarce inventory in one transaction domain; make asynchronous expiration conditional on the hold generation. | Confirmation races expiration; a seat has one final allocation, and an old timeout cannot release a newer hold. |
| `tenant-workspace` | Serve two tenants with identical project names; tenant-scoped SQL project Cells and KV preferences. | Authorize before selecting targets, derive tenant identity from the principal, and bound administrative access. | Attempt to access another tenant through every public route and forged target; reject before dispatch without leaking its data. |

### Content and artifacts

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `file-vault` | Begin an upload, resume parts, complete it, read ranges, replace it conditionally, and delete it; Blob object keys select fixed shards. | Multipart publication, ETag checks, receipt-bound visibility, and verified bounded range reads. | Restart between staging and completion; staged bytes remain invisible, the upload resumes, and recovered content matches byte for byte. |
| `media-pipeline` | Upload an image and produce a thumbnail; Blob source/result namespaces, a Workflow run, and a local image-processing Activity. | Immutable artifact inputs, external computation outside SQLite, and explicit linking of output manifests to workflow state. | Kill a worker after publishing its output but before recording completion; retry reuses or verifies the output without duplicate visible artifacts. |
| `report-export` | Request a paginated report and download its CSV; a SQL dataset Cell, a Workflow, export Activities, and Blob output. | Bounded export chunks, durable progress, deterministic output keys, and an explicit export consistency policy. | Change the dataset and interrupt the exporter; resume under the documented policy and detect duplicates, gaps, or version changes. |
| `artifact-collector` | Inventory a private Blob scope, preview candidates, and collect abandoned parts; enumerate every Cell sharing that artifact scope. | Application-owned maintenance requiring complete references, quiesced writes, a grace boundary, and bounded deletion passes. | Omit a Cell, interrupt enumeration, or fail to stop writers; refuse deletion. Shared referenced parts survive collection. |

For `report-export`, the first implementation should export a sealed application
dataset version. Repeated paginated queries alone do not establish a snapshot
across commands or Cells. `artifact-collector` should start as an offline
maintenance application; an online collector needs a separately proven
coordination protocol.

### Background work and recurring delivery

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `work-queue` | Submit jobs, run competing consumers, pause claims, inspect dead letters, and redrive; Queue producer/shard routing, a native dead-letter Queue, and a SQL inspection receiver. | Token and receipt validation, lease extension, repeatable external work, and supervised worker drain. | Kill a worker after its external action but before ack; redelivery occurs and the idempotent destination records one logical action. |
| `webhook-delivery` | Register a subscription, publish a domain event, deliver to a local receiver, and inspect attempts; SQL source intent and Effects to a delivery Workflow with HTTP Activities. | Transactional intent, subscriber fan-out, HTTP retry classification, destination idempotency keys, and terminal failure handling. | The receiver applies a request but drops the reply; delivery retries with the same external idempotency key and records one receiver action. |
| `interval-scheduler` | Create, pause, resume, and delete recurring reminders; Cron shards deliver Effects to a SQL reminder inbox. | Durable fixed intervals, occurrence identity, bounded catch-up, and explicit installation of scheduler and effect runners. | Restart while a tick or effect is in flight; no due occurrence is lost or applied twice at the destination. |
| `endpoint-monitor` | Probe a local HTTP endpoint periodically, retain checks, and open or close incidents; Cron, SQL monitor Cells, a probe Workflow, and HTTP Activities. | Separate scheduled intent, external observation, durable incident transitions, and notification intent. | Fail and recover the probe target while restarting the monitor; repeated delivery cannot create duplicate incident transitions. |

`interval-scheduler` uses the existing fixed-interval Cron contract. Calendar
expressions, time zones, notification policy, and missed-occurrence policy
belong to an application that elects to implement them.

Native Queue dead-letter targets must themselves be Queue namespaces. A source
Effect supervisor delivers to that Queue; an application consumer may then
record messages in a SQL inspection model. Do not declare a SQL namespace as
the primitive's direct dead-letter target.

### Durable workflows and external systems

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `approvals` | Submit a purchase request, collect decisions, remind approvers, and complete or time out; Workflow ID shards, signals, timers, and Activities. | Deterministic decisions, signal deduplication, explicit human authorization, and observable durable history. | Deliver the same approval twice and race it with a timeout; record one valid terminal decision under a documented precedence rule. |
| `checkout` | Place an order, reserve stock, authorize a simulated payment, and finalize or compensate; order and inventory SQL Cells, a Workflow, Effects, and payment Activities. | A saga: local transactions plus durable coordination, idempotent external actions, and explicit compensation states. | Fail after payment authorization but before finalization; recovery finishes or compensates and exposes any unresolved external outcome. |
| `provisioning` | Request a simulated resource, poll creation, publish its details, and deprovision it; a Workflow, provider Activities, and a SQL resource directory. | Stable provider operation keys, reconciliation after unknown replies, cancellation, and cleanup that can itself retry. | The provider creates a resource but loses its reply; reconciliation finds that resource and avoids creating another one. |
| `release-pipeline` | Build a local artifact, request approval, deploy to a local target, verify, and roll back; Workflow, Activities, Blob artifacts, and Effects to a SQL release record. | Pin inputs and workflow definitions, retain definitions for live runs, and distinguish cancellation from compensating external work. | Restart during deployment and while introducing a new definition; the old run follows its pinned definition and rollback remains repeatable. |

External systems in these apps should be included local simulators with durable
idempotency and fault controls. Live payment, cloud, or deployment integrations
can be adapters later. Cancelling a Workflow does not automatically reverse
an external action; compensation and reconciliation are application behavior.

### Composed product applications

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `project-tracker` | Manage issues, attach files, and browse a tenant dashboard; SQL project entity Cells, Blob attachments, and Effects into a SQL dashboard projection. | Choose aggregate boundaries, maintain an application-defined read model, and reconcile references across independently committed Cells. | Delay and reorder dashboard updates; revisions prevent regression. An attachment published before its issue link can be reconciled. |
| `telemetry-ingest` | Submit simulated device events, inspect per-device state, and query summaries; Queue ingress, SQL device Cells, and Effects into bounded summary shards. | Durable ingestion, source event deduplication, sequence-aware handling, and eventual aggregation without claiming Queue FIFO. | Repeat and reorder events and restart a consumer; device state and summary counts match the documented late-event policy. |
| `support-desk` | Open a ticket, add messages and attachments, assign an agent, and escalate overdue work; SQL ticket Cells, Blob, Workflow timers, Effects, and notification Activities. | A complete domain API combining local conversation invariants with durable deadlines and external notification delivery. | Resolve a ticket as escalation fires; generation/state checks prevent obsolete escalation from changing the resolved ticket. |
| `usage-ledger` | Record simulated usage, reconcile a billing period, and export a statement; SQL account Cells, Effects to period Cells, a closing Workflow, and Blob reports. | Immutable source event identity, eventual aggregation, explicit period sealing, and reproducible accounting checks. | Retry events and delay delivery across period closure; reconciliation accounts for every accepted event or reports an incomplete period. |

The dashboard and summaries are custom SQL modules receiving Effects, not a
built-in Projection API. A receipt proves a position in its own Cell; reading
a source at its receipt does not prove that a projection has caught up. These
apps need explicit progress/revision information and reconciliation commands.
`usage-ledger` uses synthetic units and demonstrates storage patterns, not tax
or financial compliance.

### Operations and fleet behavior

These are runnable administrative applications with concrete workloads and
observable results. Keep destructive scenarios confined to their own local
state directories and storage prefixes.

| Application slug | Runnable journey and Cell topology | Reusable pattern | Required failure demonstration |
| --- | --- | --- | --- |
| `replica-analytics` | Write a SQL dataset through its owner, then query admitted readers at a requested receipt; SQL Cells with host-managed read replicas. | Explicit owner/replica policy, observed positions, reader lifecycle, and separate native-memory budgets. | Remove or lag a selected reader; return the replica error, then recover reader service without silently changing the requested policy. |
| `fleet-service` | Serve a small entity registry across three local processes, route commands to owners, move Cells, and drain a node; SQL Cells, enrolled sessions, and authenticated peer HTTP. | A reusable serving assembly for readiness, pinned mTLS, signed peer scope, owner routing, takeover, and shutdown. | Kill an owner and separately drain a live owner under traffic; accepted outcomes survive and an old session cannot continue writing. |
| `recovery-inspector` | Commit known records, record acknowledged outcomes, remove local working state, and reconstruct from authoritative storage; SQL Cells and an optional follower-durability scenario. | Preserve pending request evidence, select the authority-pinned root, verify every required object, and compare recovered state and outcomes. | Interrupt publication or corrupt/remove a required chunk; recover the acknowledged state with valid proof or fail verification without choosing a convenient bucket root. |
| `schema-evolution` | Run a version-one app, populate state, introduce a compatible migration and a new workflow definition, and inspect retained old runs. | Stable identity/codec fixtures, declared schema ranges, ordered migration digests, and versioned workflow retention. | Start an unsupported older binary against migrated state or omit a live definition; reject incompatibility. Demonstrate rollback only where its compatibility contract permits it. |
| `provider-doctor` | Configure an object provider, probe a fresh private prefix, publish a small SQL workload, restart, and verify it; application-owned provider and scope configuration. | Readiness based on conditional-create/CAS/range-read evidence, preserved source errors, and cleanup limited to its private probe scope. | Use a local provider shim that rejects a required capability or range contract; refuse readiness and identify the failed check. |
| `load-isolation` | Drive hot and cold tenants, bound writer/readers and retained bytes, shed excess load, then drain; SQL entity Cells and explicit host budgets. | Capacity admission, bounded queues and telemetry labels, application load scheduling, and measurable resource release. | Saturate memory or admission and stop during load; reject excess work predictably and release leases, tasks, readers, slots, and SQLite handles. |

`fleet-service` and the follower mode of `recovery-inspector` need real process
and peer wiring. In-process loopback is useful for a unit scenario but is not
completion evidence for those journeys. `load-isolation` must measure the
implemented admission behavior; tenant fairness is an application policy to
demonstrate, not an assumed framework guarantee.

## Capability coverage

The catalog is comprehensive over the current public primitives and major
integration contracts. It is intentionally finite: additional domain variants
should become scenarios in an existing app unless they teach a new boundary.

| Capability or contract | First focused application | Composition or operational proof |
| --- | --- | --- |
| SQL and custom typed commands/queries | `taskboard` | `quotas`, `reservations`, `project-tracker` |
| Stable entity partitions and typed clients | `entity-registry` | `fleet-service`, `schema-evolution` |
| KV atomic checks, versions, expiry, listing | `settings` | `tenant-workspace` |
| Blob multipart, ETags, verified ranges | `file-vault` | `media-pipeline`, `report-export` |
| Safe Blob part reclamation | `artifact-collector` | Shared-reference and incomplete-inventory scenarios |
| Queue claims, validation, retry, controls, dead letters | `work-queue` | `telemetry-ingest` |
| Cron intervals, controls, bounded catch-up | `interval-scheduler` | `endpoint-monitor` |
| Workflow signals, timers, pause/resume, cancel/restart | `approvals` | `support-desk`, `release-pipeline` |
| Activity leases, retry, external outcome reconciliation | `media-pipeline` | `webhook-delivery`, `provisioning` |
| Effects, inboxes, ambiguous result resolution | `interval-scheduler` | `checkout`, `project-tracker`, `usage-ledger` |
| Request identity, durable rejection, pending resolution | `taskboard` | `quotas`, `recovery-inspector` |
| Owner ordering and receipts scoped to one Cell | `taskboard` | `replica-analytics`, `project-tracker` |
| Exact-root recovery and object/follower response proofs | `recovery-inspector` | `fleet-service` |
| Public authorization and tenant scope | `tenant-workspace` | Every public HTTP app |
| Enrollment, signed peers, pinned mTLS, fencing | `fleet-service` | `provider-doctor`, `recovery-inspector` |
| Readiness, drain, cancellation, and resource release | `fleet-service` | Every service; `load-isolation` under pressure |
| Provider construction and capability probes | `provider-doctor` | Every persistent service profile |
| Migrations, descriptor/codec contracts, pinned definitions | `schema-evolution` | `release-pipeline` |
| Cross-Cell projections and compensation | `project-tracker`, `checkout` | `telemetry-ingest`, `usage-ledger` |

## What makes an application complete

Every app must satisfy the same delivery contract. Its README is a short guide
to the executable implementation, including the topology and expected results.

| Requirement | Completion evidence |
| --- | --- |
| One command to run | A documented command from the cookbook workspace builds and runs a scenario with no cloud account, prints meaningful results, and returns a nonzero exit code when assertions fail. |
| A usable application | Domain operations, validation, typed client, seeded sample data, bounded reads, and explicit error behavior; no generic SQL or raw primitive public endpoint. |
| Persistent local mode | Stable application/tenant identities, managed SQLite and a restart-capable object store; a clean restart preserves data. A temporary smoke mode may exist alongside it. |
| Compiled compatibility contracts | Canonical entity keys, fixed namespaces and shard counts, checked migrations, stable operation/codec IDs, fixtures, and retained live workflow definitions. |
| Explicit transaction boundaries | A topology diagram and an explanation of local invariants, asynchronous delivery, projection progress, and compensation. |
| Safe retries | Caller retains prepared mutation evidence; unknown outcomes are resolved, durable rejection stays distinguishable, and external side effects use a documented idempotency/reconciliation strategy. |
| Owned background work | Install, supervise, and drain every maintenance, Activity, Effect, and consumer task the app requires; demonstrate that work progresses without manual one-shot ticks. |
| Service lifecycle | Probe required storage capabilities, enroll and renew a serving lease, install required facilities, open readiness, then stop admission and drain before withdrawal. |
| Bounded execution | Declare payload/result/Cell limits and process budgets; paginate reads, bound concurrency and catch-up, and preserve underlying errors. |
| Observable behavior | Structured diagnostics containing request IDs, targets, receipts, attempts, and progress where relevant; aggregate metrics avoid unbounded Cell or tenant labels. |
| Repeatable verification | Run the happy journey, clean restart, the row's failure scenario, and relevant invariant checks using public behavior; record source revision, profile, and outcomes. |
| Reusable code | Domain code can be called through the app library without starting a listener; infrastructure adapters do not change domain identities or bypass the durability path. |
| Controlled cleanup | App-owned state directory and storage scope, explicit reset command, and no collection of unrelated objects. |

The cookbook's local runtime can reuse provider, catalog, enrollment, and peer
assembly across apps. It must use the same lease-requiring service path as a
real embedding application; the unleased maintenance builder is reserved for
bounded offline work such as collection. A local filesystem object provider
or a local S3-compatible process can supply persistent objects, but it must pass
the relevant capability probe before serving.

Include durable local receivers/simulators with external-work apps, so an app
can demonstrate uncertain replies and idempotency without credentials. The
first protocol should be concrete and documented; avoid configuration matrices
and adapters that no cookbook journey exercises.

## Implementation order

Build complete vertical slices before multiplying crates. Each wave depends
on shared infrastructure proven by the previous wave. This ordering is a
recommendation, not a claim that any proposed app already runs.

| Wave | Applications | Exit criterion |
| --- | --- | --- |
| 1 | `taskboard`, `settings`, `file-vault`, `work-queue`, `interval-scheduler`, `approvals` | All eight primitives appear in complete local applications; persistent restart, supervised work, and the per-app failures pass. Include one concrete Activity in `approvals`. |
| 2 | `tenant-workspace`, `entity-registry`, `quotas`, `reservations`, `webhook-delivery`, `media-pipeline`, `report-export`, `endpoint-monitor` | Domain authorization, entity routing, local contention, deadlines, and uncertain external outcomes are reusable and tested. |
| 3 | `checkout`, `provisioning`, `release-pipeline`, `project-tracker`, `telemetry-ingest`, `support-desk`, `usage-ledger` | Cross-Cell reconciliation, compensation, artifact links, and definition retention work under interruption. |
| 4 | `provider-doctor`, `fleet-service`, `replica-analytics`, `recovery-inspector`, `schema-evolution`, `load-isolation`, `artifact-collector` | Independent-process recovery and lifecycle evidence exists; operational writes and deletion require their full documented preconditions. |

Build the shared single-node service assembly with the first application,
including a real lease, persistent storage, readiness, and shutdown. Add the
process/peer assembly when implementing fleet behavior. This avoids making
every domain author reconstruct infrastructure while keeping the framework's
ownership boundaries visible.

## Verification and existing sources

Use focused domain/integration checks during implementation. Run broad suites
and process faults in CI or an isolated verification snapshot, with a separate
target directory under the mounted Workspace build volume when available.
Add cookbook-specific check and scenario jobs without weakening the existing
framework checks or qualification profiles. The separate workspace needs its
own dependency-boundary and module-layout checks; the current framework scripts
do not qualify future cookbook packages.

Do not label an app a production reference because its demo passes. Distinguish
implemented journeys, local restart/fault evidence, independent-process proof,
and provider/scale/upgrade qualification. Cloud variants require their
documented credentials and environment. Reuse existing qualification contracts
and preserve their expected evidence.

| Existing source | How it constrains the cookbook |
| --- | --- |
| [Framework integration](framework.md) | Applications own ingress, authorization, credentials, supervisors, readiness, and drain. |
| [API guide](api.md) | Typed authoring, prepared commands, outcome resolution, Cell-scoped receipts, and explicit read policies. |
| [Topology](../crates/cellule-app/docs/topology.md) | Stable fixed-shard/entity identities and compatibility-sensitive descriptors. |
| [Primitive contracts](../crates/cellule-runtime/docs/primitives.md) | Current limits, non-FIFO Queue, fixed-interval Cron, workflow definition retention, and Blob collection prerequisites. |
| [Host lifecycle](../crates/cellule-host/docs/lifecycle.md) | Owned components and drain while lease maintenance remains active. |
| [Read replicas](../crates/cellule-host/docs/read-replicas.md) | Reader admission, verified refresh, separate budgets, and failure without automatic owner fallback. |
| [Peer HTTP](../crates/cellule-peer-http/README.md) | Pinned mTLS transport, signed requests, and application-owned receivers and scope policy. |
| [Provider setup](../crates/cellule-store/docs/providers.md) | Application configuration and conditional-write/range-read probes. |
| [Process host fixture](../crates/cellule-app/tests/process_node.rs) | Existing enrollment, renewal, host, reader, and follower wiring to study; tests are not application libraries to import. |
| [Qualification](../crates/cellule-runtime/qualification/README.md) | Required evidence for provider, scale, fault, and compatibility claims. |
| [Roadmap](roadmap.md) | Application patterns must not depend on proposed Timer, Projection, or hosted consumer APIs. |
