# Primitive roadmap: from Cell runtime to application framework

Cellule aims to be what Spring is for Java: a framework that gives a large
distributed application its durable primitives, its scheduling, its delivery
guarantees, and its lifecycle, so the application owner models business logic
instead of rebuilding infrastructure. This page records which contracts the
[celld](https://github.com/denoland/celld) platform gets right, which Cellule
already covers, which it refuses on purpose, and what the next slices are.

| Document intent | Value |
| --- | --- |
| Content type | Target design and gap analysis |
| Audience | Framework and primitive contributors |
| Goal | Keep one record of the primitive surface, the refusals, and the next slices |
| Status | Updated after the Timer primitive (`1a92fb8`) and its example and reference wiring (`5c8bf7f`) |

[Architecture](architecture.md) owns the crate boundaries, [quickstart](quickstart.md)
the runnable path, [runtime design notes](../crates/cellule-runtime/docs/README.md)
the mechanics, and the [synthesis ledger](synthesis.md) the Crab provenance.
This page owns the decisions those documents do not make.

## Where the framework stands

| Layer | Crate | What it owns today |
| --- | --- | --- |
| Contracts | `cellule-types` | Bucket and provider identities shared across storage boundaries |
| Transport | `cellule-store` | Conditional writes, ranged reads, retry classification |
| Durability | `cellule-ltx` | WAL capture, verified LTX roots, exact recovery, sparse reads |
| Execution | `cellule-runtime` | Fencing, authority CAS, SQL execution, primitives, scheduler, peers, publication |
| Application | `cellule-app` | Module registration, topology descriptor, typed author handles |
| Host | `cellule-host` | One-runtime node lifecycle, admission, drain, shutdown |

Primitives with a typed author surface: SQL, KV, Blob, Queue, Cron, Timer,
Workflow, Activities, and Effects. Each one keeps its machinery in the same
actor, transaction, publication, and Tick path. `QUALIFICATION_PRIMITIVES`
still names the mixed-load set the qualification receipts exercise, and Timer
is not in it yet.

What "Rust-native" excludes stays excluded: no JavaScript host, WASM loader,
dynamic library loader, or uploaded codec. Registration is compile-time
inventory, a request either publishes an exact root or proves a follower tail,
and an embedding service owns HTTP, authentication, credentials, and
deployment policy.

## Map the Spring ideas onto Cellule

| Spring idea | Cellule counterpart | Layer |
| --- | --- | --- |
| `@SpringBootApplication` | `CellApplication` plus `ApplicationBuilder::finish` | `cellule-app` |
| Component scan and DI | Explicit `register` and `bind_command` with a canonical descriptor | `cellule-app` and registry |
| `@Transactional` | Command savepoint plus request ledger in one SQLite transaction | `cellule-runtime` |
| Spring Data repositories | `SqlCell`, `KvNamespace`, `BlobNamespace`, `QueueNamespace` | `cellule-runtime` primitives |
| `@Scheduled` and `@Cron` | `CronModule` schedules and `TimerModule` deadlines advanced by the typed Tick | `cellule-runtime` |
| Messaging listeners | Queue consumers over claims, effects, and inbox deduplication | `cellule-runtime` |
| Batch jobs | Workflow runs with retryable activities | `cellule-runtime` |
| MVC controllers | Product transport adapters; never inside the runtime | embedding service |
| Actuator | Node status, Cell metrics, scheduler and lag readouts | `cellule-host` and embedding service |
| Flyway migrations | `MigrationDescriptor` with checked digests | registry |
| Testcontainers | Deterministic test host plus the coordination simulator | `cellule-host` and `cellule-runtime` |

The rule that keeps the mapping honest: durable state is reachable only through
the actor, and it becomes visible only after exact-root publication or a
verified follower proof. A primitive that needs its own scheduler, retry
ledger, or durability proof has failed review.

## Read celld as the reference surface

celld runs the Cloudflare programmatic platform on an operator's own machines,
and every primitive there is a Cell. The column that matters is what Cellule
took, adapted, or refused.

| celld primitive | Contract essentials | Cellule position |
| --- | --- | --- |
| Durable Objects | One actor, one writer, ownership by conditional bucket write, fencing epoch in the key, output gate before a response reveals a write, per-object alarms | Adopted except the alarm; the per-Cell alarm question is open below |
| KV | Namespace is one Cell, larger values in the bucket, byte-ordered `list()` with a resumable cursor, expiry filtered on read and swept later | Adopted with versions, atomic check-and-set batches, and a bounded Tick cleanup |
| D1 | Database is one Cell, `batch()` and `exec()` are the only transaction brackets, application SQL cannot open transactions | Adopted as bounded `SqlBatch`; migrations are descriptor-pinned instead of a CLI |
| Queues | Queue is one Cell, producer commit before `send()` resolves, batch formation by size or timeout, lease-backed at-least-once delivery, per-message ack and retry, dead-letter queue | Claim, lease, retry, and dead-letter mechanics adopted; hosted consumer is the next gap |
| Workflows | Instance is one Cell, step ledger keyed by name and occurrence, sleeps and `waitForEvent` commit their deadline, a waiting instance holds no isolate | Adopted with deterministic Rust transitions, timers, signals, controls, and activity leases |
| Cron Triggers | One reserved Cell per script, one alarm row for every expression, one occurrence per fleet, a bounded retry and catch-up policy | Adopted as durable schedule rows that emit typed effects; retry and catch-up policy needs a documented answer |
| R2 | Bucket keys live in the fleet store, conditional operations delegated to the store, multipart cannot resume elsewhere | Adopted as Blob with the manifest as the publication boundary, which is the stronger contract |
| Durable Object facets | Child SQLite image inside the root Cell, one replication stream, image copied on change | Refused; composition belongs to typed cross-Cell effects |
| Workers | Stateless handler with bindings, module scope is not durable state | Adopted as statically linked Rust modules with typed clients |

Two engineering disciplines in celld are worth more than any single primitive,
and Cellule already follows them in part:

1. `crates/logic` is a sans-I/O decision core: `on_event` is the only way
   behavior advances, and a deterministic simulator drives the same core as
   production.
2. Assurance is layered: differential conformance, exhaustive model checking
   of the coordination protocol at small size, seeded adversarial simulation,
   and fault injection on a live fleet.

`crates/cellule-runtime/src/coordination.rs` is the same kind of pure kernel,
and `coordination_sim.rs` enumerates its 41-event schedule space. What is
missing is the same treatment for the primitive decision cores: queue leases,
timer firing, cron advancement, and effect delivery are pure functions today,
but no seeded simulator drives them.

## Close the next gaps

Ordered by how much they block an application owner.

### Host queue consumers (delivered)

The Queue primitive is a claim API. celld instead hosts the consumer: the queue
Cell forms a batch by size or by timeout, leases it, calls a registered handler
in a stateless isolate, and settles each message.

`QueueConsumer`, `QueueConsumerSupervisor`, and the batch-forming claim now
provide the runtime half: the batch policy sits next to the module
(`MAX_BATCH_SIZE`, `MAX_BATCH_TIMEOUT_MS`, `RETRY_DELAY_MS`), nothing is leased
before the batch closes, the handler runs outside the SQL worker, and every
message is settled through the ordinary lease commands. `tests/queue_consumer.rs`
covers deferral, a full batch, and a poison batch.

The reference application now runs a native consumer over two ready messages
and asserts the acknowledged count. Still open: a standalone example, and the
embedding service owns the per-queue concurrency bound because one pass claims
one batch.

Evidence: a duplicate delivery after a consumer crash settles once; a poison
message reaches the dead-letter queue at the attempt bound; a slow handler
cannot block a producer.

### Move effect and activity supervision into the host (delivered)

`EffectSupervisor` and `ActivitySupervisor` live in `cellule-runtime`, and the
embedding service or example must drive them. `cellule-host` owns admission
and drain but not supervision, so an embedder that uses the host alone inherits
no delivery loop.

`CellNode::install_delivery` now owns the per-node loop: it scans the configured
catalog shards for Cells whose published `next_due_ms` is due, skips Cells this
node does not serve, and runs the Tick plus one bounded activity, queue
consumer, and effect pass for the registered namespaces, all inside the node
task group so drain cancels and joins it. The service still supplies the
catalog, authority, routable client, signed effect transport, and the blocking
pool, and it still chooses which catalog shards it scans.

Evidence: `crates/cellule-host/tests/delivery.rs` builds a node, installs
delivery, schedules a Timer deadline, and proves the destination SQL Cell is
released with no test-driven tick, claim, or effect delivery.

Still open: rendezvous scanner election is left to the service, and the
delivery loop does not yet bound how many Cells one pass delivers concurrently.

### Define the read-model projection contract

The framework declares a read-model Cell pattern, and nothing projects across
Cells into one. The contract needs the rigor effects already have:

- A projection is a typed consumer of another Cell's effect stream or queue.
- Apply is idempotent against a source Cell sequence or receipt.
- Lag is observable per namespace as a bounded gauge.
- A projection never becomes an authority for the source Cell's invariants.

### Extend the qualification mix to Timer and the queue consumer (delivered)

`QUALIFICATION_PRIMITIVES` drove the mixed-load matrix, the receipt row count,
and the app-side executor. It now names `timer` and `consumer` beside the
original eight primitives; the reference executor schedules and reads back a
deadline for `timer` and runs one native consumer pass for `consumer`, so a
release receipt covers both.

Evidence produced before this change carries the previous eight-primitive mix
and must be regenerated; the receipt validator compares a receipt's counts with
the compiled list.

### Drive the primitive decision cores with a seeded simulator

Take the coordination simulator's approach to Queue lease expiry, Timer due
selection, Cron advancement, and effect retry: a seeded schedule over the pure
decision functions with adversarial waits, repeated delivery, and clock jumps.
This is the highest-leverage assurance work, because those functions are
already pure.

### Make the framework observable and diagnosable

Expose bounded per-namespace metrics (queue depth, workflow status, effect lag,
scheduler pass, takeover time) and a storage probe that proves conditional
writes and ranged reads before a node serves. These use `cellule-host` status
and `cellule-store` CAS semantics rather than new machinery.

## Leave these decisions open

1. **Per-Cell alarm.** Timer is a sharded deadline Cell that delivers an effect.
   A Durable Objects-style alarm row inside an entity Cell would remove the
   effect hop for self-wake. Decide whether the extra surface buys enough.
2. **First supported audience.** Internal embedders, or third-party crates.io
   users? Publication, semantic versioning, and freeze policy depend on it;
   [releasing](releasing.md) currently describes the mechanics only.
3. **Facade crate.** One `cellule` facade over app, host, and runtime, or the
   current crate-per-layer import paths.
4. **Queue handler shape.** A batch-shaped `QueueConsumer` trait, or a
   per-message handler with the runtime owning batching.

## Reject convenient but unsafe shortcuts

- No dynamic code loading to shorten the deployment loop.
- No second scheduler, durability path, or retry ledger per primitive.
- No success response before publication or a verified follower proof.
- No exactly-once claims for effects, activities, or queue delivery.
- No multi-Cell ACID transactions or global read timestamps.
- No per-Cell metric labels or unbounded status payloads.
- No transparent repartitioning of a hot Cell; shard counts are release
  operations with an explicit transform.

## Use this evidence map

| Claim | Source |
| --- | --- |
| Primitive surfaces and limits | `crates/cellule-runtime/src/{sql,kv,blob,queue,cron,timer,workflow,effects}.rs` |
| Primitive contracts | `crates/cellule-runtime/docs/primitives.md` |
| Tick, receipts, drain, and takeover | `crates/cellule-runtime/docs/runtime.md` |
| Author and operator surfaces | `crates/cellule-app/src/lib.rs`, `crates/cellule-host/src/lib.rs` |
| Runnable primitive path | `crates/cellule-app/examples/`, `docs/quickstart.md` |
| All-primitive application | `crates/cellule-app/tests/reference_application.rs` |
| Qualification mix | `crates/cellule-runtime/src/qualification.rs` |
| celld contracts | celld `docs/services/` and `crates/logic` |
