# Cellule API guide

This guide follows the public Rust API an application uses to declare Cells and
invoke them. Start with the [README examples](../README.md#start-locally)
if you have not run a Cell yet. The executable
[SQL example](../crates/cellule-app/examples/sql.rs) shows the full path,
including catalog provisioning and a local runtime.

## Choose an entry point

| You need to… | Start with | Ownership |
| --- | --- | --- |
| Declare modules and stable topology | [`CellApplication`, `ApplicationBuilder::module`, `CellBinding`](../crates/cellule-app/README.md) | Application author |
| Bind once, then select a Cell and call typed operations | [`ApplicationBinding`, `ApplicationHandle`, `cell_client!`](../crates/cellule-app/README.md) | Application author |
| Use SQL, KV, Blob, Queue, Cron, or Workflow | [Typed primitive capabilities](#primitive-capabilities) | Application author |
| Implement a custom operation | [`CellModule`, `Command`, `Query`, `WireValue`](../crates/cellule-runtime/docs/rust-api.md) | Module author |
| Start and drain a serving node | [`CellNodeBuilder`, `CellNode`](../crates/cellule-host/README.md) | Service |
| Supply storage, credentials, or HTTP | [Framework integration guide](framework.md) | Service |

Cellule does not turn a primitive into a public endpoint. The service
still authenticates requests, chooses tenant and application identities, and
controls providers and network policy.

## 1. Compile a stable application

A `CellModule` declares its schema migrations, namespace, operation IDs, codec
versions, and bounds. In `CellApplication::register`, use
`ApplicationBuilder::module(module, bindings)` to register the module and one
explicit `CellBinding` per namespace together. The descriptor supplies role,
shards, and schema range. Compilation checks that every registry namespace has one
matching topology declaration and emits canonical descriptor bytes and a
digest. It does not provision or start a Cell.

This helper for an already declared single-namespace module is also compiled in the
[`cellule-app` crate README](../crates/cellule-app/README.md):

```rust,no_run
use cellule_app::{ApplicationBuilder, CellBinding};
use cellule_runtime::{CellModule, NamespaceId};

fn register_orders<M: CellModule>(
    builder: &mut ApplicationBuilder,
    module: M,
    namespace: NamespaceId,
) -> cellule_runtime::Result<()> {
    builder.module(
        module,
        [CellBinding::entity(namespace, "orders").with_limits(64 << 20, 16 << 20)],
    )
}
```

Choose stable names and namespace IDs, limits, and one partition scheme:

| Scheme | API | Target rule |
| --- | --- | --- |
| Fixed shards | `CellBinding::sharded(namespace, name)` | `partition_for_scope` hashes scope bytes to one declared shard. |
| Entity Cells | `CellBinding::entity(namespace, name)` | `entity_partition` derives one 33-byte partition from a nonempty canonical key; the namespace declares one shard. |
| UUID SQL Cells | `CellBinding::entity_uuid(namespace, name)` | The partition is the canonical 16-byte UUID; the SQL namespace declares one shard. |

The lower-level `register` and `cell_type` methods accept explicit `CellType`
declarations and emit the same descriptor bytes. With that path,
`with_schema_range` must match the module's schema interval. `with_limits` sets
the per-Cell database and capture ceilings. Namespaces, roles, partition schemes,
shard counts, operation IDs, and descriptor bytes are compatibility contracts.
Changing them can change routing or persisted identity. Read the
[topology guide](../crates/cellule-app/docs/topology.md) before changing a
released descriptor.

The [basic example](../crates/cellule-app/examples/basic.rs) declares KV and
Queue modules, compiles them through `CellApplication::compile(build)`, and
uses both through typed handles. Its `BuildDescriptor` records a source
revision and Cargo lock digest. The separate
[SQL example](../crates/cellule-app/examples/sql.rs) shows a SQL module.

### What a module descriptor freezes

| Declaration | Why it matters |
| --- | --- |
| `NamespaceDescriptor` | Stable namespace ID, role, shard count, effect targets, and optional dead-letter relationship. |
| `MigrationDescriptor` | Ordered schema version, SQL bytes, and digest used during bootstrap and recovery. |
| `OperationDescriptor` | Numeric command/query ID, codec version, schema interval, and input/output bounds. |
| Source and lockfile digests | Bind the compiled registry to the application build evidence. |

`CellModule::register` binds the functions named by that descriptor. The
registry rejects missing or extra bindings. A custom `Command` executes in one
Cell transaction through `CommandContext`; a `Query` receives a read-only
`QueryContext`. Their `WireValue` inputs and outputs use bounded codecs. See
[native Rust authoring](../crates/cellule-runtime/docs/rust-api.md) and the
[SQL module](../crates/cellule-app/examples/sql.rs) for the concrete
registration code.

## 2. Bind a client and select a Cell

Bind the configured client to the compiled application and installation ID once
with `ApplicationBinding::<A>::new`. A local serving node supplies the same
factory with `node.bind_local_application::<A>(layout)`, deriving the installation
ID from the supplied storage layout and using the existing runtime.

```rust,no_run
use cellule_app::{ApplicationBinding, CellApplication};
use cellule_host::CellNode;
use cellule_runtime::ltx::CellStorageLayout;

fn bind_service<A: CellApplication>(
    node: &CellNode,
    layout: CellStorageLayout,
) -> cellule_runtime::Result<ApplicationBinding<A>> {
    node.bind_local_application::<A>(layout)
}
```

Keep this factory in trusted service state. After authenticating and authorizing
the tenant, call `binding.scope(tenant)` to create its `ApplicationHandle<A>`.
The factory validates the author type and client registry once; every handle
still rejects targets outside its tenant, application, namespace, or declared
partition scheme. `ApplicationHandle::new` remains available for directly binding
one tenant through the same validation path. Binding alone does not activate
Cells; the host must provision the catalog and establish an owner before calls.
The [embedding service](../examples/application-builder-service/README.md) uses
this flow with Axum/OpenAPI and ordered shutdown.

Use `target_for_scope(namespace, scope)` to derive a target from the declared
fixed-shard or entity scheme. The local SQL example selects its sole fixed
shard explicitly:

```rust
let target = CellTarget::new(tenant, application_id, ORDERS, &partition_for_shard(0))?;
```

A target carries tenant, application, namespace, and partition identity. Its
Cell ID is stable across owner changes. Receipts refer to a specific Cell and
incarnation; do not use one Cell's receipt as a minimum for another Cell.
If you want named, compile-time checked accessors,
implement `CellKey::canonical_bytes` and declare them with `cell_client!`.
The macro checks its namespace, module, and operation IDs against the compiled
registry before accepting a handle; see the
[entity client test](../crates/cellule-app/tests/entities.rs) for a concrete
example. Key bytes must remain stable across compatible releases.

## 3. Invoke and observe

| Call | Input | Result |
| --- | --- | --- |
| `ApplicationHandle::command::<C>` | Target, `MutationIdentity`, typed input | `Committed<C::Output>` with a receipt after the durability gate. |
| `ApplicationHandle::query::<Q>` | Target, optional minimum `Receipt`, typed input | `Observed<Q::Output>` and the actual observed receipt. |
| `ApplicationHandle::prepare_command::<C>` | Same target, identity, and input as a command | A `PreparedCommand<C>` whose evidence can survive caller cancellation. |
| `ApplicationHandle::resolve` | Original `PendingMutation` evidence | The current owner's request-ledger `Resolution`. |

A `MutationIdentity` contains `request_id`, `issued_at_ms`, and
`expires_at_ms`. Give each logical command a fresh request ID, then keep that
identity unchanged if the same command must be retried or resolved. The
[SQL example](../crates/cellule-app/examples/sql.rs) passes
`Some(committed.receipt)` to `SqlCell::query`, so the read must observe the
published write. A `Receipt` identifies a Cell, owner incarnation, and commit
sequence; a query returns its actual observation position.

### Handle an uncertain command

`InvocationError<T>` preserves what the caller knows:

| Variant | What the caller knows | Next step |
| --- | --- | --- |
| `Rejected(Committed<T>)` | The handler's rejection was durably recorded. | Treat its output and receipt as final. |
| `Pending(PendingMutation)` | The command may have committed. | Resolve the original evidence; do not assign a new request ID. |
| `InvalidPublishedResult { receipt, source }` | Publication happened, but decoding the typed result failed. | Preserve the receipt and source error; investigate the codec or release. |
| `NotStarted(Error)` | This call has no pending mutation evidence. | Handle the source error and decide whether to start a new attempt. |

`resolve` can return `Committed` (success or rejection), `Absent`, `Unknown`,
or `Expired`. For a caller that may be cancelled during dispatch, prepare the
command and retain a clone before `execute()`. Resolve its `evidence()` after
cancellation or a `Pending` result. Retry that **same prepared command** only
when resolution is `Absent` and its identity is still valid. `Unknown`,
`Expired`, and a resolution error do not prove the handler did not run. See
[typed invocations](../crates/cellule-app/docs/invocation.md) and the
[`PreparedCommand` API](../crates/cellule-runtime/src/client/mod.rs).

### Choose read consistency

`ReadPolicy::CurrentOwner` is the default: typed queries run in owner order,
optionally after a minimum receipt. `with_read_policy(ReadPolicy::Replica)`
requests an admitted read replica. It returns `ReplicaUnavailable` or
`ReplicaBehind` when no reader can prove the requested position; it does not
silently switch to the owner. Commands, mutation resolution, state streams,
and primitive lease validation remain owner-ordered. The host must install
read replicas before an application can use this policy.

## Primitive capabilities

`ApplicationHandle` creates capabilities only for compiled modules whose roles
and topology match. Namespace capabilities below use declared fixed shards;
SQL and source effects select an explicit target.

| Method | Capability | Main operations |
| --- | --- | --- |
| `sql::<M>(target)` | `SqlCell<M>` | `batch(identity, SqlBatch)`, `query(minimum, SqlBatch)` |
| `kv::<M>(namespace)` | `KvNamespace<M>` | `atomic`, `get`, `list` on a scope-derived shard |
| `blob::<M>()` | `BlobNamespace<M>` | `prepare_mutation`, `mutate`, `query`, `list_shard` |
| `queue::<M>()` | `QueueNamespace<M>` | `send`, `claim`, `validate_claim`, `ack`, `retry`, `extend`, controls |
| `cron::<M>()` | `CronNamespace<M>` | `mutate`, `get`, `list_shard` |
| `workflow::<M>()` | `WorkflowNamespace<M>` | `start`, `signal`, `cancel`, `pause`, `resume`, `restart`, `state` |
| `activities::<M>()` | `WorkflowActivities<M>` | Capability consumed by `ActivitySupervisor` for external work |
| `effects::<M>(target)` | `EffectSource<M>` | `claim`, `validate`, `status`, `ack`, `retry` on a source Cell |

Blob handles require `with_blob_artifact_store` on the binding or application handle;
the [Blob example](../crates/cellule-app/examples/blob.rs) shows
the complete upload and receipt-bound read. Queue and effect lease validation
use the current owner even when ordinary queries use a replica. Activities and
effects need explicit supervisors; the application controls their lifecycle.
See the [primitive guide](../crates/cellule-runtime/docs/primitives.md) for
behavior and the [application integration suite](../crates/cellule-app/tests/integration.rs)
for exercised SQL, KV, Blob, Queue, Cron, Workflow, Activity, and Effect paths.

### Typical operation sequences

- **SQL:** get `SqlCell<M>` for an explicit target, call `batch` with a fresh
  mutation identity, then call `query(Some(committed.receipt), ...)`. The
  [SQL example](../crates/cellule-app/examples/sql.rs) checks the
  returned row and drains the runtime.
- **KV:** use one scope in `KvAtomicRequest` to combine checks and mutations,
  then `get` or `list` on its derived shard. A returned version can be used in
  the next conditional request. The [basic example](../crates/cellule-app/examples/basic.rs)
  writes a setting and reads at its receipt.
- **Blob:** `Begin`, `PutPart`, then `Complete` with separate mutation
  identities; use `prepare_mutation` to retain exact evidence before dispatch,
  and read the object at the completion receipt. Preparing a part stages its
  immutable bytes but does not publish a manifest reference. The
  [Blob example](../crates/cellule-app/examples/blob.rs) also
  verifies the bytes and content type.
- **Queue:** `send` with a producer identity, `claim` from a chosen shard,
  validate that exact claim on the owner, then `ack`, `retry`, or `extend`
  using its message ID and token. Delivery is at least once; the
  [basic example](../crates/cellule-app/examples/basic.rs) demonstrates the lease path.
- **Cron:** `mutate` a schedule, inspect it with `get`, and let the installed
  maintenance runner perform due ticks. A schedule declaration does not by
  itself start a service scheduler. The [schedules example](../crates/cellule-app/examples/schedules.rs)
  drives one explicit tick and effect delivery cycle.
- **Workflow and Activities:** `start` or `signal` a workflow, read `state`,
  and run the explicitly installed `ActivitySupervisor` for external work.
  The supervisor checks leases and records completions through the Cell. The
  [workflow example](../crates/cellule-app/examples/workflow.rs) runs this path.
- **Effects:** a command emits a source intent through
  `CommandContext::emit_effect`. An explicitly installed supervisor claims,
  validates, and acknowledges source effects; the destination must apply them
  idempotently. The [schedules example](../crates/cellule-app/examples/schedules.rs)
  uses a signed local peer loopback. No cross-Cell SQL transaction is implied.

The [primitive integration scenario](../crates/cellule-app/tests/primitives.rs)
runs all of these paths, removes the original SQLite files, restores from
published roots under a successor owner, and reads and writes again. It uses
an in-memory object store, so its success is local recovery evidence.

## Serving-node boundary

`cellule-host` composes one runtime and its facilities. A service starts with
`CellNodeBuilder::new(compiled_application)`, supplies its runtime, replica host,
and session, installs the required lease and owned components, then calls
`CellNode::start()`. Shutdown must drain accepted work and release the node
session. `build_unleased_for_maintenance()` is an offline maintenance path,
not a serving-node shortcut. Follow the [framework integration](framework.md) and
[host lifecycle](../crates/cellule-host/docs/lifecycle.md) guides for the
complete startup and drain sequence. Provider credentials, HTTP handlers,
authorization, and deployment policy remain in the service.

For exact type signatures and trait bounds, build the crate API documentation:

```sh
cargo doc -p cellule-app -p cellule-runtime -p cellule-host --no-deps --locked
```
