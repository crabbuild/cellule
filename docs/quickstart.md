# Run a Cellule application

This path uses local Cells to exercise all eight author primitives, then runs one
recovery test. Use Rust **1.97 or newer** and run commands from the workspace root. The
examples use temporary SQLite files and in-memory object storage; no cloud
credentials are required. On a workstation with the mounted Workspace volume,
put `CARGO_TARGET_DIR` under `$HOME/Workspace/crabbuild-target` and give each
checkout its own directory.

| Step | Run | What you should observe |
| --- | --- | --- |
| 1 | `basic` | Two Cell types, a receipt-bound KV read, and a claimed and acknowledged Queue job. |
| 2 | `sql` | Order 42 is committed and read back as 1999 cents. |
| 3 | `blob` | A Blob receipt is uploaded and read back. |
| 4 | `workflow` | An Activity completes a durable Workflow run. |
| 5 | `schedules` | A Cron tick emits an Effect; signed local delivery records one reminder. |
| 6 | Focused integration test | All eight primitives survive a local owner recovery. |

## 1. Compile an application and use KV and Queue

```sh
cargo run -p cellule-app --example basic --locked
```

Expected application output includes:

```text
compiled basic-example with 2 cell types, digest Digest(...)
setting theme: dark
queue job: send-email
```

The [source](../crates/cellule-app/examples/basic.rs) declares `Settings` and
`Jobs` modules with stable namespace, role, and operation IDs. `BasicApp::compile`
checks both Cell types against their modules and prints the descriptor digest.
The example provisions a fenced owner for each Cell, bootstraps managed SQLite,
and binds both handles to one `ApplicationHandle<BasicApp>`.

`KvNamespace::atomic` writes a setting in one scope; the returned receipt
gates `get`. `QueueNamespace::send` creates an at-least-once job. The example
claims a lease, validates it on the owner, and acknowledges the message.
Use a new request ID for each logical mutation, and keep it stable if that
mutation needs resolution or a retry.
In a real worker, perform idempotent external work after validating the claim
and before acknowledging it; the local example only demonstrates the lease path.

The descriptor is more than a display name: stable namespace, role, shard
count, migration versions, operation IDs, and source/lockfile digests bind the
binary to its persisted state. Read [topology](../crates/cellule-app/docs/topology.md)
before changing a released declaration.

## 2. Commit and read an order

```sh
cargo run -p cellule-app --example sql --locked
```

Expected application output:

```text
order 42 total: 1999 cents
```

The [runnable source](../crates/cellule-app/examples/sql.rs) follows the
full local path:

1. `Orders` declares a SQL schema and fixed command/query IDs; `OrdersApp`
   compiles the module and one SQL Cell type.
2. The example builds a `CellTarget`, in-memory `Store`, and
   `CellStorageLayout`, then provisions the catalog entry and initial fenced
   owner.
3. `CellRuntime::bootstrap` opens managed SQLite and installs the schema.
   `ApplicationHandle::<OrdersApp>` binds a local `CellClient` to tenant and
   application identity, then returns `SqlCell<Orders>`.
4. `SqlCell::batch` writes the order with a stable `MutationIdentity`. The
   command output carries a receipt after durable publication.
5. `SqlCell::query(Some(committed.receipt), ...)` reads at or beyond that
   position. The example checks that the row contains `1999` and drains the
   runtime even if the operation fails.

```mermaid
flowchart LR
    Register[Register module and Cell type] --> Provision[Provision catalog and owner]
    Provision --> Bootstrap[Bootstrap managed SQLite]
    Bootstrap --> Commit[Commit order]
    Commit --> Read[Query at receipt]
    Read --> Verify[Check row and shut down]
```

The fixed IDs and local endpoint in this example are fixtures. In a service,
use stable application IDs and distinct request IDs for distinct logical
commands. Reuse an identity only to resolve or retry the **same** command.

## 3. Upload and read an attachment

```sh
cargo run -p cellule-app --example blob --locked
```

Expected application output:

```text
attachment stored: receipt for order 42
```

The [Blob source](../crates/cellule-app/examples/blob.rs) provisions a
Blob Cell and adds a `BlobArtifactStore` to its application handle. It sends
`Begin`, `PutPart`, and `Complete` as separately identified mutations. The
completion receipt gates a `BlobQuery::Read`; the example checks the returned
bytes and content type. Staging a part alone does not publish an attachment.

## 4. Complete a Workflow Activity

```sh
cargo run -p cellule-app --example workflow --locked
```

Expected application output:

```text
workflow welcome/42: completed
```

The [Workflow source](../crates/cellule-app/examples/workflow.rs) pins a
definition digest, starts a run, and records an Activity intent with its
`Running` state. An explicitly installed `ActivitySupervisor` claims and
validates the work, executes a local `Echo` handler outside SQLite, and records
the completion. The application queries `Completed` state at that receipt.
External handlers should use their stable activity idempotency key when they
call another service.

## 5. Fire a Cron occurrence and deliver its Effect

```sh
cargo run -p cellule-app --example schedules --locked
```

Expected application output:

```text
schedule reminder: one occurrence delivered
```

The [Schedules source](../crates/cellule-app/examples/schedules.rs) registers a
Cron Cell and SQL receiver. It stores a fixed-interval schedule, reads it at
the returned receipt, and explicitly drives one due maintenance tick. That
tick creates a durable source effect. An `EffectSupervisor` validates the
source lease, delivers a signed command through a local peer loopback, and
acknowledges the result only after the receiver's idempotent inbox commits it.
A SQL query confirms one destination row. The host would own the scheduler,
supervisor, transport, and authorization in a serving application.

These examples cross Cell boundaries through effects, with separate source
and destination transactions. The
[primitive guide](../crates/cellule-runtime/docs/primitives.md) explains the
retry and ownership rules.

<a id="exercise-all-primitives-and-recovery"></a>
## 6. Exercise all primitives and recovery

```sh
cargo test -p cellule-app --test integration \
  primitives::typed_application_executes_every_primitive_through_a_local_router \
  --locked -- --exact --nocapture
```

The [scenario](../crates/cellule-app/tests/primitives.rs) executes SQL, KV,
Blob, Queue, Cron, Workflow, Activities, and Effects through typed handles.
It checks read-back results, removes the original local SQLite files, restores
from published roots under a fenced successor owner, and checks reads and new
writes. Its object store is in memory. This is local application and recovery
evidence, not cloud-provider qualification.

For narrower host behavior, run the named integration groups:

```sh
cargo test -p cellule-app --test integration host --locked
cargo test -p cellule-app --test integration entities --locked
```

These cover signed peer routing, ambiguous outcomes, owner loss, read replicas,
promotion, and entity partitioning. Use an isolated CI or verification snapshot
for broad or process suites as described in [CONTRIBUTING](../CONTRIBUTING.md).

## Continue beyond local fixtures

| Goal | Next guide |
| --- | --- |
| Pick a primitive and handle its result | [API](api.md) and [primitive behavior](../crates/cellule-runtime/docs/primitives.md) |
| Understand ownership and recovery | [Architecture](architecture.md) |
| Add Cellule to a serving application | [Framework integration](framework.md) |
| Run provider and process evidence | [Qualification](../crates/cellule-runtime/qualification/README.md) and [performance scenarios](../crates/cellule-app/PERFORMANCE.md) |

The ignored three-process RustFS smoke requires a disposable bucket, explicit
credentials, and a unique prefix. Its environment and exact selector live in
the [performance guide](../crates/cellule-app/PERFORMANCE.md); it is a separate
qualification step rather than a prerequisite for this quickstart.
