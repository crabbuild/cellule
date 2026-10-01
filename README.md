# Cellule

Cellule is an embedded Rust framework for distributed, SQLite-backed **Cells**.
An application defines typed modules and Cell topology; Cellule runs commands
through a fenced owner, publishes durable outcomes, and restores exact state.
The application owns HTTP ingress, authorization, credentials, and deployment.

![Cellule architecture: application ownership, framework components, one durable Cell, and the eight primitives](diagram/cellule-components.svg)

For a visual introduction to a Cell, its components, and the smallest
integration path, see [Cellule at a glance](docs/at-a-glance.md).

## The Cell model

An application compiles **modules** (what a Cell can do) and **Cell types**
(which namespace, role, and partition rule each module uses). At runtime, a
target selects one Cell. Its identity stays stable when a different node takes
ownership:

```text
tenant + application + namespace + partition
                     |
                     v
                stable Cell ID
                     |
                     v
          one fenced writer at a time
                     |
                     v
       managed SQLite: state + request outcomes
```

The fenced owner handles commands for that Cell. Authority records the current
owner and pins the published root used for object-store recovery. LTX captures
SQLite changes into verified, immutable objects. Your service still chooses
tenants, authorizes callers, and supplies storage credentials and endpoints.

| Term | Meaning |
| --- | --- |
| **Module** | Statically linked Rust code that declares schemas and operations. |
| **Cell type** | A stable namespace, role, and partition rule compiled into an application descriptor. |
| **Cell** | One addressed state partition with a single fenced writer. |
| **Target** | Tenant, application, namespace, and partition used to address a Cell. |
| **Request identity** | A stable ID and validity window for one logical mutation and its recorded outcome. |
| **Receipt** | A returned observation position that a later read can require. |

### Pick a Cell boundary

One command changes one Cell, so place state that must commit together behind
one target. A Cell type declares how targets are partitioned:

| Partition rule | Target | When to use it |
| --- | --- | --- |
| Fixed shards | Hash a canonical scope into one of the declared shards. | Scoped collections such as settings or jobs. |
| Entity partitions | Derive a partition from a stable entity key. | One independently owned Cell per entity. |

The SQL lesson below uses one fixed shard. A namespace, role, partition rule,
or shard-count change can alter persisted identity and routing; treat it as a
reviewed application change. The [topology guide](crates/cellule-app/docs/topology.md)
covers the exact rules. Work that spans Cells uses durable Effects and an
idempotent destination inbox.

## Start locally

Use Rust **1.97 or newer**. From the workspace root, run these local examples;
they use temporary SQLite files and in-memory object storage:

```sh
cargo run -p cellule-app --example basic --locked
cargo run -p cellule-app --example sql --locked
cargo run -p cellule-app --example blob --locked
cargo run -p cellule-app --example workflow --locked
cargo run -p cellule-app --example schedules --locked
```

| Example | First thing to notice |
| --- | --- |
| [`basic`](crates/cellule-app/examples/basic.rs) | Two Cell types in one descriptor; a receipt-bound KV read and a leased Queue claim. |
| [`sql`](crates/cellule-app/examples/sql.rs) | A parameterized write, durable receipt, and read of the committed order. |
| [`blob`](crates/cellule-app/examples/blob.rs) | Staged parts become visible only after a Blob reference is completed. |
| [`workflow`](crates/cellule-app/examples/workflow.rs) | A supervised Activity advances a durable Workflow. |
| [`schedules`](crates/cellule-app/examples/schedules.rs) | A Cron tick emits an Effect that reaches an idempotent destination inbox. |

Together these paths exercise all eight primitives through typed handles. No
cloud credentials are needed. Follow the [step-by-step quickstart](docs/quickstart.md)
for expected output and a local recovery test. Each source file starts with a
run command and an ASCII outline of its path.

## Follow one complete application path

The [runnable SQL example](crates/cellule-app/examples/sql.rs) contains
the complete local setup. It declares a SQL module and migration, compiles an
application, provisions a catalog entry and fenced owner, bootstraps a managed
Cell, invokes it through a typed handle, checks the observed row, and drains
the runtime. The snippets below are from that compiled example; keep the full
source open if you are copying them into an application.

The path is a short sequence you can trace in `sql.rs`:

1. **Describe** `Orders`: SQL schema, namespace, and stable command/query IDs.
2. **Compile** `OrdersApp`: bind the module to a `CellType` and freeze the
   descriptor and registry.
3. **Provision** a `CellTarget`, catalog entry, and fenced owner.
4. **Start** managed SQLite and an LTX replica for that Cell.
5. **Commit** one parameterized SQL batch with a stable request identity; get
   its output and receipt after the durability gate.
6. **Observe** with `query(Some(receipt), ...)`; check the committed row.
7. **Drain** the runtime on either success or failure.

```text
describe -> compile -> provision -> start -> commit -> observe -> drain
                                         (request ID)  (receipt)
```

The example assembles a local owner and in-memory object store so that every
step can run on one machine. A serving service assembles its provider, owner
routing, and node lifecycle separately; see the [framework guide](docs/framework.md).

### Declare the application

`Orders` and `ORDERS` are defined in the full source. Their module descriptor
includes the schema migration and operation contracts used here:

```rust
struct OrdersApp;

impl CellApplication for OrdersApp {
    const NAME: &'static str = "orders-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Orders)?;
        builder.cell_type(CellType::new(
            Orders::NAME,
            "orders",
            ORDERS,
            CatalogRole::Sql,
            1,
        )?)?;
        Ok(())
    }
}
```

`CellApplication::compile` checks the module and topology before any Cell
starts. Stable namespace IDs, partition rules, schema versions, and operation
IDs are compatibility contracts; see the [API guide](docs/api.md) before
changing them.

### Commit and verify a receipt-bound read

This function is the example's complete application-level operation. The
caller obtains `SqlCell<Orders>` from `ApplicationHandle::<OrdersApp>` after
bootstrapping the Cell:

```rust
async fn commit_and_read_order(sql: &SqlCell<Orders>) -> ExampleResult<i64> {
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let committed = sql
        .batch(
            MutationIdentity {
                request_id: RequestId::from_bytes([6; 16]),
                issued_at_ms: now_ms,
                expires_at_ms: now_ms + 60_000,
            },
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO orders (id, total_cents) VALUES (?1, ?2)".into(),
                    parameters: vec![
                        SqlValue::Integer(ORDER_ID),
                        SqlValue::Integer(ORDER_TOTAL_CENTS),
                    ],
                }],
            },
        )
        .await?;

    // The receipt requires the query to observe this published command.
    let observed = sql
        .query(
            Some(committed.receipt),
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT total_cents FROM orders WHERE id = ?1".into(),
                    parameters: vec![SqlValue::Integer(ORDER_ID)],
                }],
            },
        )
        .await?;
    let [result] = observed.output.as_slice() else {
        return Err(Error::Control("expected one order query result").into());
    };
    let [row] = result.rows.as_slice() else {
        return Err(Error::Control("expected one order row").into());
    };
    let [SqlValue::Integer(total_cents)] = row.as_slice() else {
        return Err(Error::Control("expected an integer order total").into());
    };
    if *total_cents != ORDER_TOTAL_CENTS {
        return Err(Error::Control("published order total differs").into());
    }
    Ok(*total_cents)
}
```

The request ID stays the same for retries of **this** command; a new logical
command needs a new ID. `batch` returns a `Committed` value only after the
durability gate. Passing `Some(committed.receipt)` to `query` requires the read
to observe that committed position. The function checks the actual row instead
of treating a successful transport call as proof of application behavior.

## Choose a primitive

Every primitive uses the same Cell owner, durable request outcome, publication
gate, and receipt. Choose by the shape of work, then use the typed capability
from `ApplicationHandle`:

| Primitive | Use it for | Entry point | Key rule |
| --- | --- | --- | --- |
| **SQL** | Relational state in one Cell. | `sql::<M>(target)` → `batch`, `query` | Parameterized, bounded statements; one Cell transaction. |
| **KV** | Scoped metadata and conditional updates. | `kv::<M>(namespace)` → `atomic`, `get`, `list` | Checks and mutations share one shard transaction. |
| **Blob** | Multipart content and metadata. | `blob::<M>()` → `mutate`, `query` | Stage parts, publish references, read at a receipt. |
| **Queue** | At-least-once work delivery. | `queue::<M>()` → `send`, `claim`, `ack` | Claims have tokens and leases; validate before external work. |
| **Cron** | Durable recurring schedules. | `cron::<M>()` → `mutate`, `get` | Activation is bounded and deduplicated. |
| **Workflow** | Durable, multi-step decisions. | `workflow::<M>()` → `start`, `signal`, `state` | Decisions and history survive owner changes. |
| **Activities** | External work requested by a workflow. | `activities::<M>()` → `ActivitySupervisor` | Run outside SQLite with explicit supervision and lease checks. |
| **Effects** | Delivery between Cells. | `effects::<M>(target)` → `claim`, `ack` | Source intent is durable; destination applies idempotently. |

Primitives can be combined without treating multiple Cells as one SQL
transaction. Two examples show where background supervisors enter the path:

```text
Recurring delivery:
  Cron Cell -> due tick -> durable Effect -> supervisor -> destination inbox
                                                        -> destination Cell

External workflow step:
  Workflow Cell -> Activity intent -> supervisor -> external work
  Workflow Cell <- recorded completion <- supervisor <- result
```

The service installs and drains those supervisors. The destination must
handle repeated delivery idempotently; a Queue consumer must likewise
validate its lease and make external work safe to repeat.

The [example map](crates/cellule-app/docs/examples.md) explains how each
runnable path works, including the [Blob upload](crates/cellule-app/examples/blob.rs),
[Workflow activity](crates/cellule-app/examples/workflow.rs), and
[Cron effect delivery](crates/cellule-app/examples/schedules.rs). The
[application integration suite](crates/cellule-app/tests/integration.rs)
also exercises all eight primitives and recovery. Read the [primitive guide](crates/cellule-runtime/docs/primitives.md)
and [API guide](docs/api.md#primitive-capabilities) for method details and
failure behavior. Cross-Cell work uses effects and inboxes; it is not one
transaction across Cells.

## Why a successful reply survives recovery

![Durable command sequence: SQLite outcome, verified LTX root, authority CAS, then result and receipt](diagram/durable-command.svg)

A command stores its state change and outcome in one SQLite transaction. On
the object-store path, the owner prepares verified LTX bytes and pins the exact
root through fenced authority compare-and-swap before replying. A configured
follower-log path can acknowledge after recoverable follower proof, with object
publication following. Recovery selects the authority-pinned root and verifies
every required chunk; a bucket listing cannot choose state. If a response is
lost, resolve the original request ID before retrying. See
[architecture](docs/architecture.md) and [runtime execution](crates/cellule-runtime/docs/runtime.md).

```text
Reply received -> use the output and receipt
Reply lost     -> keep the original request identity
               -> resolve its recorded outcome
                  |-- committed: use the recorded output and receipt
                  |-- absent: retry the same prepared command if still valid
                  `-- unknown or expired: investigate; do not invent a new ID
```

An owner-ordered query is the default. A read can require a minimum receipt
from **that same Cell**. An explicitly selected replica must prove that it has
reached the minimum or return a lag/unavailable error; it does not silently
fall back to the owner. See [read policies](docs/api.md#choose-read-consistency)
and [uncertain commands](docs/api.md#handle-an-uncertain-command).

## From the local lesson to a service

The examples supply a temporary database, in-memory objects, and a local
fenced owner. To serve requests across nodes, the application must:

1. Compile the same modules and Cell topology used by its typed clients.
2. Choose and probe an object provider, then enroll a node with a live lease
   and install the facilities it declares as required.
3. Authenticate and authorize public requests before selecting tenant and
   Cell targets; install a peer receiver if remote owner routing is needed.
4. Supervise Activities, Effects, and scheduled maintenance that the service
   uses, and drain accepted work before withdrawing the node session.

The [framework integration guide](docs/framework.md) walks through readiness,
peer boundaries, replicas, and shutdown. The local examples demonstrate API
and durability mechanics; provider and process qualification have separate
[evidence requirements](crates/cellule-runtime/docs/delivery.md).

## Go deeper

| Task | Guide |
| --- | --- |
| Run examples and a recovery test | [Quickstart](docs/quickstart.md) |
| Choose typed methods and handle outcomes | [API](docs/api.md) |
| Understand ownership, storage, and recovery | [Architecture](docs/architecture.md) |
| Embed Cellule in a serving service | [Framework integration](docs/framework.md) |
| Understand Cells and integration options visually | [Cellule at a glance](docs/at-a-glance.md) |
| Find the right crate or test | [Workspace reference](docs/reference.md) |
| Evaluate current support and gaps | [Roadmap](docs/roadmap.md) |
| Qualify and publish a matched crate set | [Release guide](docs/releasing.md) |

The workspace layers are `cellule-types → cellule-store → cellule-ltx →
cellule-runtime → cellule-app → cellule-host`; the optional
[cellule-peer-http](crates/cellule-peer-http/README.md) adapter uses runtime
contracts. [Contributing](CONTRIBUTING.md) lists local checks, and the
[qualification guide](crates/cellule-runtime/docs/delivery.md) separates local
tests from provider and production evidence. The dated
[verification report](docs/verification.md) and
[performance evidence](crates/cellule-app/PERFORMANCE.md) describe their
recorded revisions and environments.

## Website development

The Next.js and Fumadocs website lives in [apps/web](apps/web/README.md).
With Node.js 22+ and npm 11, run `npm ci` and `npm run dev:web` from this
repository root. The site renders the canonical guides, crate references,
and measured evidence with searchable navigation and SVG diagrams.

## License

The Cellule workspace crates are licensed under the [Apache License 2.0](LICENSE).
The adapted `cellule-ltx` source retains [upstream attributions](crates/cellule-ltx/UPSTREAM.md)
and a separate [BSD-3-Clause license](crates/cellule-ltx/LICENSE.pierrec-lz4)
for its included LZ4 block implementation.
