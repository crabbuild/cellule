# Cellule

Cellule is an embedded Rust framework for distributed applications whose state is partitioned into SQLite-backed Cells. A Cell has one fenced writer, a durable control record, immutable LTX history in object storage, and an exact recovery root. Applications register statically linked modules and invoke typed commands and queries. The embedding service owns network endpoints, authentication, cloud credentials, and deployment policy.

The source repository is public. The crates are currently unpublished while provider and deployment qualification remains open; run the examples from this workspace checkout.

## Crates

| Layer | Crate | Responsibility |
| --- | --- | --- |
| Contracts | [cellule-types](crates/cellule-types/README.md) | Dependency-light provider and bucket identities shared across storage boundaries. |
| Transport | [cellule-store](crates/cellule-store/README.md) | Provider-neutral object-store operations, conditional writes, retries, and error classification. |
| Persistence | [cellule-ltx](crates/cellule-ltx/README.md) | Managed SQLite WAL capture, verified LTX recovery, immutable Cell roots, and sparse reads. Remote replication requires the `replica` feature. |
| Coordination and execution | [cellule-runtime](crates/cellule-runtime/README.md) | Cell identities, owner fencing, authority CAS, SQL execution, durable outcomes, and distributed primitives. |
| Application | [cellule-app](crates/cellule-app/README.md) | Module registration, stable topology, and typed author handles. |
| Host | [cellule-host](crates/cellule-host/README.md) | One-runtime node lifecycle, resource admission, drain, and shutdown. |

The dependencies point downward: `host → app → runtime → ltx → store → types`. `host` also uses runtime directly. No crate depends on Crab, Git, or an HTTP server.

## How a write becomes durable

1. The current owner executes a command through the managed SQLite writer and records its outcome in the same transaction.
2. LTX captures the committed WAL boundary and prepares immutable, verified objects.
3. The runtime conditionally publishes an exact root in the Cell control record. A conflict fences a stale owner.
4. A successful response follows the durable publication or the configured, recoverable follower-log path.

Recovery begins from the authority-pinned root and verifies the referenced data before activating a Cell. A bucket listing never selects authoritative state.

## Develop

Rust 1.97 or newer is required:

```sh
cargo check --workspace --locked
cargo test --workspace --locked
cargo test -p cellule-ltx --features replica --locked
```

Start with the [runnable reference application guide](docs/quickstart.md). It walks through real SQL orders, KV carts, Blob attachments, Queue notifications, Workflow fulfillment, Activity execution, Cron invoice delivery, and durable Effect delivery. The [reference application source](crates/cellule-app/tests/reference_application.rs) is compiled and exercised in CI. See [architecture](docs/architecture.md) for ownership rules, [qualification](crates/cellule-runtime/qualification/README.md) for evidence requirements, and [performance examples](crates/cellule-app/PERFORMANCE.md) for measured local workloads. `cellule-ltx` retains its [upstream attribution](crates/cellule-ltx/UPSTREAM.md) and bundled licenses.

## Integration

Cellule is developed and tested as a separate workspace. An embedding service supplies its own storage provider, application modules, network transport, and authentication. The architecture document defines the crate boundaries; Crab-specific design notes remain with Crab.

## Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, verification, and compatibility rules.
