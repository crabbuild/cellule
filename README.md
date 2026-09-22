# Cellule

Cellule is an embedded Rust framework for distributed applications whose state is partitioned into SQLite-backed Cells. A Cell has one fenced writer, a durable control record, immutable LTX history in object storage, and an exact recovery root. Applications register statically linked modules and invoke typed commands and queries. The embedding service owns network endpoints, authentication, cloud credentials, and deployment policy.

## Crates

| Layer | Crate | Responsibility |
| --- | --- | --- |
| Contracts | `cellule-types` | Dependency-light provider and bucket identities shared across storage boundaries. |
| Transport | `cellule-store` | Provider-neutral object-store operations, conditional writes, retries, and error classification. |
| Persistence | `cellule-ltx` | Managed SQLite WAL capture, verified LTX recovery, immutable Cell roots, and sparse reads. Remote replication requires the `replica` feature. |
| Coordination and execution | `cellule-runtime` | Cell identities, owner fencing, authority CAS, SQL execution, durable outcomes, and distributed primitives. |
| Application | `cellule-app` | Module registration, stable topology, and typed author handles. |
| Host | `cellule-host` | One-runtime node lifecycle, resource admission, drain, and shutdown. |

The dependencies point downward: `host → app → runtime → ltx → store → types`. `host` also uses runtime directly. No crate depends on Crab, Git, or an HTTP server.

## How a write becomes durable

1. The current owner executes a command through the managed SQLite writer and records its outcome in the same transaction.
2. LTX captures the committed WAL boundary and prepares immutable, verified objects.
3. The runtime conditionally publishes an exact root in the Cell control record. A conflict fences a stale owner.
4. A successful response follows the durable publication or the configured, recoverable follower-log path.

Recovery begins from the authority-pinned root and verifies the referenced data before activating a Cell. A bucket listing never selects authoritative state.

## Develop

Rust 1.97 or newer is required. Set `CARGO_TARGET_DIR` outside the checkout when building:

```sh
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-main" cargo check --workspace --locked
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-main" cargo test --workspace --locked
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-main" cargo test -p cellule-ltx --features replica --locked
```

See [architecture](docs/architecture.md) for ownership rules and [the application reference test](crates/cellule-app/tests/reference_application.rs) for SQL, KV, Blob, Queue, Cron, Workflow, Activity, and Effect registration. `cellule-ltx` retains its [upstream attribution](crates/cellule-ltx/UPSTREAM.md) and bundled licenses.

## Extraction status

This workspace is an independent source extraction. Crab still has its original Cell crates and server integration. Migration of Crab to Cellule, publication of packages, and compatibility qualification across the existing Crab data and peer contracts remain open. The architecture document defines Cellule's crate boundaries; the original product-specific design notes remain in Crab.
