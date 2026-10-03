# Workspace reference

Use this page to find the owner of a contract, its runnable example, and its
verification route. Start with the [README](../README.md) for the application
story or the [quickstart](quickstart.md) to run it locally.

## Crate map

Dependencies point downward through the main stack. `cellule-host` also uses
`cellule-runtime` directly; the optional peer adapter depends on runtime
contracts and stays outside the storage layers.
The optional Axum adapter uses application and runtime contracts.

```text
cellule-host → cellule-app → cellule-runtime → cellule-ltx → cellule-store → cellule-types
                      cellule-peer-http → cellule-runtime
                      cellule-axum → cellule-app + cellule-runtime
```

| Crate | Owns | Read next |
| --- | --- | --- |
| [cellule-types](../crates/cellule-types/README.md) | Shared provider and bucket identities. | Its public types and tests. |
| [cellule-store](../crates/cellule-store/README.md) | Provider-neutral object operations, conditional writes, retries, and error classification. | [Provider integration](../crates/cellule-store/docs/providers.md) |
| [cellule-ltx](../crates/cellule-ltx/README.md) | Managed SQLite WAL capture, LTX objects, roots, and verified recovery. | [Upstream provenance](../crates/cellule-ltx/UPSTREAM.md) |
| [cellule-runtime](../crates/cellule-runtime/README.md) | Cell actor, fencing, authority, request outcomes, receipts, and primitive execution. | [Runtime notes](../crates/cellule-runtime/docs/README.md) |
| [cellule-app](../crates/cellule-app/README.md) | Application descriptors, module registration, and typed author handles. | [SQL example](../crates/cellule-app/examples/sql.rs) |
| [cellule-host](../crates/cellule-host/README.md) | Node startup, admission, drain, and shutdown. | [Host lifecycle](../crates/cellule-host/docs/lifecycle.md) |
| [cellule-peer-http](../crates/cellule-peer-http/README.md) | Optional peer transport and pinned mTLS. | Application-owned ingress in the [framework guide](framework.md). |
| [cellule-axum](../crates/cellule-axum/README.md) | Typed Axum extraction and outcome-aware HTTP responses. | [SQL HTTP service](../crates/cellule-axum/examples/sql.rs) |

## Find an example or test

| Need | Source |
| --- | --- |
| Compile two Cell types; write KV and claim Queue work | [Basic example](../crates/cellule-app/examples/basic.rs) |
| Commit SQL, read at a receipt, drain | [SQL example](../crates/cellule-app/examples/sql.rs) |
| Serve SQL operations through Axum | [Axum orders service](../crates/cellule-axum/examples/sql.rs) |
| Stage and read Blob content | [Blob example](../crates/cellule-app/examples/blob.rs) |
| Run a Workflow and supervised Activity | [Workflow example](../crates/cellule-app/examples/workflow.rs) |
| Fire a Cron occurrence and deliver its Effect | [Schedules example](../crates/cellule-app/examples/schedules.rs) |
| Exercise public primitives and recovery | [Application integration suite](../crates/cellule-app/tests/integration.rs) and [primitive suite](../crates/cellule-app/tests/primitives.rs) |
| Understand shutdown behavior | [Host lifecycle tests](../crates/cellule-host/tests) |
| Study qualification and measured evidence | [Qualification profiles](../crates/cellule-runtime/qualification/README.md) |
| Plan complete application crates and their acceptance criteria | [Application cookbook catalog](cookbook.md) |

## Verification routes

Use a separate `CARGO_TARGET_DIR` for this checkout under the mounted
`$HOME/Workspace/crabbuild-target` volume when available. Broad suites and
process tests belong in CI or an isolated verification snapshot.

| Scope | Command |
| --- | --- |
| Format | `cargo fmt --all --check` |
| Features and targets | `cargo check --workspace --all-targets --all-features --locked` |
| Tests | `cargo test --workspace --all-features --locked` |
| Local LTX | `cargo test -p cellule-ltx --no-default-features --locked` |
| Lints | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` |
| API docs | `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked` |
| Boundaries and layout | `python3 scripts/check-boundaries.py` and `python3 scripts/check-module-layout.py` |
| Document syntax and links | `python3 scripts/check-doc-rust-fences.py` and `python3 scripts/check-doc-links.py` |
| SQL and peer contracts | `node crates/cellule-runtime/docs/validate.mjs` |

The [release guide](releasing.md) adds packaging and qualification checks.
The [architecture guide](architecture.md) describes durable publication and
recovery; the [API guide](api.md) describes author-visible choices.
