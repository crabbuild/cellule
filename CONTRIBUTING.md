# Contributing to Cellule

Cellule is a layered Rust workspace for reusable Cell storage, coordination, and application mechanics. Start with the [architecture](docs/architecture.md), [local quickstart](docs/quickstart.md), and [embedding guide](docs/embedding.md). Read the nearest crate `AGENTS.md` before changing a crate.

## Local setup

Use Rust 1.97 or newer. The design-contract validator also needs Node.js and the `sqlite3` command-line tool. Local examples and workspace tests use temporary files and in-memory object storage; they do not need cloud credentials.

From the workspace root:

```sh
cargo run -p cellule-app --example orders --locked
cargo +1.97.0 check --workspace --all-targets --locked
cargo test --workspace --locked
cargo test -p cellule-ltx --features replica --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
python3 scripts/check-boundaries.py
node crates/cellule-runtime/docs/validate.mjs
```

Use a Cargo target directory outside the checkout if local disk space is limited. The [reference application smoke](docs/quickstart.md#run-the-reference-application) exercises SQL, KV, Blob, Queue, Workflow/Activity, and Cron/Effect with visible read-back results.

## Change boundaries

Put provider-neutral transport in `cellule-store`, SQLite/LTX mechanics in `cellule-ltx`, authority and execution in `cellule-runtime`, typed application declarations in `cellule-app`, and node lifecycle in `cellule-host`. Product authentication, HTTP, credentials, and deployment policy belong to the embedding service. Run the boundary checker after dependency changes.

Cell IDs, object paths, LTX data, descriptors, schema versions, and signed peer messages are persisted or exchanged contracts. Before changing one, read its writer, reader, tests, and migration path. Add a runnable example or update the quickstart when author-facing behavior changes. Keep tests focused on observable behavior and failure recovery.

The [qualification guide](crates/cellule-runtime/qualification/README.md) separates local contract checks from provider, multi-process, and production evidence. Do not present a local smoke or synthetic receipt as production qualification. Do not add credentials or generated qualification artifacts to a PR.
