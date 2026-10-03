# Contributing to Cellule

Cellule is a layered Rust workspace for reusable Cell storage, coordination, and application mechanics. Start with the [architecture](docs/architecture.md), [local quickstart](docs/quickstart.md), and [framework integration guide](docs/framework.md). Read the nearest crate `AGENTS.md` before changing a crate.

## Local setup

Use Rust 1.97 or newer. The design-contract validator also needs Node.js and the `sqlite3` command-line tool. Local examples and workspace tests use temporary files and in-memory object storage; they do not need cloud credentials.

From the workspace root:

```sh
cargo run -p cellule-app --example sql --locked
cargo run -p cellule-app --example blob --locked
cargo run -p cellule-app --example basic --locked
cargo run -p cellule-app --example workflow --locked
cargo run -p cellule-app --example schedules --locked
cargo +1.97.0 check --workspace --all-targets --locked
cargo test --workspace --all-features --locked
cargo test -p cellule-ltx --features replica --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
python3 scripts/check-boundaries.py
python3 scripts/check-module-layout.py
python3 scripts/check-doc-rust-fences.py
python3 scripts/check-doc-links.py
node crates/cellule-runtime/docs/validate.mjs
```

On workstations with the mounted Workspace volume, always set `CARGO_TARGET_DIR` beneath the mounted
`$HOME/Workspace/crabbuild-target`, with a unique directory per checkout. Use CI
or a dedicated verification snapshot for broad suites and process tests. The [application integration smoke](docs/quickstart.md#exercise-all-primitives-and-recovery) exercises SQL, KV, Blob, Queue, Workflow/Activity, and Cron/Effect with visible read-back results.

## Website and documentation

The [website guide](apps/web/README.md) documents the Next.js and Fumadocs app.
Use Node.js 22+ and npm 11. Run `npm ci`, `npm run check:web`, and
`npm run build:web` from the repository root. The website browser checks run
against a running server with `npm run test:web`; install Chromium with
`npx playwright install chromium` first.

Edit canonical guides in their current repository paths. Website builds map
those documents into Fumadocs and rewrite local links. Put website-specific
entry pages in `apps/web/content/authored/`; do not edit generated docs.
Keep measured claims tied to the report's revision and environment. CI checks
all internal website documentation links and SVG diagram rendering.

## Change boundaries

Put provider-neutral transport in `cellule-store`, SQLite/LTX mechanics in `cellule-ltx`, authority and execution in `cellule-runtime`, typed application declarations in `cellule-app`, and node lifecycle in `cellule-host`. Product authentication, HTTP, credentials, and deployment policy belong to the application. Run the boundary checker after dependency changes.

Cell IDs, object paths, LTX data, descriptors, schema versions, and signed peer messages are persisted or exchanged contracts. Before changing one, read its writer, reader, tests, and migration path. Add a runnable example or update the quickstart when author-facing behavior changes. Keep tests focused on observable behavior and failure recovery.

The [qualification guide](crates/cellule-runtime/qualification/README.md) separates local contract checks from provider, multi-process, and production evidence. Do not present a local smoke or synthetic receipt as production qualification. Do not add credentials or generated qualification artifacts to a PR.

For SemVer rules, matched workspace versioning, and crates.io publication, see the [release guide](docs/releasing.md).
