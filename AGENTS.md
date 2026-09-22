# Cellule contributor guide

Read the nearest crate `AGENTS.md` before changing that crate. This workspace is a reusable Rust framework; product authentication, HTTP, cloud credentials, and deployment wiring belong to the embedding application.

## Layers

`cellule-types → cellule-store → cellule-ltx → cellule-runtime → cellule-app → cellule-host`. Higher layers may use lower layers; `cellule-host` also uses `cellule-runtime` directly. Keep storage transport separate from authority and application policy. `scripts/check-boundaries.py` checks workspace dependencies and the pure coordination kernel.

## Contracts

- A Cell has one fenced writer. A successful command response follows durable publication or a recoverable follower-log proof.
- Recovery verifies the authority-pinned root and every required chunk; reconstruction is byte-identical or fails.
- Persisted IDs, descriptors, object paths, LTX formats, and signed peer messages are compatibility contracts. Read producers and consumers before changing them.
- Blob part deletion requires a complete cross-Cell reference set, quiesced writes, and a grace boundary.
- Shutdown drains accepted work and releases leases, slots, tasks, and SQLite handles on every exit path.

## Work

Search callers, callees, sibling implementations, tests, and documentation before changing an API. Keep one canonical path; avoid speculative configuration and compatibility shims. Preserve source errors. Do not use `unwrap`, `expect`, or `panic!` outside tests. Keep comments near non-obvious ownership and ordering invariants. Update documentation and runnable examples with behavior changes.

Run `cargo fmt --all`, focused tests, `cargo test --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`, `python3 scripts/check-boundaries.py`, and `node crates/cellule-runtime/docs/validate.mjs` as applicable. Qualification profiles and expected evidence must never be edited merely to silence a failure. Cloud and process fault tests need their documented isolated environment. Build artifacts belong in a target directory unique to the checkout; on CrabBuild workstations use the mounted Workspace volume.
