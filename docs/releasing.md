# Releasing Cellule

The six workspace crates publish to crates.io as `cellule-types`,
`cellule-store`, `cellule-ltx`, `cellule-runtime`, `cellule-app`, and
`cellule-host`. They share one version: an embedding service depends on the
set, and the intra-workspace requirements pin it exactly (`=0.1.0`).

## Before the first release

1. Confirm the crates.io names are still free (they were unclaimed when this
   page was written):

   ```sh
   for crate in cellule-types cellule-store cellule-ltx cellule-runtime cellule-app cellule-host; do
     curl -s -A 'cellule-release-check' "https://crates.io/api/v1/crates/$crate" \
       | grep -q 'does not exist' && echo "$crate: free" || echo "$crate: TAKEN"
   done
   ```

2. Run the full gate from a clean checkout:

   ```sh
   cargo fmt --all --check
   cargo check --workspace --all-targets --locked
   cargo test --workspace --locked
   cargo test -p cellule-ltx --features replica --locked
   cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
   RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
   python3 scripts/check-boundaries.py
   node crates/cellule-runtime/docs/validate.mjs
   ```

3. Package every crate. `cargo package` for a single crate cannot resolve the
   unpublished workspace dependencies, so package the workspace as a set:

   ```sh
   cargo package --workspace --locked
   ```

## Publishing order

Publish in dependency order so each crate can resolve the ones below it:

```sh
cargo publish -p cellule-types
cargo publish -p cellule-store
cargo publish -p cellule-ltx
cargo publish -p cellule-runtime
cargo publish -p cellule-app
cargo publish -p cellule-host
```

Wait for each crate to appear in the registry index before publishing the next
one. `cargo publish --dry-run` verifies a crate that has no unpublished
dependencies, which is why the order matters. Publishing requires a crates.io
token; nothing in this repository uploads on its own.

## After publishing

- Tag the release and record the exact revisions in the release notes.
- Switch Crab from its in-tree `crab-cell-*` crates to the published
  `cellule-*` dependencies. From that point Cellule is the upstream, and
  `docs/synthesis.md` is historical.
- Keep the bundled attributions with the published crates: `cellule-ltx`
  ships `LICENSE` and `LICENSE.pierrec-lz4`, and its `UPSTREAM.md` must keep
  naming the Celld, rustyriver, Litestream, and LTX sources.
