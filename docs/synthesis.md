# Crab synthesis ledger

Cellule's runtime crates are a synthesis of the Cell and LTX infrastructure in
[Crab](https://github.com/crabbuild/crab). This page records the source, the
crate mapping, the deliberate adaptations, and how to re-sync.

## Source

| Field | Value |
| --- | --- |
| Repository | `crabbuild/crab` |
| Final synced revision | `3ab25264` (`#291 fix(cell-runtime): bind cluster receipt owner losses to one chain`) |
| Previous synced revisions | `792d8182` (`#290`), `a3edf0b1` (`#278`) |
| Earlier bulk-sync base | `c8871a7c` (`#274 test(cell): cover local owner fault boundaries`) |
| Framework-side scenario from the expiry stack | `#289 test(cell): recover expired Blob upload after owner loss` |

Crab developed the Cell runtime as product infrastructure. Cellule carries the
reusable mechanics; product wiring (HTTP, authentication, provider
credentials, deployment policy, release CI) stays with the embedding service.

`3ab25264` is the last revision synthesized from Crab. From the first Cellule
release onward the dependency points the other way: Crab consumes the published
`cellule-*` crates as an external dependency, and Cellule evolves
independently. Changes driven by an embedding product arrive here as regular
Cellule issues and pull requests; there is no scheduled re-sync.

## Crate mapping

| Crab | Cellule |
| --- | --- |
| `crab-cell-runtime` | `cellule-runtime` |
| `crab-cell-app` | `cellule-app` |
| `crab-cell-host` | `cellule-host` |
| `crab-ltx` | `cellule-ltx` |
| `crab-storage` | `cellule-store` |
| `crab-types` | `cellule-types` |

## Deliberate adaptations

Persisted and exchanged contracts keep the Crab identities they were created
with:

- `crab.*.v1` hash domains, the CRB1 bundle footer's `repository` key, and the
  LTX and Cell object layouts are unchanged.
- The Blob part kind `blob-parts` moved under Cellule's canonical global
  prefix: `.cellule/blob-parts/<two hex digits>/<digest>`, or the scoped
  `global_prefix` of a path-limited store view.
- The application descriptor magic is `cellule.application.v1`, the release
  descriptor runtime name is `cellule`, and the generic catalog role is
  `application`.

Naming and ownership adapt to the framework:

- Crate and identifier names use `cellule*` (`cellule_runtime`,
  `cellule_store`, `cellule_ltx`, `cellule_app`, `cellule_host`).
- The test-only filesystem CAS store moved from
  `crab-cell-runtime/src/process_store.rs` to
  `cellule-store`'s `test_support` module behind the `test-support` feature.
- Crate READMEs, agent guides, examples, and the runtime design notes under
  `crates/cellule-runtime/docs/` are written for the framework; references to
  the product are phrased as "the embedding service".

Cellule also carries hardening that Crab has not adopted yet: ambiguous
peer-resolution and migration replies map to unknown outcomes instead of
guessed state, the host bounds its shutdown-lock wait and resumes a timed-out
runtime drain, and workflow activity events are typed for author handlers.

## Re-syncing (historical)

This procedure applied while Crab was the upstream. Keep it for reference when
auditing the extraction, or for a deliberate forward-port before the first
release; do not run it once Crab depends on published Cellule crates.

1. Fetch `crabbuild/crab` and identify the crab revision after the last synced
   one.
2. For every file in the mapped crates, run a three-way merge with
   `base = crab@last-synced`, `ours = Cellule`, `theirs = crab@new`, rewriting
   crate, path, and identifier names as in the mapping tables above. Keep the
   persisted identities listed under deliberate adaptations.
3. Add the framework-side equivalents of new product qualification scenarios;
   product harness tests in `crab-http-server` have no direct counterpart.
4. Update the source table on this page.
5. Verify: `cargo fmt --all --check`,
   `cargo check --workspace --all-targets --locked`,
   `cargo test --workspace --locked`,
   `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`,
   `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`,
   `python3 scripts/check-boundaries.py`,
   `node crates/cellule-runtime/docs/validate.mjs`, and the TLC model checks.
