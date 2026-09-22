# cellule-ltx

Owns SQLite WAL capture, checksum-bearing LTX, exact restore, Cell object layout, and optional `replica` mechanics. The embedding runtime owns authority, leases, acknowledgement, and HTTP policy. Every restore must produce byte-identical state or an error; do not infer authority from bucket listings.

Read `README.md`, `UPSTREAM.md`, nearby tests, and the dependency contracts before format or provider changes. Preserve upstream notices. Run `cargo test -p cellule-ltx --features replica --locked` and scoped Clippy after changes.
