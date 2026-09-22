# cellule-store

Owns provider-neutral object-store transport: bounded reads, conditional and immutable writes, retries, error classification, multipart behavior, and observation. It does not select Cell owners or authoritative roots. Preserve provider conflict and source-error semantics; never turn a failed compare-and-swap into a retry that appears successful.

`src/test_support/` contains reusable, feature-gated test stores. Keep production APIs free of test fixtures. Read `README.md` and provider contract tests before editing. Run `cargo test -p cellule-store --features test-support --locked` and relevant `cellule-ltx` tests after transport changes.
