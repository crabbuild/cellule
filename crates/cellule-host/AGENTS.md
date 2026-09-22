# cellule-host

Owns exactly one runtime, node admission, facility/task lifecycle, drain, and shutdown for a compiled application. The embedding service supplies provider adapters, peer transport, authorization, and deployment policy. Keep cancellation and resource-release ordering explicit; no background task may outlive host shutdown unnoticed.

Read `src/lib.rs` and host tests before changing lifecycle APIs. Run `cargo test -p cellule-host --locked` and a reference application smoke when wiring changes.
