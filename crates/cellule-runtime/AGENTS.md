# cellule-runtime

Owns Cell identity, authority transitions, actor execution, durable outcomes, primitives, publication, recovery, and qualification contracts. Product ingress, authentication, cloud credentials, and deployment policy stay outside. `src/coordination.rs` is a pure decision kernel; adapters in actors perform I/O after a decision.

Protect fencing, exact-root recovery, staged publication ordering, complete Blob references, and drain/release lifecycle. Test the public path through `cellule-app` and `cellule-host` when changing an exported contract. Read `qualification/README.md` before changing evidence or profiles; never weaken profiles to pass CI. Run `cargo test -p cellule-runtime --locked`, the process-support tests when relevant, and `node docs/validate.mjs` for contract changes.
