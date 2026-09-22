# cellule-types

Owns stable provider and bucket identity types shared across Cellule. Keep this crate small and dependency-light. Changing serialized identity or equality semantics affects storage routing and persistent cache identity; inspect every consumer first. Do not add transport, Cell authority, or product policy here.

Read `src/storage.rs` and its tests. Run `cargo test -p cellule-types --locked` after changes.
