# cellule-types

Stable, dependency-light storage identities shared by the Cellule layers. `StorageProviderKind` identifies the provider family; `BucketIdentity` identifies a physical storage destination without depending on `object_store` or a product server.

Changes to identity equality or serialization affect cache and storage routing. See [the API](src/storage.rs), [architecture](../../docs/architecture.md), and `cargo test -p cellule-types --locked`.
