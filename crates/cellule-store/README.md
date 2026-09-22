# cellule-store

`cellule-store` is Cellule's provider-neutral object-store transport. It wraps `object_store` with bounded reads, immutable writes, conditional create and update, classified errors, retries, multipart uploads, and optional request and byte observation. It does not choose a Cell owner or authoritative root.

```rust
use std::sync::Arc;
use bytes::Bytes;
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

# async fn example() -> Result<(), cellule_store::StorageError> {
let store = Store::new(Arc::new(InMemory::new()));
let path = Path::from("cells/example/object");
store.put(&path, Bytes::from_static(b"state")).await?;
let (body, _etag) = store.get_with_etag(&path).await?;
assert_eq!(&body[..], b"state");
# Ok(())
# }
```

The caller supplies a validated object-store prefix. `cellule-ltx` defines the Cell object layout, and `cellule-runtime` decides when a proposed root becomes authoritative. The embedding service resolves credentials and constructs cloud providers. `build_explicit_store` is available when that service wants Cellule's provider builder.

## Guarantees

- Conditional create and update retain provider conflict semantics. A failed comparison is a state conflict, not a successful retry.
- Bounded reads validate size and response framing before exposing data. Range and stream reads preserve provider errors.
- Immutable writes compare content on an existing-object conflict, so an identical retry is safe and differing bytes fail.
- Multipart failures attempt cleanup while retaining the original error. Resumable uploads can use caller-owned journals.
- Read admission and cancellation apply before backend work; observed request and byte counts remain bounded.

`StorageError` retains transport causes where available, and `RetryClass` separates transient, throttled, state-dependent, and fatal failures. See [the crate API](src/lib.rs) and [the workspace architecture](../../docs/architecture.md).
