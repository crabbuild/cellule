# cellule-host

`CellNodeBuilder` binds a compiled application, one runtime, a replica host, and a node session. `CellNode` owns admission, registered facilities, task groups, drain, and shutdown. A server attaches its provider and peer adapters; the host keeps accepted work and resource release under one lifecycle.

Create the node with `CellNodeBuilder::new`, install task and lease ownership before serving, obtain a typed application handle, then call `shutdown` after ingress has stopped. The [host API](src/lib.rs) validates required setup and rejects duplicate or late lifecycle installation. See the [reference application](../../docs/quickstart.md) for typed actions and `cargo test -p cellule-host --locked` for lifecycle behavior.
