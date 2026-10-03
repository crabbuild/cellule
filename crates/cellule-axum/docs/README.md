# Axum integration guide

The [crate guide](../README.md) covers handlers, router state, receipt JSON,
outcome-aware failures, retries, and shutdown. The
[runnable SQL service](../examples/sql.rs) shows the complete local path.

| Concern | Owner |
| --- | --- |
| Typed capability extraction and response conversion | `cellule-axum` |
| Routes, authentication, tenant selection, and HTTP listener | Embedding application |
| Fencing, durable publication, and mutation resolution | Existing Cellule runtime |
| Drain HTTP requests, then node/runtime work | Embedding application lifecycle |

Signed node-to-node transport uses the separate
[`cellule-peer-http` adapter](../../cellule-peer-http/README.md).
