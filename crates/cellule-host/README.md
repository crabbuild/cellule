# cellule-host

`CellNode` owns one runtime, its admission ledger, facilities, and task group.
An application supplies identity, provider, ingress, and authorization.

Use `node.bind_local_application::<A>(layout)` once during composition to get an
`ApplicationBinding<A>` over the node's existing runtime. The supplied layout
determines the installation ID. Keep the factory in trusted service state and
call `binding.scope(tenant)` after authorizing the caller. It creates no runtime,
acquires no Cell, and leaves readiness and drain with the original node.

`bind_application` accepts an already configured client, including an
application-owned remote transport. Both paths reject mismatched application
types and client registries. See the
[complete embedding service](../../examples/application-builder-service/README.md)
for scoped Axum routes and OpenAPI using these bindings.

```text
Starting → Ready → Draining → Closing → Stopped
           │         │          │         │
       lease and     stop      accepted  close log;
       components  admission     work    withdraw
       installed  + producers  drained    session
```

| Guide | Topic |
| --- | --- |
| [Lifecycle](docs/lifecycle.md) | Builder, readiness, drain, and shutdown. |
| [Read replicas](docs/read-replicas.md) | Admission, recruitment, refresh, and eviction. |
| [Crate API](src/lib.rs) | Exported node and facility types. |

```sh
cargo test -p cellule-host --locked
```
