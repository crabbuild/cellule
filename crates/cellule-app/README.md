# cellule-app

Declare a stable Cell topology, compile native Rust modules, and expose typed
application handles. `cellule-host` manages runtime lifecycle; the embedding
service owns network wiring and authorization.

Each SQL Cell owns its SQLite database, request ledger, and capture/recovery
lineage. Cells can share worker threads and resource budgets within a host,
and run on different nodes under separate fenced ownership. The application
builder declares Cell types, partitions, schemas, and operations; the host
handles placement, activation, routing, and recovery.

```text
Module descriptor + explicit CellBinding
                    │
                    ▼
          ApplicationBuilder::module
                    │
                    ▼
           CompiledApplication
                    │
                    ▼
      ApplicationBinding + existing client
                    │
      authorize tenant → scope(tenant)
                    │
                    ▼
          ApplicationHandle → typed operations
```

Register each module and all its namespace bindings together. The module supplies
role, shards, and schema range; you choose stable Cell names, namespace IDs,
partition modes, and limits. The compiler validates the choices before calling
the module's registration hook, then uses the existing canonical descriptor path.

This helper accepts an already declared single-namespace module:

```rust,no_run
use cellule_app::{ApplicationBuilder, CellBinding};
use cellule_runtime::{CellModule, NamespaceId};

fn register_orders<M: CellModule>(
    builder: &mut ApplicationBuilder,
    module: M,
    namespace: NamespaceId,
) -> cellule_runtime::Result<()> {
    builder.module(
        module,
        [CellBinding::entity(namespace, "orders").with_limits(64 << 20, 16 << 20)],
    )
}
```

Use `CellBinding::sharded` for fixed shards, `entity` for hashed entity keys,
or `entity_uuid` for canonical UUID SQL Cells. Their descriptor bytes match the
equivalent explicit `CellType` declarations. Include every namespace in the
module exactly once. Module registration errors abort compilation; typed
registration hooks are not rolled back.

Bind your configured `CellClient`, compiled artifact, and installation ID once
with `ApplicationBinding::<A>::new`. Authorize each tenant before calling
`binding.scope(tenant)`. `cellule-host` provides
`CellNode::bind_local_application` to create this factory from its existing
runtime and a supplied storage layout. The same artifact can feed Axum/OpenAPI;
the [complete service example](../../examples/application-builder-service/README.md)
uses these framework building blocks.

| Guide | Topic |
| --- | --- |
| [API guide](../../docs/api.md) | Compile an application, bind handles, invoke commands, and handle outcomes. |
| [Topology](docs/topology.md) | Stable IDs, shards, entity partitions, and descriptors. |
| [Invocations](docs/invocation.md) | Typed clients, read policy, and receipts. |
| [Examples and tests](docs/examples.md) | Runnable paths and proof levels. |

This complete declaration is compiled as a crate doc test:

```rust
use cellule_app::CellType;
use cellule_runtime::{CatalogRole, NamespaceId};

fn orders_topology() -> cellule_runtime::Result<CellType> {
    CellType::new(
        "orders",
        "orders",
        NamespaceId::from_bytes([1; 16]),
        CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()
}

assert!(orders_topology().is_ok());
```

For SQL Cells addressed directly by canonical 16-byte UUIDs, use
`CellType::entity_uuid`. It has a separate persisted partition version from
the 33-byte hashed entity mode. See [Topology](docs/topology.md).

```sh
cargo run -p cellule-app --example basic --locked
cargo run -p cellule-app --example sql --locked
cargo run -p cellule-app --example blob --locked
cargo run -p cellule-app --example workflow --locked
cargo run -p cellule-app --example schedules --locked
cargo test -p cellule-app --locked
```
