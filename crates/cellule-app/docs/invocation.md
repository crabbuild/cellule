# Typed invocations and receipts

```mermaid
sequenceDiagram
    participant Author
    participant Handle as ApplicationHandle
    participant Runtime
    Author->>Handle: Typed command and stable request ID
    Handle->>Runtime: Resolve target and invoke
    Runtime-->>Handle: Committed output and receipt
    Author->>Handle: Query with minimum receipt
    Handle-->>Author: Observed output at proven position
```

| Operation | Route and evidence |
| --- | --- |
| `ApplicationBinding::new` | Validates a configured client against the author type and compiled registry once during composition. |
| `ApplicationBinding::scope` | Creates a tenant capability from the bound client after application authorization; starts no runtime and opens no Cell. |
| `ApplicationHandle::new` | Checks author type and registry digest before calls. |
| `cell_client!` | Binds declared namespace, operation IDs, and `CellKey` type. |
| Command | Always uses current owner; durable outcome includes a receipt. |
| Default query | `ReadPolicy::CurrentOwner` preserves owner ordering. |
| Replica query | `ReadPolicy::Replica` requires an admitted reader and reports its actual receipt. |
| Resolution, stream, lease validation | Remain owner-ordered. |

A missing or lagging replica returns `ReplicaUnavailable` or `ReplicaBehind`;
the client does not silently fall back to the owner. The host installs read
replicas and the authenticated peer client. Application code sees only typed
capabilities. Run the [SQL example](../examples/sql.rs) for a command and
visible read-back.

Keep one `ApplicationBinding<A>` in service state and call `scope` after verifying
the caller's tenant permission. Each handle retains the bound installation ID,
compiled artifact, read policy, and Blob artifact store. Scoped invocations still
reject targets from another tenant, installation, or module. The factory itself
grants capabilities and must remain in trusted application code.
