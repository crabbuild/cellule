# Cookbook node support

This crate assembles application-owned single-node infrastructure through
`CellNode`, `NodeDirectory`, `CellCatalog`, and `CellAuthority`. It probes the
provider before readiness, uses a unique signed boot session with a live
renewed lease, and installs explicit worker, memory, and disk budgets.

| Boundary | Behavior |
| --- | --- |
| Domain policy | Remains in each application's library. |
| Local provider | Pinned RustFS through an explicit S3 configuration; the plain filesystem adapter lacks required ETag updates. |
| Ownership | Catalog publication precedes fenced owner creation. Existing owners require durable takeover proof. |
| Recovery | Restores the exact authority-pinned root into a fresh boot-session directory; local SQLite is working state. |
| Readiness | Requires a successful provider probe, healthy task group, and live lease. |
| Maintenance | Supervises bounded native ticks for due resident Cells; every module must register maintenance. A stale scan hint retries from the tick's published receipt, at most four attempts per Cell per pass, preserving native sequence checks and fairness under concurrent claims. |
| Application workers | `spawn_worker` retains bounded work in the host task group, passes its drain cancellation token, and closes readiness on failure. Drain preserves and reports the originating task error chain. |
| Local peers | `LocalPeer` pins a signed process-local transport and one destination or an explicit bounded roster of up to four; applications supply principals and authorization policy. Dynamic dispatch authenticates and authorizes before acquiring an explicitly matched receiver handle. |
| Callback nodes | `sibling_factory` keeps installation identity and provider, enrolls an independent boot, and requires application-bounded concurrency and explicit receiver drain. Reservations exercises one serialized receiver for prior-event callbacks. |
| Shutdown | Drains accepted work while renewal remains live; withdraws after runtime drain, then removes its session's working files. |
| Drop without drain | Fences local admission; callers must explicitly await shutdown to complete release. |
| Compatibility | Current assembly supports schema version one and rejects changed stored code/schema; migrations require the planned evolution application. |

The local CLI's authorization boundary is the operating-system user and a fixed
application-selected tenant. A network service must supply its own authenticated
ingress and target scope. The development enrollment endpoint is an identity
placeholder, not a network listener or a fleet transport implementation.

Taskboard, Settings, File vault, Work queue, Interval scheduler, and Approvals
integration scenarios exercise this assembly. Tenant workspace also exercises
two verified tenants through authenticated HTTP routes and owned listener drain. A worker-failure scenario verifies that
readiness closes, the source error survives drain, and a successor can acquire
the released ownership and restore acknowledged state. Future apps
should reuse it where the lifecycle contract fits, rather than copy the
framework's test fixtures or introduce another publication path.

Interrupted sessions can leave local evidence behind. A new session uses fresh
files and does not delete another session's directory. After all processes
using the state directory have stopped, an operator may remove these working
files; authoritative objects remain in the provider.
