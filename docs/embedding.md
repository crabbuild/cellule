# Embed Cellule in a service

Cellule is a Rust library, not a server. An embedding service supplies its own
API, authentication, cloud credentials, private peer transport, node directory,
and deployment policy. Start with the [local SQL, KV, Queue, Workflow, and Cron examples](quickstart.md),
which commit and read real Cells without those adapters. This guide
covers the additional ownership needed before a service can accept traffic.

## Assemble one node

1. Compile a [`CellApplication`](../crates/cellule-app/src/lib.rs) with stable
   namespace, operation, and schema IDs. Supply the deployed source revision
   and lockfile digest in `BuildDescriptor`; the examples' fixed revisions are
   only for fresh, disposable stores.
2. Construct an object-store provider and a [`Store`](../crates/cellule-store/src/lib.rs).
   Give each application an explicit storage prefix through
   [`CellStorageLayout`](../crates/cellule-ltx/src/cell_layout.rs).
   Keep credentials in the service. Provision or load the Cell catalog and
   authority before acquiring a Cell; recovery must follow the authority-pinned
   root, not a bucket listing.
3. Build one [`CellNode`](../crates/cellule-host/src/lib.rs) with
   `CellNodeBuilder::new(compiled)`, `with_runtime`, `with_replica_host`, and
   `with_session`. Set resource limits from the node's actual CPU, memory, and
   local disk capacity. Install its task group before the node lease. Register
   provider adapters as owned components or facilities so shutdown retains
   their drain callbacks.
4. Build the service's authenticated ingress and private peer transport. A
   single-process service can route known local handles with
   `CellClient::local_many`; a fleet supplies `PeerRoundTrip`, a signed
   `CellClient::peer`, and an inbound verifier. Bind a typed
   `ApplicationHandle` through the node after routing is ready. Do not expose
   a peer endpoint merely because a socket has bound: serving also requires
   the expected application release and a live owner session. The transport
   adapter must classify a lost reply or post-dispatch failure as
   `PeerTransportUnknown`; an HTTP status such as 503 cannot prove non-delivery.
   A received response permits a safe retry only when its valid peer reply
   explicitly says `NotStarted`.
5. Probe the provider operations the deployment uses, publish the node session
   through the service's authoritative directory, and install the resulting
   [`NodeLeaseGuard`](../crates/cellule-runtime/src/node_lease.rs) with
   `install_node_lease_for_startup`. The guard must come from a successful
   authoritative publish or refresh. Run lease renewal and a self-fencing
   watcher in the node task group. Call `start()` after required components,
   listeners, and supervisors are ready; then expose service readiness only
   while both `CellNode::is_ready()` and the service's lease and release checks
   hold.

The [Crab server integration](https://github.com/crabbuild/crab/pull/277) is a
concrete embedding example: it builds one node, installs the task group and
owned adapters, publishes a session, installs its lease, starts supervisors,
and opens readiness last. Its HTTP, Git, identity, and deployment choices
belong to Crab rather than to Cellule.

Release migration is also a peer operation. `MigrationPeerClient::migrate`
re-describes the Cell when the migration reply is lost, malformed, or cannot
confirm the requested successor. If it still cannot confirm the successor, it
returns `PeerTransportUnknown`; inspect the current Cell description before
deciding whether to retry the same migration plan.

`build_unleased_for_maintenance()` is for private, bounded offline work. It
uses object-only admission and cannot be advertised as a serving node.

## Serve and stop

For a request, authenticate at the service boundary, select the exact tenant,
application, namespace, and partition, then use a typed capability from
`ApplicationHandle`. Reuse the same `MutationIdentity` when retrying an
ambiguous command; resolve a pending mutation instead of inventing a second
identity. Cellule treats a timed-out round trip or an unusable mutation reply
as an unknown outcome; the mutation may still have reached its owner. Use a
commit receipt as a query minimum when the caller needs to observe its write. The
[reference application](quickstart.md#run-the-reference-application)
exercises these calls for SQL, KV, Blob, Queue, Workflow/Activity, and
Cron/Effect.

On shutdown, stop new ingress, cancel the service's supervisors, and call
`CellNode::shutdown_until(deadline)`. A deadline error is not a clean stop. A
runtime drain already started continues, and a later call can wait for its result.
Only report a clean stop after shutdown succeeds. The host releases accepted
work and its registered facilities; the service still owns listener shutdown,
directory withdrawal, credentials, and any external effect destination.

## Deliver due Cells

Durable work does not run itself: a Cell's `next_due_ms` becomes actionable only
when something dispatches its maintenance Tick, and activities, queue
consumers, and source effects need a claim loop outside SQLite. Install
`CellDelivery` once the task group and node lease exist:

```rust,ignore
let config = CellDeliveryConfig::new(tenant, application)
    .with_catalog_shards(my_scanner_shards);
let delivery = CellDelivery::new(
    config,
    CellCatalog::new(layout.clone(), tenant),
    CellAuthority::new(layout.clone()),
    client,
    node.runtime(),
    Arc::clone(application.registry()),
    effect_peer_client,
    Arc::new(BlockingActivityPool::for_system()?),
)?;
node.install_delivery(delivery)?;
```

Each pass scans the configured catalog shards, keeps only Cells whose published
`next_due_ms` is due, skips Cells this node does not serve, and then runs the
Tick plus one bounded activity, queue-consumer, and effect pass for the
registered namespaces. A Cell that advanced between the scan and the dispatch
is skipped and rescanned on the next pass. The service supplies what only it
can: the catalog and authority, a client that routes local and peer calls, the
signed effect transport, and the blocking-activity pool. A multi-node fleet
passes the rendezvous-assigned shards it scans; a single-node deployment scans
all of them. The loop runs inside the node task group, so node drain cancels
and joins it.

## Qualify the deployment

The in-memory examples and local three-process storefront smoke prove
application wiring, not cloud-provider or deployment behavior. Before a
production rollout, run the [qualification profiles](../crates/cellule-runtime/qualification/README.md)
against each selected provider and the intended node topology. Capture real
conditional-write, range-read, multipart, owner-loss, recovery, resource, and
fault observations. Bind protected receipts to the exact source revision,
image, profile, and measured artifacts; verify them with the pinned attestation
key. The embedding service owns the harness, credentials, deployment, and
release gate.
