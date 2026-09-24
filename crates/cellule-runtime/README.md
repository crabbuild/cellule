# cellule-runtime

The runtime owns the Cell control record, fenced owner transitions, SQLite actors, durable request outcomes, publication, recovery, and the SQL, KV, Blob, Queue, Queue-consumer, Workflow, Activity, Cron, Timer, Effect, and Projection primitives. The embedding service supplies network endpoints, authentication, object-store credentials, and deployment policy.

A command runs against one Cell database. The actor records its outcome in the transaction, LTX captures and verifies the committed WAL, and authority publication makes the root visible. A stale owner is fenced on a conditional-write conflict. On recovery, the runtime reads the authority-pinned root and verifies its immutable dependencies; bucket listings do not choose state.

Releasing an idle Cell for transfer requires the exact Cell generation and a transfer-specific indexed durable-work inspection. Retained request and inbox outcomes, Blob metadata, Queue producer identities, and future Cron schedules may follow the exact root. Live or due Effects, ready or leased Queue messages, pending Workflow activities and timers, due Cron delivery, and unknown inspection state block movement; the maintenance-release inventory stays conservative.

New Blob part artifacts live at `.cellule/blob-parts/<two hex digits>/<digest>`. A sweep needs a complete cross-Cell live set, quiesced writers, and an age cutoff. The public `BlobArtifactStore` provides bounded uploads, reads, and reachability sweeps.

Start with the [runnable reference application](../../docs/quickstart.md). See [runtime design notes](docs/README.md), [qualification profiles](qualification/README.md), and [architecture](../../docs/architecture.md). Run `cargo test -p cellule-runtime --locked`; process-fault tests also use `--features process-test-support`.

Workflow definitions can call `decode_workflow_activity_event` to distinguish a completed Activity, a failed or expired Activity, and an unrelated event. Tagged malformed events return an error; the [fulfillment example](../cellule-app/examples/fulfillment.rs) shows the full transition and native handler path.

Operations read bounded aggregates rather than primitive rows: `queue_info` for one shard, `workflow_status_counts` and `effect_status_counts` for one Cell, `projection_watermark` for one source Cell, and `CellNode::delivery_stats` for one node. Every decision core — queue and timer, projection, workflow, blob, effects, cron — also carries a seeded adversarial schedule plus an exhaustive three-step sweep with the invariants checked after every step; see the [delivery plan](docs/delivery.md#drive-seeded-primitive-schedules).
