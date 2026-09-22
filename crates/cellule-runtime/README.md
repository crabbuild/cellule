# cellule-runtime

The runtime owns the Cell control record, fenced owner transitions, SQLite actors, durable request outcomes, publication, recovery, and the SQL, KV, Blob, Queue, Workflow, Activity, Cron, and Effect primitives. The embedding service supplies network endpoints, authentication, object-store credentials, and deployment policy.

A command runs against one Cell database. The actor records its outcome in the transaction, LTX captures and verifies the committed WAL, and authority publication makes the root visible. A stale owner is fenced on a conditional-write conflict. On recovery, the runtime reads the authority-pinned root and verifies its immutable dependencies; bucket listings do not choose state.

New Blob part artifacts live at `.cellule/blob-parts/<two hex digits>/<digest>`. A sweep needs a complete cross-Cell live set, quiesced writers, and an age cutoff. The public `BlobArtifactStore` provides bounded uploads, reads, and reachability sweeps.

Start with the [runnable reference application](../../docs/quickstart.md). See [qualification profiles](qualification/README.md), [executable contracts](docs/README.md), and [architecture](../../docs/architecture.md). Run `cargo test -p cellule-runtime --locked`; process-fault tests also use `--features process-test-support`.
