# cellule-app

The application layer compiles statically linked modules and Cell topology into a stable descriptor. Authors declare namespaces, operation IDs, schemas, and `CellType` placement, then use typed `ApplicationHandle` methods. The descriptor digest is checked when a Cell is restored or a release changes.

The [reference application](tests/reference_application.rs) models orders, carts, attachments, notifications, fulfillment, and invoices across SQL, KV, Blob, Queue, Workflow, Activity, Cron, and Effect primitives. [Run the guide](../../docs/quickstart.md) for a complete local action and visible result. [Performance examples](PERFORMANCE.md) add three-runtime and three-process workloads with explicit limits on what they measure.

Run `cargo test -p cellule-app --locked` after changing registration or handles. Provider construction, networking, and node lifecycle belong outside this crate.
