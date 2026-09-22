# cellule-app

`cellule-app` compiles statically linked modules and Cell topology into a deterministic application descriptor. It gives application code typed handles after an embedding service has started and routed the Cells. The crate is not yet published to crates.io; use the public workspace checkout while release qualification is in progress.

## Run the examples

From the workspace root:

```sh
cargo run -p cellule-app --example orders --locked
cargo run -p cellule-app --example carts --locked
cargo run -p cellule-app --example notifications --locked
cargo run -p cellule-app --example fulfillment --locked
cargo test -p cellule-app --test reference_application \
  performance::reference_storefront_smoke --locked -- --exact --nocapture
```

The [orders executable](examples/orders.rs) creates one local SQL Cell, commits an order, and reads at the commit receipt. The [carts executable](examples/carts.rs) creates one KV Cell, conditionally saves a cart, and reads the published value. The [notifications executable](examples/notifications.rs) creates one Queue Cell, sends a notification, validates its claim, acknowledges it, and reads the acknowledged count. The [fulfillment executable](examples/fulfillment.rs) starts a Workflow, executes its native packing Activity, and reads the completed result. The [storefront application](tests/reference_application.rs) registers seven Cells and verifies SQL orders, KV carts, Blob attachments, Queue notifications, Workflow/Activity fulfillment, and Cron/Effect invoices. The [quickstart](../../docs/quickstart.md) maps each action to its call sequence. All five runs use temporary SQLite files and in-memory object storage.

## Author an application

1. Implement `CellModule` for each statically linked module. Its `ModuleDescriptor` declares the stable module name, namespace IDs and roles, schema range, migrations, and operation IDs. Bind each command and query in `register`; the registry checks that the bindings match the descriptor.
2. Implement `CellApplication::register` to register those modules and add one `CellType` for every declared namespace. `ApplicationBuilder::finish` rejects missing, duplicate, or mismatched topology before the service opens readiness.
3. Compile with `CellApplication::compile(BuildDescriptor { source_revision, cargo_lock_digest })`. Use real build evidence for a persistent deployment; the standalone examples use fixed demo revisions because their stores are fresh on every run.
4. Once the embedding service has a routed `CellClient`, construct `ApplicationHandle` with the compiled application, tenant ID, and application ID. Get `sql`, `kv`, `blob`, `queue`, `workflow`, `activities`, `cron`, or `effects` capabilities for the registered modules.

Keep namespace IDs, operation IDs, and schema versions stable for persisted Cells. The registry requires a nonzero source digest and contiguous, digest-verified migrations covering the declared schema range. A descriptor change needs a migration or explicit compatibility proof; changing the build revision or lockfile digest also changes release identity. See the [application source](tests/reference_application.rs) for a multi-module declaration and the [architecture guide](../../docs/architecture.md) for the persistent boundary.

For built-in KV, Blob, Queue, Cron, and Workflow modules, use the exported version-one `*_SCHEMA_SQL` value in the module's `MigrationDescriptor`. The corresponding `install_*_schema` function installs those same bytes at bootstrap. Keep any application table migrations consistent with their bootstrap SQL.

## Invoke and verify

Caller-issued mutations carry a `MutationIdentity` with one request ID and a bounded lifetime. Reuse that identity when retrying the same mutation. A successful typed command returns `Committed<T>` with a receipt after the runtime's durability boundary; pass that receipt to a query when the read must include the commit. A pending mutation can be resolved through `ApplicationHandle::resolve` after an ambiguous outcome.

`ApplicationHandle` checks tenant, application, and module scope before dispatch; typed capability constructors also validate the registered namespace and role. Provider construction, network ingress, authentication, and node lifecycle belong to the embedding service or `cellule-host`; they are outside this crate.

Run `cargo test -p cellule-app --locked` after changing registration or handles. The [performance guide](PERFORMANCE.md) describes the three-runtime and three-process workloads and their measurement limits.
