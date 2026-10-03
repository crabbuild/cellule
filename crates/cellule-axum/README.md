# cellule-axum

Use typed Cellule capabilities in Axum 0.8 handlers. This optional adapter
provides a state extractor, JSON outputs with receipts, and errors that retain
mutation outcome evidence. Your application owns its router, listener,
authentication, tenant selection, and shutdown.

| API | Purpose |
| --- | --- |
| `Cellule<A>` | Clone an existing `ApplicationHandle<A>` from router state. |
| `CellJson<T>` | Return a serializable output and its Cell receipt. |
| `HttpError` | Convert runtime/invocation failures with `?`, preserving the original source. |

## Write a handler

Use a tenant-scoped handle already built by `ApplicationHandle::new` or
`CellNode::application_handle`. The same handle can serve several routes.

```rust,no_run
use axum::{Router, routing::get};
use cellule_app::{ApplicationHandle, CellApplication};
use cellule_axum::{Cellule, HttpError};
use cellule_runtime::NamespaceId;

fn routes<A: CellApplication + 'static>(handle: ApplicationHandle<A>) -> Router {
    Router::new().route("/cell", get(cell::<A>)).with_state(handle)
}

async fn cell<A: CellApplication>(app: Cellule<A>) -> Result<String, HttpError> {
    // Substitute a namespace declared by your application.
    let target = app.target_for_scope(NamespaceId::from_bytes([1; 16]), b"orders")?;
    Ok(format!("{:?}", target.cell_id()))
}
```

For a larger state, implement Axum's `FromRef<ServiceState>` for your
`ApplicationHandle<MyApp>`. `Cellule<MyApp>` then extracts that substate and
can precede body extractors such as `Json<Input>`.

The handle is scoped before extraction. Authorize access to that scope in
application middleware. For multiple tenants, an application extractor or
authorization middleware can select a tenant, construct its handle, and pass
it through Axum's `Extension<ApplicationHandle<MyApp>>`. Never treat a tenant
header as authorization. This adapter does not select tenants from requests.

For a serializable command/query output, return `CellJson::from(result)`.
For domain DTOs, construct `CellJson { output: dto, receipt: result.receipt }`.
The response is:

```json
{"output":{"id":42,"total_cents":1999},"receipt":{"cell":"<64 lowercase hex digits>","incarnation":"<32 lowercase hex digits>","commit_sequence":1}}
```

Receipts retain the Cell and incarnation: a sequence number alone is not a
read minimum. `CellJson` supports commands and queries; only a `Committed`
command result proves a newly published mutation. Applications may override
success status with `(StatusCode::CREATED, CellJson::from(result))`.

## Handle failures and retries

`HttpError` emits JSON with `code` and a fixed `message`. It retains the exact
original error through `std::error::Error::source` and `into_source`, including
typed rejected outputs, receipts, and `PendingMutation` evidence. Source
strings, SQL text, provider errors, and local paths stay out of public bodies.
All error replies carry `Cache-Control: no-store`.

| Failure | HTTP | Code and evidence |
| --- | --- | --- |
| Invalid identity or command input | 400 | `invalid_request` |
| Identity reused for different command bytes | 409 | `request_conflict` |
| Durably rejected command | 409 | `command_rejected`, receipt |
| Pending command | 503 | `outcome_unknown`, original request ID |
| Published output cannot be decoded | 500 | `invalid_published_result`, receipt |
| Deadline before submission | 504 | `deadline_exceeded` |
| Capacity, draining, fencing, unavailable replica or peer | 503 | `unavailable` |
| Other runtime failure | 500 | `internal_error` |

Inspect `InvocationError` before conversion when your service needs a domain
rejection body or wants to persist recovery evidence. Keep the original request
identity and exact input for retries. A pending outcome requires resolution;
a durable rejection already has an outcome. The HTTP status alone cannot tell
a client whether a command committed. The adapter never retries commands.

HTTP cancellation and response serialization can occur after a commit. For
operations that must survive handler cancellation, prepare the typed command
and retain its `PendingMutation` before execution in application-owned storage.
Use the [uncertain command guide](../../docs/api.md#handle-an-uncertain-command)
to resolve it. A response serialization error also requires resolution of the
original command; keep wire DTOs simple and serializable.

## Run a complete SQL service

The [orders example](examples/sql.rs) supplies temporary SQLite, in-memory
objects, and a local fenced owner. It binds `127.0.0.1:3000` and provides
`POST /orders` and `GET /orders/{id}`. It drains HTTP handlers before shutting
down the runtime, including setup and serving failures.

```sh
cargo run -p cellule-axum --example sql --locked
```

In another terminal, prepare one logical mutation and retain the resulting
body for retries:

```sh
now_ms=$(python3 -c 'import time; print(time.time_ns() // 1000000)')
body=$(printf '{"request_id":"00000000-0000-4000-8000-000000000006","issued_at_ms":%s,"expires_at_ms":%s,"id":42,"total_cents":1999}' "$now_ms" "$((now_ms + 60000))")
curl -i http://127.0.0.1:3000/orders -H 'content-type: application/json' -d "$body"
curl -i http://127.0.0.1:3000/orders/42
```

The POST returns `201` with the order and its durable receipt, after verifying
the row with a receipt-bound query. Repeating the exact POST within the
identity's validity window returns the recorded outcome. Press Ctrl-C to drain.
Service deployment still supplies providers, credentials, authorization,
readiness, and node enrollment; see the [framework guide](../../docs/framework.md).

## Verify HTTP against RustFS

The same example can use real S3 objects. Set `CELLULE_TEST_ENDPOINT`,
`CELLULE_TEST_BUCKET`, a fresh nonempty `CELLULE_TEST_PREFIX`,
`AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. `AWS_DEFAULT_REGION`
defaults to `us-east-1`; `AWS_SESSION_TOKEN` is optional.
`CELLULE_AXUM_BIND` selects a loopback listener (default `127.0.0.1:3000`).
`CELLULE_AXUM_CELLS` selects 1–16 active SQL Cells, and
`CELLULE_AXUM_WORKERS` selects 1–16 SQL workers; both default to one.
Order IDs route to shard `id mod active_cells` (Euclidean remainder).
The application declares 16 fixed shards and activates the selected prefix
of that topology. Each active Cell has its own database, writer, publication
root, and request ledger; handlers use `CellClient::local_many` through the
same typed application handle and `Cellule` extractor.
The service refuses readiness unless all six storage capability checks pass.

After Ctrl-C drains HTTP and releases the Cell, starting the **same binary**
with the same S3 prefix **and Cell count** restores every authoritative root
into a new temporary SQLite directory, using a fresh fenced session.
Changing the Cell count changes order routing, so use a fresh prefix for
each performance point. An active owner is refused;
this tutorial demonstrates graceful restart, not failed-owner takeover.
Keep the binary unchanged so its application code digest matches the catalog.

The [performance runner](../../scripts/bench-axum-rustfs.py) measures real
HTTP POST and GET requests, using the adapter, application handles, SQL
runtime, LTX publication, and RustFS. It verifies receipt sequences, every
acknowledged row, exact retries, conflicting inputs, expired identities,
released authority, and cold recovery followed by another durable write.
For multiple Cells, it verifies distinct Cell identities and contiguous
sequences separately for every shard, restores every database, and publishes
one new command per recovered writer. A shared request identity across
distinct Cells also verifies their independent request ledgers.
It saves request envelopes, individual timings, responses, service logs,
and aggregate results. An unexpected response fails the run; uncertain
commands are never silently retried with new identities.

Use an isolated source snapshot for these process tests. From the repository
root, build a release binary and start the cookbook's pinned local provider:

```sh
task_dir="$HOME/Workspace/crabbuild-target/cellule-axum-rustfs-$(date +%s)"
mkdir -p "$task_dir/source"
git archive HEAD | tar -x -C "$task_dir/source"
export CARGO_TARGET_DIR="$task_dir/target"
cargo build --release -p cellule-axum --examples --locked --manifest-path "$task_dir/source/Cargo.toml"

export COMPOSE_PROJECT_NAME=cellule-axum-perf
export CELLULE_COOKBOOK_STORAGE_PORT=19751
sh "$task_dir/source/cookbook/scripts/local-storage.sh" up
export CELLULE_TEST_ENDPOINT=http://127.0.0.1:19751
export CELLULE_TEST_BUCKET=cellule-cookbook
export CELLULE_TEST_PREFIX="axum-http-$(date +%s)"
export AWS_ACCESS_KEY_ID=cellule-cookbook
export AWS_SECRET_ACCESS_KEY=cellule-cookbook-local-only
export AWS_DEFAULT_REGION=us-east-1

python3 "$task_dir/source/scripts/bench-axum-rustfs.py" \
  --binary "$CARGO_TARGET_DIR/release/examples/sql" \
  --output "$task_dir/results" \
  --repeats 3 --cells 1 4 8 16 --workers 4 --concurrency 16 \
  --warmup 16 --writes 512 --reads 2048
sh "$task_dir/source/cookbook/scripts/local-storage.sh" down
```

This local Compose project uses disposable example credentials. The runner
needs Python 3.11 or newer and retains its S3 prefixes for inspection. The
`down` command preserves the provider's volume. `--cells` and `--concurrency`
form a matrix; each phase must contain at least one request per active Cell.
The command above holds workers, client concurrency, and total operation
counts constant while varying the number of Cells. Powers of two and the
chosen operation counts distribute work evenly across the Cells.
Measurements describe one service process under closed-loop load on the
current machine; they do not qualify
production capacity, distributed ownership, or fault recovery.

See the [steady-read optimization](performance/2026-10-03-rustfs-steady.md),
[earlier multicell comparison](performance/2026-10-03-rustfs-multicell.md),
and [single-Cell report](performance/2026-10-03-rustfs-http.md) for results
and retained evidence. CI also runs a small 1/4-Cell × 1/4-client
matrix with the same correctness checks, without performance thresholds.

For steady-state reads, build both examples and add the Rust driver:

```sh
python3 "$task_dir/source/scripts/bench-axum-rustfs.py" \
  --binary "$CARGO_TARGET_DIR/release/examples/sql" \
  --read-driver "$CARGO_TARGET_DIR/release/examples/http_load" \
  --output "$task_dir/steady-results" \
  --repeats 3 --cells 1 4 16 --workers 4 --concurrency 16 64 \
  --warmup 16 --writes 32 --reads 64 \
  --read-warmup-seconds 5 --read-seconds 60
```

The provider must remain running and the S3 prefix must be fresh. Each client
visits all acknowledged orders, validates every output and minimum receipt,
and reuses HTTP connections. The bounded histogram records all attempts at
10 µs resolution through one second; an overflow percentile is reported as
unknown, and the exact maximum is retained. Latency includes body decoding
and validation. Payload throughput counts successful JSON response bodies,
excluding HTTP headers and transport overhead. CPU windows include warmup.
Service logs report lifetime mean actor queue, worker round-trip, and primitive
query durations after drain; these include warmup and correctness checks.

Add `--baseline-binary /absolute/path/to/previous/sql` for paired comparisons
with alternating baseline/candidate order and separate fresh prefixes. Keep
the driver, topology, HTTP Tokio thread count, and workload identical; repeat
the matrix with different `--workers` values to vary SQL capacity separately.
`TOKIO_WORKER_THREADS` controls the example's HTTP runtime independently of
SQL workers. On macOS, `--sample` records server stacks for diagnosis; omit
it from final comparisons to avoid profiler overhead.

For a dedicated Linux runner, dispatch the existing capacity workflow in
`axum-reads` mode. This runs the paired HTTP benchmark separately from the
unchanged capacity qualification jobs and retains its evidence:

```sh
gh workflow run write-capacity.yml --ref YOUR_BRANCH \
  -f mode=axum-reads \
  -f baseline_ref=a277e5282badab55ceb58433fdddf0dee4dc8542 \
  -f read_seconds=60
```

```sh
cargo test -p cellule-axum --all-targets --locked
```
