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

```sh
cargo test -p cellule-axum --all-targets --locked
```
