# cellule-axum

Use typed Cellule capabilities in Axum 0.8 handlers. This optional adapter
provides scoped extractors, typed request/response envelopes, and errors that
retain mutation outcome evidence. Your application owns its router, listener,
authentication, tenant selection, and shutdown.

| API | Purpose |
| --- | --- |
| `Cellule<A>` | Clone an existing `ApplicationHandle<A>` from router state. |
| `RequestCellule<A>` | Extract a handle installed by application authorization middleware. |
| `MutationJson<T>` | Decode a caller-created identity and typed input with safe JSON errors. |
| `ReceiptDto`, `MinimumReceipt` | Share full receipts between replies and bounded query headers. |
| `CellJson<T>` | Return a serializable output and its Cell receipt. |
| `HttpError` | Convert runtime/invocation failures with `?`, preserving the original source. |
| `openapi` feature | Utoipa 6 schemas, error responses and minimum-receipt parameters. |
| `CellApi`, `EndpointSpec` (`openapi`) | Register typed POST handlers and OpenAPI from one declaration. |

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
it through Axum's `Extension<ApplicationHandle<MyApp>>` for `RequestCellule`.
See the [multi-tenant recipe](docs/README.md#request-scope-in-a-multi-tenant-service).
Never treat a tenant
header as authorization. This adapter does not select tenants from requests.

For a serializable command/query output, return `CellJson::from(result)`.
For domain DTOs, use `CellJson::from(result).map(convert)` or `try_map(convert)`.
Both retain the receipt; fallible mapping and JSON serialization failures return
`invalid_published_result` with that receipt and retain the original error.
The response is:

```json
{"output":{"id":42,"total_cents":1999},"receipt":{"cell":"<64 lowercase hex digits>","incarnation":"<32 lowercase hex digits>","commit_sequence":1}}
```

Receipts retain the Cell and incarnation: a sequence number alone is not a
read minimum. `CellJson` supports commands and queries; only a `Committed`
command result proves a newly published mutation. Applications may override
success status with `(StatusCode::CREATED, CellJson::from(result))`.

## Handle failures and retries

`MutationJson<T>` accepts this command body. The caller creates the identity
once and retains all fields unchanged across attempts; runtime preparation
validates its lifetime. Set an application body limit with Axum's
`DefaultBodyLimit`. Unknown envelope/identity fields are refused.

```json
{"identity":{"request_id":"00000000-0000-4000-8000-000000000006","issued_at_ms":1700000000000,"expires_at_ms":1700000060000},"input":{"id":42}}
```

`MinimumReceipt` extracts an optional `x-cellule-receipt` header containing the
JSON receipt object from a previous response. `ReceiptDto::to_header_value`
produces it. Missing headers yield `None`; malformed, duplicate, oversized
(over 256 bytes) and noncanonical identities return 400. Pass the native receipt
to the typed query; the runtime checks its Cell, incarnation and read minimum.
It is an observation constraint, not an authorization credential.

```rust
use cellule_axum::{ReceiptDto, MINIMUM_RECEIPT_HEADER};
use cellule_runtime::{Receipt, identity::{CellId, IncarnationId}};
let receipt = Receipt {
    cell: CellId::from_bytes([1; 32]),
    incarnation: IncarnationId::from_bytes([2; 16]),
    commit_sequence: 7,
};
let mut headers = axum::http::HeaderMap::new();
headers.insert(MINIMUM_RECEIPT_HEADER, ReceiptDto::from(receipt).to_header_value()?);
# Ok::<(), cellule_axum::HttpError>(())
```

Receipt sequences remain unsigned 64-bit JSON integers. TypeScript SDKs must
decode them losslessly beyond `Number.MAX_SAFE_INTEGER`; converting a rounded
number back into a header can change the query's minimum.

`HttpError` emits JSON with `code` and a fixed `message`. It retains the exact
original error through `std::error::Error::source` and `into_source`, including
typed rejected outputs, receipts, and `PendingMutation` evidence. Source
strings, SQL text, provider errors, and local paths stay out of public bodies.
All error replies carry `Cache-Control: no-store`.

| Failure | HTTP | Code and evidence |
| --- | --- | --- |
| Invalid identity or command input | 400 | `invalid_request` |
| Malformed JSON / typed JSON mismatch | 400 / 422 | `invalid_request`, original Axum rejection |
| Body exceeds limit / missing JSON content type | 413 / 415 | `body_too_large` / `unsupported_media_type` |
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

## OpenAPI and routine typed routes

Enable `cellule-axum = { version = "0.1", features = ["openapi"] }`. Existing
handlers can use `CellJson<T>` / `MutationBody<T>` as Utoipa schemas,
`HttpError` in `responses`, and `params(MinimumReceipt)`. Your public DTOs
derive `ToSchema`; native Cellule wire formats remain unchanged.

For routine routes, `CellApi` validates the registered namespace/module/id/codec
and generates both the handler and document. Commands accept `MutationBody`;
queries use POST with typed JSON input and an optional minimum receipt. Both
return 200 with `CellJson`. Keep domain DTO mapping, custom statuses and inputs
that require principal-specific validation in application-written handlers.

```rust,no_run
# #[cfg(feature = "openapi")]
# mod example {
use cellule_app::{CellApplication, CompiledApplication};
use cellule_axum::{CellApi, CommandEndpoint, EndpointSpec, utoipa::ToSchema,
    utoipa_axum::router::OpenApiRouter};
use cellule_runtime::{Command, Error, NamespaceId};
use axum::extract::FromRequestParts;
use serde::{Serialize, de::DeserializeOwned};

fn command_routes<A, C, X, S>(application: &CompiledApplication,
    namespace: NamespaceId) -> Result<OpenApiRouter<S>, Error>
where
    A: CellApplication + 'static,
    C: Command,
    C::Input: DeserializeOwned + ToSchema,
    C::Output: Serialize + ToSchema + Sync,
    S: Clone + Send + Sync + 'static,
    X: CommandEndpoint<A, C> + FromRequestParts<S>,
    X::Rejection: Send,
{
    Ok(CellApi::<A, S>::new(application)?
        .command::<C, X>(namespace, EndpointSpec::new("/orders", "createOrder"))?
        .into_router())
}
# }
```

The application's extractor authenticates and authorizes the operation and
target before decoding the body. Its `CommandEndpoint::retain` hook durably
stores snapshot metadata and original encoded input **before dispatch**.
The adapter never chooses a tenant, mints identities, or retries commands.
Registration refuses duplicate routes/operation IDs, malformed route syntax,
missing path-parameter schemas and mismatched contracts. Every request verifies
the selected handle's compiled artifact and namespace before invocation.

Returned `OpenApiRouter`s support merge/nest and `split_for_parts`. Add your
security schemes, 401/403 responses, API version and server URLs before serving
the document. The [integration example](examples/integration.rs) shows the
concrete application types and SQLite custody implementation.

```sh
cargo run -p cellule-axum --example integration --features openapi --locked
```

It serves `POST /total`, `POST /total/read` (JSON `null` input), authorized
`POST /recovery/{request_id}`, `/scope`, `/ready` and `/openapi.json` on port
3001. Protected routes require `Authorization: Bearer local-orders` in this
local tutorial. Readiness queries the initialized Cell; shutdown drains HTTP
before runtime cleanup. Its files and object storage are temporary. The
[integration recipes](docs/README.md) explain production tenant authorization,
recovery supervision, provider readiness and SDK retry/receipt contracts.

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
cargo test -p cellule-axum --all-targets --all-features --locked
```
