# Integration recipes

The adapter uses existing application capabilities. Public routes, user
authentication, tenant/resource authorization, provider setup and deployment
remain application responsibilities.
The [crate guide](../README.md) covers the public API and wire envelopes.
Signed node transport uses the separate
[`cellule-peer-http` adapter](../../cellule-peer-http/README.md).

| Need | Route |
| --- | --- |
| Fixed tenant with application-written handlers | [`sql` example](../examples/sql.rs) |
| Authorized request context, typed registration and OpenAPI | [`typed-api-service` example](../examples/typed-api-service/main.rs) |
| Native application builder, tenant scopes, leased host startup and shutdown | [Standalone application service](../examples/application-builder-service/README.md) |
| Atomic evidence storage | [Example journal](../examples/typed-api-service/recovery/mod.rs) |
| Local provider setup and cleanup on failure | [Example support](../examples/typed-api-service/support/mod.rs) |
| Durable runtime snapshot and resolution contracts | [Uncertain command guide](../../../docs/api.md#handle-an-uncertain-command) |

## Request scope in a multi-tenant service

1. Authenticate the session/token using application middleware.
2. Look up its permitted tenant and application. Authorize the route's action
   and resource; derive its Cell target using the compiled topology.
3. Create the corresponding `ApplicationHandle<MyApp>` with a trusted
   `ApplicationBinding<MyApp>::scope(authorized_tenant)`, or select an existing
   scoped handle, and install
   it into request extensions. Keep any authorized actor/resource context
   alongside it; a handle is a capability, not a principal.
4. Extract `RequestCellule<MyApp>` in a manual handler, or implement
   `CellEndpoint<MyApp>` on an application `FromRequestParts` extractor for
   generated handlers. A command-specific extractor can enforce write access;
   a query-specific extractor can enforce read access.

Never construct a handle from an unverified tenant hint. Never install a
global fallback when authorization fails. Body input cannot select a different
Cell target. If inputs contain actor/resource identifiers, the domain operation
or application-written handler must validate/bind them to the authorized
context. Typed registration is suitable when the entire typed operation and
its input are safe to expose to that authorized caller.

The example credential grants one local fixture scope. A real multi-tenant
application replaces that lookup with its identity and permission services;
the adapter does not authenticate credentials. `RequestCellule` refuses a
missing extension with a safe configuration error. Application middleware
emits and documents its own 401/403 responses.

## Custody and recovery

The example journal commits snapshot metadata and the **original encoded
input** in one SQLite transaction before dispatch. Its key binds Cell identity
and mutation identity. Identical evidence is idempotent; conflicting bytes
are refused. Keep the database private and apply application retention policy;
snapshot consistency checks do not authenticate a caller or establish custody.

| Resolution of original evidence | Recovery action |
| --- | --- |
| `Committed` | Decode the stored success/rejection and return its original receipt. |
| `Absent` | Execute the restored original command; runtime checks its expiry and incarnation. |
| `Unknown` | Keep evidence and resolve later; do not infer absence or create a new identity. |
| `Expired` | Stop replay; expiry does not authorize a replacement mutation. |
| Invalid scope, artifact, bytes or incarnation | Stop and investigate; never rewrite the snapshot to pass validation. |

`POST /recovery/{request_id}` in the example is authorized against the same
scope before reading the journal. It restores bytes through
`ApplicationHandle::restore_command`, then resolves before executing. A
production service supervises recovery tasks and retains pending work across
process restarts. This example uses temporary files and in-memory objects;
running it again creates a fresh local environment. It demonstrates custody
and recovery within that environment, not persistent deployment storage.

## Lifecycle

The support module provisions the catalog and fenced owner before runtime
bootstrap. Readiness probes a typed query against the initialized local Cell;
production readiness additionally checks enrollment, provider probes, lease
renewal and any required recovery supervisor. See the
[framework lifecycle guide](../../../docs/framework.md#bring-one-node-to-readiness).

On shutdown, stop ingress and drain accepted HTTP handlers, then await runtime
shutdown before releasing SQLite paths and providers. Setup and listener
failures also go through runtime cleanup. Do not detach recovery/renewal tasks:
the embedding host must cancel and join them on every exit path.

## OpenAPI and SDKs

With `openapi`, existing handlers can use `CellJson<T>` and `MutationBody<T>`
schemas, `HttpError` response descriptions, and `params(MinimumReceipt)` in
Utoipa annotations. Generated routes carry the same schemas automatically.
Merge/nest the returned `OpenApiRouter` and then use `split_for_parts` to expose
the final document. Add application security schemes, 401/403 responses, API
version and server URLs to that document beside the corresponding middleware.

Validate the exported document in your application's CI and generate SDKs
from that artifact. Generated HTTP methods do not supply mutation semantics:
the SDK/service must retain one identity and original bytes, distinguish
`outcome_unknown` from `unavailable`, and resolve ambiguous dispatches. HTTP
503 alone never authorizes replay. Receipts use an unsigned 64-bit JSON integer
(`uint64` schema); TypeScript clients require lossless decoding beyond
`Number.MAX_SAFE_INTEGER`. Preserve all receipt fields when constructing the
`x-cellule-receipt` JSON header. The adapter does not generate SDK packages or
install a recovery endpoint for application-written handlers.
