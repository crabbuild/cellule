# AGENTS.md

Scoped rules for `crates/cellule-axum/`. Root guidance applies.

- Keep this adapter above the application/runtime layers. Routes, listeners,
  authentication, tenant selection, and provider construction belong to the
  embedding application.
- Extract existing typed capabilities; do not create another runtime or bypass
  target validation, publication, request identity, or receipt checks.
- Preserve invocation errors and their pending/committed evidence. HTTP error
  responses must distinguish an uncertain outcome from a durable rejection.
- Public behavior belongs in integration tests. Runnable examples drain HTTP
  requests before the runtime, on both successful and failed serving paths.

