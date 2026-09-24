# cellule-app

Owns statically linked module registration, stable application descriptors, topology declarations, and typed author handles. Keep provider construction, network ingress, and node ownership in the embedding service or `cellule-host`. A descriptor digest is a persisted contract: changes need a migration or explicit compatibility proof.

The standalone SQL, KV, Queue, Workflow/Activity, Cron/Effect, and Timer deadline examples and the nine-Cell reference application exercise SQL, KV, Blob, Queue, Workflow, Activity, Cron, Timer, Effect, and Projection primitives. Keep examples compiled and verify their visible outcomes. Read `PERFORMANCE.md` before changing ignored benchmarks; preserve measured-run provenance. Run `cargo test -p cellule-app --locked` after changes.
