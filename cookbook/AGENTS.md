# Cookbook contributor guide

The root contributor guide applies. These crates are embedding applications;
framework crates must never depend on them. Read the application catalog at
`../docs/cookbook.md` and the nearest framework crate guide before changing a
framework API.

- Implement complete domain journeys with public typed APIs and one Cellule
  durability path. Never import framework test fixtures as application code.
- Application libraries own schemas, canonical keys, codecs, and invariants;
  binaries own ingress, providers, authorization, and deployment configuration.
- Shared support contains exercised node/provider plumbing, not domain policy.
- Preserve source errors, bound admission and output, and drain on every exit.
- Record actual implementation status. Planned applications are not runnable.
- Run format, focused public-behavior tests, and Clippy with warnings denied.
  Process and broad suites run in CI or an isolated source snapshot, with a
  target directory unique to this checkout under the mounted Workspace volume.
- Do not weaken existing framework qualification evidence or checks.
