# Project tracker

Manage issues in independently committed project SQL Cells, publish immutable
native Blob attachments, and browse a tenant dashboard receiving signed Effects.
The reusable library owns keys, schemas, codecs, revisions, reference bindings,
and recovery APIs. The binary owns tenant selection, providers, retained files,
and process lifetime. Local shell access is its authorization boundary.

**Status: implemented and qualified.** Ten public-behavior tests and 40 persistent
process checks verify the domain journey, native reclamation, original outcomes,
interrupted-demo drain, and cold byte restoration. The demo, three retained repeat
demos, and documented launcher also pass against local authoritative storage.

## Run

From the repository root, with Rust 1.97 and Docker Compose:

```sh
sh cookbook/scripts/project-tracker.sh
```

The demo creates or reopens an issue, publishes and verifies its complete Blob,
reconciles its immutable link, closes the issue, and delivers revisioned summaries.
The demo pins its controls to the captured native intent from the older edit.
Two native workers actually deliver an older summary after newer state; the
receiver returns `stale` without regressing. The first delayed reply is lost after
publication and resolved through the native inbox. Repeat demos retain the same
issue and immutable attachment, with new explicitly prepared edit identities.

Create an input file and freeze a request before applying it:

```json
{"tenant":"acme","change":{"project":"roadmap","mutation":{"operation":"create","name":"Roadmap"}}}
```

```sh
sh cookbook/scripts/project-tracker.sh prepare /tmp/project.json /tmp/project.request.json
sh cookbook/scripts/project-tracker.sh apply cookbook/.state/project-tracker /tmp/project.request.json
sh cookbook/scripts/project-tracker.sh get cookbook/.state/project-tracker acme roadmap
```

Other mutations are `rename` with `expected_revision` and `name`, `create_issue`
with `id` and `fields`, and `edit_issue` with `id`, `expected_revision`, and
complete replacement `fields`. Issue fields are `title`, `description`, optional
canonical `assignee`, and `status` (`open` or `closed`). The expected revision
for an issue edit belongs to that issue; a project rename checks the project
revision. Closure preserves the issue ID and every attachment reference.

Publish and link an attachment as separate, inspectable steps:

```sh
sh cookbook/scripts/project-tracker.sh prepare-attachment acme roadmap issue-one notes notes.txt /tmp/notes.txt 1 /tmp/attachment.plan.json
sh cookbook/scripts/project-tracker.sh publish cookbook/.state/project-tracker /tmp/attachment.plan.json
sh cookbook/scripts/project-tracker.sh reconcile cookbook/.state/project-tracker /tmp/attachment.plan.json
sh cookbook/scripts/project-tracker.sh resolve-link cookbook/.state/project-tracker /tmp/attachment.plan.json
sh cookbook/scripts/project-tracker.sh download cookbook/.state/project-tracker /tmp/attachment.plan.json /tmp/restored-notes.txt
```

The issue must exist before linking. Publication does not change its SQL state.
`reconcile` verifies complete native bytes and the manifest, then reuses the
original link identity and issue revision. A concurrent issue edit produces a
durable `conflict`; recovery never refreshes the revision. After inspecting the
issue, an operator can explicitly prepare another plan for the same immutable
descriptor and bytes with the current revision. The original rejected request
keeps its original outcome. Reusing an attachment ID with different bytes or
display metadata fails; identical existing links do not add a revision or Effect.

## Dashboard delivery and recovery

Save an explicit source roster:

```json
{"tenant":"acme","projects":["roadmap","operations"]}
```

```sh
sh cookbook/scripts/project-tracker.sh serve cookbook/.state/project-tracker /tmp/roster.json 600 0 0
```

The final two numbers are first-delivery and first-reply delays, each bounded to
10 seconds. The 30-second native lease covers both supported delays plus time for
publication and acknowledgment; a regression test verifies settlement on the
original attempt at the maximum delays. The library can pin controls to one
existing intent in the declared source roster; startup rejects a foreign intent
before spawning workers. Add `drop-reply` to lose the first delayed reply after
real receiver publication. The serve command emits `projected` checkpoints with source revision,
stable Effect ID, receiver decision, and separate source/dashboard commit sequences.
Interrupt stops serving and drains accepted work. After drain, use `progress`,
`effect`, `lookup`, and `dashboard` to inspect source and receiver evidence.

`effect STATE TENANT PROJECT EFFECT_HEX` reads a historical source intent without
revealing its lease token. Missing ledger evidence proves no dashboard absence.
`dashboard STATE TENANT [AFTER_PROJECT] [LIMIT]` reads 1..16 current summaries in
canonical key order. Each page observes its own dashboard commit, rather than a
multi-page snapshot. Stop serving before using another CLI writer against its Cells.

| Domain | Durable responsibility |
| --- | --- |
| Project entity | Complete issue/reference roster, issue and project revisions, and an atomic native summary intent. |
| Private Blob | Missing-key conditional staging, immutable complete manifests, and verified content. |
| Tenant dashboard | Monotonic full summary per project, counts, and complete source-state digest at that revision. |

A source receipt proves a position only in that project Cell. It does not prove
that the dashboard is current. Summaries carry the full source revision and
digest: older revisions succeed harmlessly, identical revisions are repeatable,
and different content at the same revision is rejected. The signed receiver pins
the exact source Cell, project key, destination, command, tenant, and application.
The explicit roster is application input; a dashboard page is never treated as
a complete source inventory.

Retain request and plan files outside working directories removed during cold
restoration. Upload phases and links preserve their original five-minute native
windows; expired resolution runs offline and proves no absence. Immutable
manifest and business bindings survive beyond those request windows. A stopped
owner can restore local SQLite from authority-pinned objects. After a crash,
allow the documented writer/session and Effect leases to expire before takeover.

## Bounds and verification

| Resource | Local profile |
| --- | --- |
| Issues per project | 32 permanent IDs. |
| Links per issue / project | Eight / 64 immutable references. |
| Dashboard projects per tenant / page | 64 / 16. |
| Complete attachment bytes | 1..65536, one native staged part. |
| Serving roster / workers | One or two projects; two native workers per project. |
| Source query / retained input or output | 128 / 384 KiB. |
| Display name or title / issue description | 120 / 512 UTF-8 bytes without padding or controls. |
| Native Effect lease / intent lifetime | 30 seconds / seven days. |
| Native database / capture limits per Cell | 64 / 16 MiB. |

This profile has four local SQL worker slots: two projects, one dashboard, and
one Blob Cell. Larger source rosters require an embedding's fleet assembly.
No issue, manifest, or Blob part deletion is implemented. Reference collection
requires the complete cross-Cell reference set, quiesced writes, and a grace
boundary; pending dashboard rows cannot establish that set.

```sh
cargo test --manifest-path cookbook/Cargo.toml -p cellule-cookbook-project-tracker --all-features --locked
cargo clippy --manifest-path cookbook/Cargo.toml -p cellule-cookbook-project-tracker --all-targets --all-features --locked -- -D warnings
python3 cookbook/scenarios/project-tracker.py BINARY
```

Run process qualification only in CI or an isolated source snapshot with private
authoritative storage. The process driver verifies actual crashes after Blob and
dashboard publication, out-of-order delivery, original-outcome resolution, cold
byte restoration, and SIGTERM after real demo delivery with accepted work drained.

See the [catalog](../../../docs/cookbook.md), [cookbook guide](../../README.md),
and [application integration guide](../../../docs/framework.md).
