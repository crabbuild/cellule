# Types guide

`cellule-types` holds stable, dependency-light storage identities shared by the
Cellule layers.

| Field | Value |
| --- | --- |
| Content type | Guide and crate reference |
| Audience | Storage, LTX, and runtime contributors |
| Goal | Change identity equality or serialization without breaking routing |

| Document | Use it for |
| --- | --- |
| [Identity contracts](identity.md) | Provider aliases, bucket equality, and scoped paths. |
| [Crate entry](../README.md) | Ownership and verification. |

<a id="contents"></a>
## Contents

- [Overview](#overview)
- [Change impact](#change-impact)
- [Verification](#verification)
- [See also](#see-also)

<a id="overview"></a>
## Overview

- `StorageProviderKind` identifies the provider family.
- `BucketIdentity` identifies a physical storage destination without depending
  on `object_store` or a product server.
- Identity fields are persisted or reused across requests. Review all consumers
  before changing normalization or serialized values.

```text
BucketIdentity
├── cloud      StorageProviderKind: S3 | Gcs | Azure | Local
├── host       normalized: lowercased, trailing slashes trimmed
└── container  normalized: lowercased, trailing slashes trimmed
```

<a id="change-impact"></a>
## Change impact

Changes to identity equality or serialization affect cache and storage routing.
See [the API](../src/storage.rs) and
[architecture](../../../docs/architecture.md).

<a id="verification"></a>
## Verification

```sh
cargo test -p cellule-types --locked
```

<a id="see-also"></a>
## See also

| Document | What it covers |
| --- | --- |
| [Identity contracts](identity.md) | Provider aliases, bucket equality, and scoped paths. |
| [Crate entry](../README.md) | Ownership and verification. |
| [Store guide](../../cellule-store/docs/README.md) | Provider-neutral transport that consumes these identities. |
| [LTX guide](../../cellule-ltx/docs/README.md) | Cell object layout and root formats. |
| [Architecture overview](../../../docs/architecture.md) | Layer placement and boundaries. |
