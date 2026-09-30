# Peer HTTP guide

`PeerHttpRoundTrip` sends authenticated Cell peer envelopes to the current
enrolled owner. This crate sends peer envelopes; it does not expose a public Cell
endpoint.

| Field | Value |
| --- | --- |
| Content type | Guide and crate reference |
| Audience | Application authors wiring peer transport |
| Goal | Route peer requests with bounded retries and honest unknown outcomes |
| Status | Transport mechanics current; the embedding application owns ingress, enrollment, and authorization |

| Document | For |
| --- | --- |
| [Routing and outcomes](routing.md) | Retry and unknown-result semantics. |
| [Security boundary](security.md) | Identity, scope, and ingress ownership. |
| [Crate entry](../README.md) | Overview and verification. |

<a id="contents"></a>
## Contents

- [Overview](#overview)
- [Owner-routed attempts](#owner-routed-attempts)
- [Outcome and deadline rules](#outcome-and-deadline-rules)
- [Security boundary](#security-boundary)
- [Admission bounds](#admission-bounds)
- [Verification](#verification)
- [Historical notes](#historical-notes)
- [See also](#see-also)

<a id="overview"></a>
## Overview

- Reloads the authority and node directory on a retry.
- Bounds responses.
- Distinguishes an unknown outcome from a request that never started.
- Caches HTTP clients by enrolled session, certificate, and public key.

<a id="owner-routed-attempts"></a>
## Owner-routed attempts

Owner-routed requests make at most two attempts:

```mermaid
sequenceDiagram
    participant Caller
    participant RoundTrip as PeerHttpRoundTrip
    participant Directory as Authority and node directory
    participant Owner as Enrolled owner
    Caller->>RoundTrip: Signed envelope and deadline
    RoundTrip->>Directory: Resolve the current owner
    Directory-->>RoundTrip: Owner advertisement
    RoundTrip->>Owner: POST internal/cells/v1/forward
    alt Reply within bounds
        Owner-->>RoundTrip: Peer reply
        RoundTrip-->>Caller: Decoded reply
    else 429 or 503
        Owner-->>RoundTrip: Admission rejection
        Note over RoundTrip: Integer Retry-After paces the retry,<br/>otherwise ownership refreshes immediately
        RoundTrip->>Directory: Reload ownership
        RoundTrip->>Owner: Second and final attempt
        Owner-->>RoundTrip: Reply or capacity error
        RoundTrip-->>Caller: Reply, capacity error, or deadline
    else Lost or invalid response
        Owner--xRoundTrip: No usable reply
        RoundTrip-->>Caller: Unknown outcome, not retried here
    end
    Note over RoundTrip,Owner: Direct-node activation stays a single attempt
```

<a id="outcome-and-deadline-rules"></a>
## Outcome and deadline rules

| Response or condition | Transport behavior |
| --- | --- |
| HTTP 429/503 with an integer `Retry-After` | Pace the retry within the original deadline, then reload ownership. |
| HTTP 429/503 without that header | Retain the immediate owner-refresh behavior used by existing receivers. |
| Persistent admission rejection | Returns a capacity error. |
| A delay that leaves no request budget | Returns a deadline error without sleeping. |
| Lost or invalid response | Remains an unknown outcome and is not retried here. |
| Direct-node activation | Stays a single attempt; its caller owns scheduling and retries and receives the same capacity distinction. |

<a id="security-boundary"></a>
## Security boundary

- The product supplies `PeerTargetScope` and `PeerHttpClientFactory`.
- The factory must authenticate its local client identity and pin the remote
  certificate and public key passed to it.
- The receiver must verify the peer envelope, enrollment, and product
  authorization before dispatching through `PeerDispatcher`.
- The crate provides no public ingress or credential discovery.
- Install a receiver at `internal/cells/v1/forward` only after the application
  has wired its identity, enrollment, authorization, and readiness checks.

<a id="admission-bounds"></a>
## Admission bounds

Both owner-routed and direct-node requests reject bodies above
`MAX_PEER_REQUEST_BYTES` and a zero deadline before provider lookup or dispatch.

<a id="verification"></a>
## Verification

`cargo test -p cellule-peer-http --locked` covers both routes' admission bounds,
real local HTTP retry/unknown-outcome classification, and TLS fleet identity
canonicalization.

The HTTP fixtures do not qualify deployed mTLS enrollment or certificate
rotation; those require the application's fleet tests.

<a id="historical-notes"></a>
## Historical notes

The original synthesis documented this transport as part of the Crab HTTP
server. Those product routes, listeners, and deployment steps are not Cellule
requirements; this crate keeps only the owner-resolving transport mechanics
above.

<a id="see-also"></a>
## See also

| Document | What it covers |
| --- | --- |
| [Routing and outcomes](routing.md) | Retry and unknown-result semantics. |
| [Security boundary](security.md) | Identity, scope, and ingress ownership. |
| [Crate entry](../README.md) | Overview and verification. |
| [Follower durability and owner loss](../../cellule-runtime/docs/failover-and-followers.md) | Owner moves, follower proof, and recovery. |
| [Authority, storage, and recovery](../../cellule-runtime/docs/storage.md) | Control records and owner-fenced authority. |
| [Embed and operate Cellule](../../cellule-runtime/docs/deployment.md) | Peer TLS identity and fleet wiring. |
| [Architecture overview](../../../docs/architecture.md) | Where peer transport sits in Cellule. |
