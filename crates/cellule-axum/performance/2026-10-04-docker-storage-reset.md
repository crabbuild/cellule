# Docker storage reset for verification

The `durable-pending-16` probe is excluded from performance comparisons: the
shared Docker filesystem exhausted its inodes, and subsequent evidence copying
failed with ENOSPC. Its results do not establish a framework improvement or
regression. Raw outcomes and source provenance remain archived.

After archiving 2,948 evidence files (1,444,355,779 bytes) outside Docker and
confirming no benchmark processes were running, the task-owned RustFS container
and its complete fixture volume were removed. Rebuildable debug artifacts were
also reclaimed. Release benchmark binaries, source snapshots and journals were
preserved. Unrelated Docker resources were retained.

| Storage check | Before fixture reset | After cleanup |
| --- | ---: | ---: |
| Available disk space | 13 GiB | 34.4 GiB |
| Free inodes, of 5,242,880 | 9,995 | 2,705,498 |

The fixture uses the same pinned RustFS image, environment, network and
four-CPU/two-GiB limits. Its fresh `cellule-node-capacity` bucket passed a signed
object write, byte-identical read and deletion. This verifies storage readiness;
the [node capacity target](node-capacity.md) remains unqualified. New runs must
use fresh prefixes and empty follower stores, monitor disk and inode capacity,
and avoid overlapping builds or tests with measurement.

The [dataset](2026-10-04-docker-storage-reset.json) records the exact free-byte
count, inode counts, readiness evidence and preserved binary hashes. The
external evidence directory contains the original fixture configuration,
archive, before/after storage records and failed probe logs.
