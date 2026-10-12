# Remote unpublished-tail verification

This fixture checks a follower-acknowledged write after owner process loss or
a follower TCP partition, using the SQL example's original signed mTLS transport
and the public recovery
coordinator. It requires three separate Linux VMs and a support Docker host.
It is an environment-dependent fault test, not a throughput benchmark.

| Requirement | Verification |
| --- | --- |
| Three separate node VMs | Distinct Docker host IDs and kernel boot IDs |
| Each node has 1 vCPU and 2 GiB | Host inventory and container cgroup limits |
| Native durable storage | Fresh ext4 Docker volumes for SQLite and follower logs |
| Remote durability before publication | Both followers acknowledge; Cell root and selected bundle coverage remain zero |
| Owner process loss | SIGKILL after the ACK; successor uses a different fresh volume |
| Recovery authority | Actual predecessor lease expiry, fencing CAS, remote sealing and complete tail recovery |
| Exact retry | Original result and commit sequence; retry handler fails if executed |
| Partitioned output | After blocking both follower paths, a captured second mutation and its observed read cannot return success |
| Missing recovery witness | Recovery refuses both unreachable followers before installing an overlay or taking over the Cell |
| One complete witness | Reaching one original sealed follower recovers the first ACK while the second path remains blocked |
| Joined resources | Successor and followers exit successfully; all owned containers become terminal without OOM |

The support host runs the pinned RustFS provider. Configure loopback routes
between these hosts before running: every node must reach the provider at
`127.0.0.1:9000`, and the owner must reach the two followers at
`127.0.0.1:8090` and `127.0.0.1:8100`. Forward raw TCP; the application keeps
mTLS and signed peer authorization end to end. The support host needs two
available CPUs for its provider, and node VMs must have swap disabled.

Build the SQL example's **test executable** for the node VMs' Linux architecture
in an isolated source snapshot. Use the pinned Rust image from
[the capacity driver](../scripts/perf/run.py), with `CARGO_TARGET_DIR` under the
mounted Workspace target directory. The build command is:

```sh
cargo test --release --locked -p cellule-axum --example sql --no-run --message-format=json
```

Use the `compiler-artifact` executable for target `sql` with `profile.test=true`,
rather than the ordinary SQL server executable. The driver checks that it
contains the exact ignored `fleet::remote_fault_tests::remote_crash_fixture_child`
test. Supply that executable and the Docker contexts:

```sh
python3 scripts/verify-remote-follower-tail.py \
  --artifacts "$HOME/Workspace/crabbuild-target/cellule-remote-tail" \
  --binary /absolute/path/to/linux-sql-test-executable \
  --context support-context \
  --node-contexts owner-context follower-1-context follower-2-context \
  --tag remote-tail-r1
```

Use a new tag for each case. The driver creates private fixture certificates,
an isolated object prefix, and fresh provider/node volumes. It retains lifecycle
records, logs, native-storage/resource observations, and the recovered result
outside the source checkout. Completed containers and volumes remain available
for inspection. Run it separately from performance measurements on these hosts.

For the partition case:

```sh
python3 scripts/verify-remote-follower-tail.py \
  --artifacts "$HOME/Workspace/crabbuild-target/cellule-remote-tail" \
  --binary /absolute/path/to/linux-sql-test-executable \
  --context support-context \
  --node-contexts owner-context follower-1-context follower-2-context \
  --tag peer-partition-r1 \
  --fault peer-partition \
  --owner-ssh-config /absolute/path/to/owner-vm/ssh.config \
  --owner-ssh-host owner-vm-alias
```

The SSH account needs passwordless access to `sudo iptables`. The driver checks
that SSH and Docker reach the same owner kernel, then installs two uniquely
tagged OUTPUT rules for the follower TCP ports. Packet counters must show actual
drops on both paths. It restores one path for recovery, verifies the original
ACK and exact retry while the other remains blocked, then heals that path before
joining the successor and followers. Cleanup removes only its own rules, and
stops the provider after all node roles become terminal. All markers are fsynced
on the roles' native volumes.

Two fresh cases passed with one unpublished ACK per case on the three-VM
profile. The original failed fixture invocation is retained: successor
enrollment occurred before predecessor lease expiry, exceeding the example's
three-live-session bound. The corrected driver waits for actual expiry without
changing that bound or spoofing time. See
[the measurement record](publication-progress-measurements.md).

Two corrected peer-partition cases also pass with one unpublished ACK each.
Earlier failed cases remain retained, including a pre-partition grant rejection
caused by receiver issue times ahead of the sender's clock. See the same
measurement record for the diagnosis, correction, native packet counters and
per-case limits.

This checks process loss and, when selected, the two follower TCP paths. It does
not simulate physical power loss or partition the object-store path.
The existing [device-backed fault tests](../crates/cellule-runtime/tests/runtime/lifecycle/ownership/follower_process.rs)
cover separate object-store TCP-partition cases. Neither fixture establishes
capacity, latency improvement, or the cause of the original intermittent
maintenance `Fenced` failures.
