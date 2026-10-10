#!/bin/sh
# Store diagnostics use their own explicit budget; legacy process gates remain
# in run.sh. Complete records and cold readback are checked by the Rust test.
set -eu
role=${CELLULE_FOLLOWER_BENCH_ROLE:?}
case "$role" in *[!a-z0-9-]*) exit 2 ;; esac
snapshot() {
  for counter in cpu.max cpu.stat memory.max memory.swap.max memory.peak memory.events; do
    printf '%s\n' "$counter"
    cat "/sys/fs/cgroup/$counter"
  done
}
snapshot > "/evidence/$role-kernel-before.txt"
read -r quota period < /sys/fs/cgroup/cpu.max
test "$quota" -eq "$((4 * period))"
test "$(cat /sys/fs/cgroup/memory.max)" -eq 4294967296
test "$(cat /sys/fs/cgroup/memory.swap.max)" -eq 0
set +e
/benchmark --ignored --exact follower::tests::performance::warm_append_diagnostic --nocapture \
  > "/evidence/$role.log" 2>&1
result=$?
set -e
snapshot > "/evidence/$role-kernel-after.txt"
cat "/evidence/$role.log"
test "$result" -eq 0
grep -Eq 'test result: ok\. 1 passed; 0 failed;' "/evidence/$role.log"
