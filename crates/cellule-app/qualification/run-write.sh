#!/bin/sh
# This primary diagnostic has explicit budgets and its own selector. Keep
# run.sh's one-CPU regression checks unchanged.
set -eu
case "${1:-}" in
  node) role="node-${CELLULE_PERF_PROCESS_NODE:?}"; memory=8589934592; selected=entities::process::entity_process_node ;;
  driver) role=driver; memory=4294967296; selected=entities::process::driver::primary::write_process_capacity ;;
  *) exit 2 ;;
esac
cores=4
case "${CELLULE_WRITE_PROFILE:?}" in
  v1) ;;
  small-kv-fleet-v1|small-kv-attribution-v1|small-kv-attribution-v2)
    if test "$role" != driver; then
      cores=8; memory=17179869184
      test "$(stat -f -c '%T' /scratch)" = tmpfs
      test "$(ulimit -n)" -eq 65536
    fi ;;
  *) exit 2 ;;
esac
test ! -e "/evidence/$role.log"
snapshot() {
  for counter in cpu.max cpu.stat memory.max memory.swap.max memory.peak memory.events; do
    printf '%s\n' "$counter"
    cat "/sys/fs/cgroup/$counter"
  done
  printf 'scratch_filesystem\n'
  stat -f -c '%T' /scratch
}
sha256sum /benchmark > "/evidence/$role-binary.sha256"
snapshot > "/evidence/$role-kernel-before.txt"
read -r quota period < /sys/fs/cgroup/cpu.max
test "$quota" -eq "$((cores * period))"
test "$(cat /sys/fs/cgroup/memory.max)" -eq "$memory"
test "$(cat /sys/fs/cgroup/memory.swap.max)" -eq 0
set +e
/benchmark --ignored --exact "$selected" --nocapture > "/evidence/$role.log" 2>&1
result=$?
set -e
snapshot > "/evidence/$role-kernel-after.txt"
cat "/evidence/$role.log"
test "$result" -eq 0
grep -Eq 'test result: ok\. 1 passed; 0 failed;' "/evidence/$role.log"
