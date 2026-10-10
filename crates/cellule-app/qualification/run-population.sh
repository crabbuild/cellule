#!/bin/sh
set -eu
case "${CELLULE_WRITE_PROFILE:?}" in
  population-v1) selected=entities::process::population::owner_population ;;
  owner-reads-v1) selected=entities::process::population::owner_read_capacity ;;
  *) exit 2 ;;
esac
test ! -e /evidence/owner.log
mkdir -p /evidence/control
snapshot() {
  for counter in cpu.max cpu.stat memory.max memory.swap.max memory.peak memory.events; do
    printf '%s\n' "$counter"
    cat "/sys/fs/cgroup/$counter"
  done
}
sha256sum /benchmark > /evidence/owner-binary.sha256
uname -a > /evidence/linux-platform.txt
cat /proc/self/limits > /evidence/linux-limits.txt
snapshot > /evidence/owner-kernel-before.txt
read -r quota period < /sys/fs/cgroup/cpu.max
test "$quota" -eq "$((8 * period))"
test "$(cat /sys/fs/cgroup/memory.max)" -eq 17179869184
test "$(cat /sys/fs/cgroup/memory.swap.max)" -eq 0
test "$(ulimit -Sn)" -eq 65536
test "$(ulimit -Hn)" -eq 65536
set +e
/benchmark --ignored --exact "$selected" --nocapture > /evidence/owner.log 2>&1
result=$?
set -e
snapshot > /evidence/owner-kernel-after.txt
cat /evidence/owner.log
test "$result" -eq 0
grep -Eq 'test result: ok\. 1 passed; 0 failed;' /evidence/owner.log
