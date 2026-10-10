#!/bin/sh
set -eu
test ! -e /evidence/pacer.log
snapshot() {
  for counter in cpu.max cpu.stat memory.max memory.swap.max memory.peak memory.events; do
    printf '%s\n' "$counter"
    cat "/sys/fs/cgroup/$counter"
  done
}
sha256sum /benchmark > /evidence/binary.sha256
snapshot > /evidence/kernel-before.txt
read -r quota period < /sys/fs/cgroup/cpu.max
test "$quota" -eq "$((4 * period))"
test "$(cat /sys/fs/cgroup/memory.max)" -eq 4294967296
test "$(cat /sys/fs/cgroup/memory.swap.max)" -eq 0
set +e
/benchmark --ignored --exact entities::process::driver::pacing::pacer_timing_diagnostic --nocapture > /evidence/pacer.log 2>&1
result=$?
set -e
snapshot > /evidence/kernel-after.txt
cat /evidence/pacer.log
test "$result" -eq 0
grep -Eq 'test result: ok\. 1 passed; 0 failed;' /evidence/pacer.log
