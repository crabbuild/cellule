"""Reject incomplete primary audits and optimistic throughput classifications."""

import copy
import json
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from write import CELLS, PRIMARY, SMALL_KV, ATTRIBUTION, ATTRIBUTION_V2, WriteFleet, verify_attempt, verify_audit, verify_window, verify_idle, verify_backend_limits


class BackendEnvironment(unittest.TestCase):
    def setUp(self):
        self.observed = dict(HostConfig=dict(NanoCpus=2_000_000_000, Memory=2 * 2**30,
            MemorySwap=2 * 2**30, Ulimits=[dict(Name="nofile", Soft=65_536, Hard=65_536)]),
            State=dict(Running=True, OOMKilled=False, Health=dict(Status="healthy")))
        # Exact Linux /proc/1/limits formatting captured from the RustFS process.
        self.limits = "Max open files            65536                65536                files     \n"
        self.kernel = {"cpu.max": "200000 100000\n", "memory.max": str(2 * 2**30),
            "memory.swap.max": "0", "memory.current": "1000000", "memory.peak": "2000000",
            "memory.events": "oom 0\noom_kill 0\n", "open_descriptors": "100", "process_name": "rustfs\n"}

    def verify(self):
        return verify_backend_limits(self.observed, self.limits, self.kernel, 65_536)

    def test_new_environment_retains_traffic_gates_and_old_profile_limits(self):
        self.assertEqual(replace(ATTRIBUTION_V2, name=ATTRIBUTION.name, backend_nofile=0), ATTRIBUTION)
        self.assertTrue(all(profile.backend_nofile == 0 for profile in (PRIMARY, SMALL_KV, ATTRIBUTION)))
        self.assertEqual(self.verify()["nofile_soft"], 65_536)

    def test_compose_limit_cannot_replace_actual_process_or_kernel_evidence(self):
        self.limits = self.limits.replace("65536", "1024")
        with self.assertRaisesRegex(AssertionError, "process file limit"):
            self.verify()
        self.limits = self.limits.replace("1024", "65536")
        self.kernel["cpu.max"] = "400000 100000"
        with self.assertRaises(AssertionError):
            self.verify()

    def test_backend_failure_and_resource_overshoot_are_rejected(self):
        original = dict(self.kernel)
        for key, value in (("memory.events", "oom 1\noom_kill 0\n"),
                           ("memory.peak", str(2 * 2**30 + 1)),
                           ("open_descriptors", "65537"), ("process_name", "sh\n")):
            with self.subTest(key=key):
                self.kernel = dict(original, **{key: value})
                with self.assertRaises(AssertionError):
                    self.verify()

    def test_failed_preflight_retains_actual_process_and_kernel_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = object.__new__(WriteFleet)
            fleet.evidence = Path(directory)
            fleet.profile = ATTRIBUTION_V2
            fleet.compose = lambda *args: "backend\n"
            fleet.inspect = lambda container: self.observed
            outputs = {f"/sys/fs/cgroup/{key}": value for key, value in self.kernel.items()}
            outputs["/sys/fs/cgroup/cpu.stat"] = "usage_usec 100\n"
            outputs["/proc/1/limits"] = self.limits.replace("65536", "1024")
            outputs["/proc/1/comm"] = self.kernel["process_name"]
            outputs["/proc/1/fd"] = "\n".join(str(fd) for fd in range(100))
            fleet.run = lambda *args: outputs[args[-1]]
            with self.assertRaisesRegex(AssertionError, "process file limit"):
                fleet.backend_snapshot("before")
            retained = json.loads((fleet.evidence / "backend-before.json").read_text())
            self.assertEqual(retained["process_limits"], outputs["/proc/1/limits"])
            self.assertEqual(retained["kernel"]["open_descriptors"], "100")
            self.assertNotIn("report", retained)


def attempt(request, entity, sequence=1, key=None):
    return dict(request=str(request), entity=str(entity), scheduled_us="0", generated_us="0",
                dispatched_us="0", terminal_us="1000", outcome="ok", sequence=str(sequence), resolution="none",
                command_id=request.to_bytes(8, "big").hex() + "00" * 8,
                key=str(entity if key is None else key), digest=request.to_bytes(32, "big").hex(), payload_bytes="1024")


class PrimaryWindow(unittest.TestCase):
    def setUp(self):
        self.window = dict(window="2", phase="measure", rate="1", seconds="60", concurrency="1024",
                           started_ms="1000", ended_ms="61001", started_boot_ms="1000", ended_boot_ms="61001",
                           elapsed_us="60001000", fully_served="true")
        self.attempts = []
        for index in range(60):
            row = attempt((2 << 32) | index, index % CELLS, index + 1)
            row.update(scheduled_us=str(index * 1_000_000), generated_us=str(index * 1_000_000),
                       dispatched_us=str(index * 1_000_000), terminal_us=str(index * 1_000_000 + 1000))
            self.attempts.append(row)

    def test_complete_window(self):
        result = verify_window(self.window, self.attempts)
        self.assertTrue(result["fully_served"])
        self.assertEqual(result["acknowledgement_tps"], 1)

    def test_missing_intended_arrival(self):
        with self.assertRaisesRegex(AssertionError, "missing intended"):
            verify_window(self.window, self.attempts[:-1])

    def test_resolved_unknown_remains_outside_successful_tps(self):
        self.attempts[-1].update(outcome="unknown", resolution="committed")
        self.window["fully_served"] = "false"
        result = verify_window(self.window, self.attempts)
        self.assertFalse(result["fully_served"])
        self.assertEqual(result["acknowledgement_tps"], 59 / 60)
        self.window["fully_served"] = "true"
        with self.assertRaisesRegex(AssertionError, "false primary"):
            verify_window(self.window, self.attempts)

    def test_slow_success_cannot_be_claimed_served(self):
        self.attempts[-1]["terminal_us"] = "59100000"
        with self.assertRaisesRegex(AssertionError, "false primary"):
            verify_window(self.window, self.attempts)

    def test_command_identity_mismatch(self):
        self.attempts[0]["command_id"] = "ff" * 16
        with self.assertRaises(AssertionError):
            verify_attempt(self.attempts[0])


class PrimaryAudit(unittest.TestCase):
    def setUp(self):
        self.attempts = [attempt(entity, entity) for entity in range(CELLS)]
        self.attempts += [attempt(2 << 32, 0, 2, 0), attempt((2 << 32) | 64, 0, 3, 0)]
        self.audit = [{key: row[key] for key in ("entity", "request", "command_id", "key", "digest", "sequence")}
                      for row in self.attempts]
        latest = {row["entity"]: row for row in self.attempts}
        self.values = [dict(entity=row["entity"], key=row["key"], digest=row["digest"], sequence=row["sequence"], payload_bytes="1024")
                       for row in latest.values()]
        self.owners = [dict(entity=str(entity), cell=f"cell-{entity}", owner=str(entity % 3), epoch="1", incarnation=f"incarnation-{entity}") for entity in range(CELLS)]
        self.roots = [dict(**owner, root_sequence="3" if owner["entity"] == "0" else "1",
                           required_sequence="3" if owner["entity"] == "0" else "1", root_digest="Digest(" + "ab" * 32 + ")") for owner in self.owners]

    def verify(self):
        verify_audit(self.attempts, self.audit, self.values, self.owners, self.roots)

    def test_every_overwritten_command_is_audited(self):
        self.verify()
        self.audit.pop(-2)
        with self.assertRaisesRegex(AssertionError, "missing or duplicate"):
            self.verify()

    def test_duplicate_audit_cannot_replace_a_missing_one(self):
        self.audit[-2] = copy.deepcopy(self.audit[-1])
        with self.assertRaisesRegex(AssertionError, "duplicate audit"):
            self.verify()

    def test_final_value_uses_commit_order(self):
        self.values[0]["digest"] = self.attempts[-2]["digest"]
        with self.assertRaisesRegex(AssertionError, "overwritten value"):
            self.verify()

    def test_missing_published_suffix(self):
        self.roots[0]["root_sequence"] = "2"
        with self.assertRaisesRegex(AssertionError, "durable suffix"):
            self.verify()


class SmallKvWindow(unittest.TestCase):
    def setUp(self):
        PrimaryWindow.setUp(self)
        for row in self.attempts:
            row["payload_bytes"] = "96"

    def test_named_profile_keeps_the_1024_request_bound_and_reports_owner_tps(self):
        result = verify_window(self.window, self.attempts, SMALL_KV)
        self.assertTrue(result["fully_served"])
        self.assertAlmostEqual(sum(row["acknowledgement_tps"] for row in result["per_owner"].values()), 1)
        self.assertEqual(result["logical_mib_per_second"], 96 / 2**20)
        self.window["concurrency"] = "96"
        with self.assertRaises(AssertionError):
            verify_window(self.window, self.attempts, SMALL_KV)

    def test_small_profile_cannot_be_reported_as_primary(self):
        with self.assertRaises(AssertionError):
            verify_window(self.window, self.attempts)

    def test_last_cell_is_valid_only_in_the_small_profile(self):
        row = attempt(123, 999)
        row["payload_bytes"] = "96"
        verify_attempt(row, SMALL_KV)
        with self.assertRaises(AssertionError):
            verify_attempt(row)

    def test_all_thousand_cells_and_their_durable_roots_are_required(self):
        attempts = [dict(attempt(entity, entity), payload_bytes="96") for entity in range(SMALL_KV.cells)]
        audit = [{key: row[key] for key in ("entity", "request", "command_id", "key", "digest", "sequence")}
                 for row in attempts]
        values = [dict(entity=row["entity"], key=row["key"], digest=row["digest"], sequence="1", payload_bytes="96")
                  for row in attempts]
        owners = [dict(entity=str(entity), cell=f"cell-{entity}", owner=str(entity % 3), epoch="1", incarnation=f"incarnation-{entity}")
                  for entity in range(SMALL_KV.cells)]
        roots = [dict(**owner, root_sequence="1", required_sequence="1", root_digest="Digest(" + "ab" * 32 + ")")
                 for owner in owners]
        verify_audit(attempts, audit, values, owners, roots, SMALL_KV)
        with self.assertRaises(AssertionError):
            verify_audit(attempts, audit, values, owners[:-1], roots, SMALL_KV)
        values[-1]["payload_bytes"] = "1024"
        with self.assertRaises(AssertionError):
            verify_audit(attempts, audit, values, owners, roots, SMALL_KV)


class IdleAttributionEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "write-idle.tsv").write_text(
            "seconds\tstarted_ms\tended_ms\tstarted_boot_ms\tended_boot_ms\telapsed_us\n"
            "60\t100000\t160001\t100000\t160001\t60001000\n")
        for node in range(3):
            prefix = self.root / f"node-{node}"
            Path(f"{prefix}-executions.tsv").write_text("at_ms\n")
            Path(f"{prefix}-node-log-submissions.tsv").write_text("at_ms\n")
            Path(f"{prefix}-control-transitions.tsv").write_text(
                "at_ms\tcell\ttransition\telapsed_us\tsucceeded\n"
                "120000\tCellId(" + "01" * 32 + ")\tRenew\t20\ttrue\n")
            Path(f"{prefix}-object-operations.tsv").write_text(
                "at_ms\toperation\toutcome\tduration_us\tbytes_read\tbytes_written\n"
                "120000\tput\tsuccess\t20\t0\t100\n")
            cells = len(range(node, ATTRIBUTION.cells, 3))
            Path(f"{prefix}-resources.tsv").write_text(
                f"at_ms\tactive_cells\n101000\t{cells}\n159000\t{cells}\n")

    def test_idle_control_is_separate_and_preserves_the_small_kv_gates(self):
        self.assertEqual(ATTRIBUTION.rates, SMALL_KV.rates)
        self.assertEqual(ATTRIBUTION.cells, SMALL_KV.cells)
        self.assertEqual(ATTRIBUTION.payload_bytes, SMALL_KV.payload_bytes)
        result = verify_idle(self.root, ATTRIBUTION)
        self.assertEqual(result["nodes"][0]["control_transitions"]["Renew"]["attempts"], 1)
        for profile in (SMALL_KV, PRIMARY):
            with self.assertRaisesRegex(AssertionError, "unexpected idle"):
                verify_idle(self.root, profile)

    def test_application_sql_or_submission_invalidates_idle_control(self):
        for name, message in (("executions", "application SQL"), ("node-log-submissions", "node-log submission")):
            path = self.root / f"node-0-{name}.tsv"
            path.write_text("at_ms\n120000\n")
            with self.assertRaisesRegex(AssertionError, message):
                verify_idle(self.root, ATTRIBUTION)
            path.write_text("at_ms\n")

    def test_idle_requires_renewal_and_the_complete_interval(self):
        path = self.root / "node-0-control-transitions.tsv"
        original = path.read_text()
        path.write_text("at_ms\tcell\ttransition\telapsed_us\tsucceeded\n")
        with self.assertRaisesRegex(AssertionError, "did not exercise Cell renewal"):
            verify_idle(self.root, ATTRIBUTION)
        path.write_text(original)
        path = self.root / "write-idle.tsv"
        path.write_text(path.read_text().replace("60001000", "59001000"))
        with self.assertRaises(AssertionError):
            verify_idle(self.root, ATTRIBUTION)


if __name__ == "__main__":
    unittest.main()
