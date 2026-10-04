import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "apply-axum-write-telemetry.py"
SPEC = importlib.util.spec_from_file_location("telemetry_backport", SCRIPT)
BACKPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BACKPORT)

BEFORE = "begin\nadmission\nend\nstable\nstable2\npolicy=baseline\n"
AFTER = BEFORE.replace("admission\n", "admission\nobserve\n")


class TelemetryBackportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.candidate = self.root / "candidate"
        self.baseline = self.root / "baseline"
        self.evidence = self.root / "evidence"
        for source in (self.candidate, self.baseline):
            source.mkdir()
            for target in ("prepare.rs", "host.rs"):
                (source / target).write_text(AFTER)
        (self.candidate / "scripts").mkdir()
        patch = "".join(
            f"diff --git a/{target} b/{target}\n"
            f"--- a/{target}\n+++ b/{target}\n"
            "@@ -1,3 +1,4 @@\n begin\n admission\n+observe\n end\n"
            for target in ("prepare.rs", "host.rs")
        )
        (self.candidate / "scripts/axum-write-telemetry.patch").write_text(patch)

    def instrument(self):
        audit = BACKPORT.instrument(self.candidate, self.baseline, self.evidence)
        self.assertEqual(
            json.loads((self.evidence / "telemetry-admission.json").read_text()), audit
        )
        return {item["path"]: item for item in audit["targets"]}

    def reorganize_host(self):
        for source in (self.candidate, self.baseline):
            (source / "host.rs").write_text(AFTER.replace("begin", "new heading"))

    def test_fixed_patch_applies_to_uninstrumented_baseline(self):
        for target in ("prepare.rs", "host.rs"):
            (self.baseline / target).write_text(BEFORE)
        (self.candidate / "prepare.rs").write_text(AFTER.replace("baseline", "candidate"))
        audit = self.instrument()
        for target in audit:
            self.assertEqual((self.baseline / target).read_text(), AFTER)
            self.assertEqual(audit[target]["proof"], "fixed-patch-applied")
            self.assertNotEqual(
                audit[target]["baseline_before_sha256"],
                audit[target]["baseline_after_sha256"],
            )

    def test_whole_reverse_check_leaves_baseline_unchanged(self):
        audit = self.instrument()
        for target in audit:
            self.assertEqual(audit[target]["proof"], "fixed-patch-reverse-check")
            self.assertEqual(audit[target]["baseline_before_sha256"],
                             audit[target]["baseline_after_sha256"])

    def test_reorganized_host_and_unrelated_candidate_policy_are_admitted(self):
        self.reorganize_host()
        (self.candidate / "prepare.rs").write_text(AFTER.replace("baseline", "candidate"))
        audit = self.instrument()
        self.assertEqual(audit["prepare.rs"]["proof"], "fixed-target-reverse-check")
        self.assertEqual(audit["host.rs"]["proof"], "candidate-byte-equality")
        self.assertEqual((self.baseline / "prepare.rs").read_text(), AFTER)
        for item in audit.values():
            self.assertEqual(item["baseline_before_sha256"], item["baseline_after_sha256"])

    def test_missing_observation_is_rejected_without_mutating_baseline(self):
        self.reorganize_host()
        (self.baseline / "prepare.rs").write_text(BEFORE)
        with self.assertRaisesRegex(ValueError, "cannot prove baseline telemetry in prepare.rs"):
            self.instrument()
        self.assertEqual((self.baseline / "prepare.rs").read_text(), BEFORE)

    def test_changed_observation_is_rejected(self):
        self.reorganize_host()
        changed = AFTER.replace("observe", "different_phase")
        (self.baseline / "prepare.rs").write_text(changed)
        with self.assertRaisesRegex(ValueError, "cannot prove baseline telemetry in prepare.rs"):
            self.instrument()
        self.assertEqual((self.baseline / "prepare.rs").read_text(), changed)

    def test_reorganized_host_mismatch_is_rejected(self):
        self.reorganize_host()
        changed = (self.baseline / "host.rs").read_text().replace("baseline", "other")
        (self.baseline / "host.rs").write_text(changed)
        with self.assertRaisesRegex(ValueError, "cannot prove baseline telemetry in host.rs"):
            self.instrument()
        self.assertEqual((self.baseline / "host.rs").read_text(), changed)


if __name__ == "__main__":
    unittest.main()
