"""Store diagnostics reject incomplete samples and substituted environments."""

import csv
from pathlib import Path
import tempfile
import unittest

from follower import CASES, cases, verify_probe


class Profiles(unittest.TestCase):
    def test_focused_profile_preserves_existing_windows(self):
        self.assertEqual(cases("focused"), CASES)

    def test_full_matrix_has_both_coverage_modes_and_all_widths_within_budget(self):
        matrix = cases("d1-matrix")
        self.assertEqual(len(matrix), 18)
        for coverage in ("zero", "advancing"):
            self.assertEqual({(lanes, frames) for lanes, frames, _, mode in matrix if mode == coverage},
                             {(lanes, frames) for lanes in (1, 8, 32) for frames in (1, 16, 64)})
        self.assertTrue(all(1 <= rounds <= 32 and lanes * frames * (rounds + 1) <= 65_536
                            for lanes, frames, rounds, _ in matrix))

    def test_unknown_profile_is_rejected(self):
        with self.assertRaises(AssertionError):
            cases("unknown")


class ProbeEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.role = "probe"
        self.case = (1, 1, 2, "zero")
        self.observed = dict(
            HostConfig=dict(NanoCpus=4_000_000_000, Memory=4_294_967_296,
                            MemorySwap=4_294_967_296, PidsLimit=256,
                            ReadonlyRootfs=True, CapDrop=["ALL"]),
            State=dict(ExitCode=0, OOMKilled=False),
            Mounts=[dict(Destination="/scratch", Type="volume", Name="private")])
        (self.root / "probe.log").write_text(
            "FOLLOWER_APPEND_DIAGNOSTIC lanes=1 frames=1 rounds=2 coverage=zero batches=2 elapsed_us=1000 batches_per_second=2000.000\n"
            "test result: ok. 1 passed; 0 failed;\n")
        (self.root / "probe-kernel-after.txt").write_text(
            "cpu.max\n400000 100000\nmemory.max\n4294967296\nmemory.swap.max\n0\nmemory.peak\n100000\nmemory.events\noom 0\noom_kill 0\n")
        self.samples = [dict(leader="SessionId(" + "01" * 16 + ")", frames="1",
                             encoded_bytes="4096", blocking_queue_us="5", accounting_wait_us="5",
                             accounting_hold_us="90", lane_wait_us="1", append_us="80",
                             prune_us="10", data_sync_us="30", directory_sync_us="10",
                             recount_us="5", total_us="100", data_sync_calls="1",
                             directory_sync_calls="1", recounts="1", succeeded="true") for _ in range(2)]
        self.write_samples()

    def write_samples(self):
        with (self.root / "probe.tsv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=self.samples[0], delimiter="\t")
            writer.writeheader()
            writer.writerows(self.samples)

    def verify(self):
        return verify_probe(self.root, self.role, self.case, self.observed)

    def test_units_are_batches_and_encoded_frames(self):
        result = self.verify()
        self.assertEqual(result["batches_per_second"], 2000)
        self.assertNotIn("write_tps", result)

    def test_missing_attempt_is_rejected(self):
        self.samples.pop()
        self.write_samples()
        with self.assertRaises(AssertionError):
            self.verify()

    def test_accounting_is_independent_of_filesystem_duration(self):
        self.samples[0]["accounting_hold_us"] = "1"
        self.write_samples()
        self.verify()

    def test_impossible_accounting_duration_is_rejected(self):
        self.samples[0]["accounting_hold_us"] = "101"
        self.write_samples()
        with self.assertRaises(AssertionError):
            self.verify()

    def test_unsynced_new_frames_are_rejected(self):
        self.samples[0]["data_sync_calls"] = "0"
        self.write_samples()
        with self.assertRaises(AssertionError):
            self.verify()

    def test_invalid_nested_phase_is_rejected(self):
        self.samples[0]["prune_us"] = "81"
        self.write_samples()
        with self.assertRaises(AssertionError):
            self.verify()

    def test_substituted_environment_is_rejected(self):
        self.observed["HostConfig"]["NanoCpus"] = 8_000_000_000
        with self.assertRaises(AssertionError):
            self.verify()

    def test_memory_backed_scratch_is_rejected(self):
        self.observed["Mounts"][0]["Type"] = "tmpfs"
        with self.assertRaises(AssertionError):
            self.verify()


if __name__ == "__main__":
    unittest.main()
