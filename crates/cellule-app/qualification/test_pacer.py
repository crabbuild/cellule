import csv
from pathlib import Path
import struct
import tempfile
import unittest

from pacer import verify_run, verify_window


class PacerEvidenceTests(unittest.TestCase):
    def sample(self):
        window = dict(round="0", guard_us="50", rate="30", seconds="10", count="300",
                      elapsed_us="10000100", cpu_us="500000")
        raw = b"".join(struct.pack(">QQQ", arrival, arrival * 1_000_000 // 30 + 12,
                                   arrival * 1_000_000 // 30 + 20) for arrival in range(300))
        return window, raw

    def test_complete_window_preserves_delays_and_count(self):
        window, raw = self.sample()
        result = verify_window(window, raw)
        self.assertEqual(result["count"], 300)
        self.assertEqual(result["generator_delay"]["p99_ms"], 0.012)
        self.assertEqual(result["handoff_delay"]["p99_ms"], 0.020)

    def test_missing_arrival_is_rejected(self):
        window, raw = self.sample()
        with self.assertRaisesRegex(AssertionError, "lost pacer"):
            verify_window(window, raw[:-24])

    def test_substituted_arrival_is_rejected(self):
        window, raw = self.sample()
        with self.assertRaisesRegex(AssertionError, "substituted"):
            verify_window(window, struct.pack(">Q", 1) + raw[8:])

    def test_early_generation_is_rejected(self):
        window, raw = self.sample()
        changed = raw[:32] + struct.pack(">Q", 1) + raw[40:]
        with self.assertRaisesRegex(AssertionError, "timestamp"):
            verify_window(window, changed)

    def test_receipt_after_measurement_is_rejected(self):
        window, raw = self.sample()
        changed = raw[:-8] + struct.pack(">Q", 10000200)
        with self.assertRaisesRegex(AssertionError, "timestamp"):
            verify_window(window, changed)

    def test_clock_reversal_is_rejected(self):
        window, raw = self.sample()
        changed = struct.pack(">QQQ", 0, 100000, 100020) + raw[24:]
        with self.assertRaisesRegex(AssertionError, "backward"):
            verify_window(window, changed)

    def test_impossible_cpu_usage_is_rejected(self):
        window, raw = self.sample()
        window["cpu_us"] = "100000000"
        with self.assertRaises(AssertionError):
            verify_window(window, raw)

    def test_missing_window_is_rejected(self):
        window, _ = self.sample()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / "windows.tsv").open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=window, delimiter="\t")
                writer.writeheader()
                writer.writerow(window)
            with self.assertRaisesRegex(AssertionError, "calibration window"):
                verify_run(root)


if __name__ == "__main__":
    unittest.main()
