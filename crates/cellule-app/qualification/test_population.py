"""Reject false resident population, coverage, idle, and drain evidence."""

import unittest

from population import POPULATIONS, verify_population


class OwnerPopulation(unittest.TestCase):
    def setUp(self):
        self.samples = []
        for cells in POPULATIONS:
            self.samples.append(dict(cells=str(cells), activation_us="1000" if cells else "0", idle_us="15000000" if cells else "0",
                                     idle_cpu_us="100000" if cells else "0", memory_current_bytes=str(10_000_000 + cells * 1000),
                                     memory_peak_bytes=str(10_000_000 + cells * 1000), process_rss_bytes=str(1_000_000 + cells * 100),
                                     descriptors=str(20 + cells), sqlite_descriptors=str(cells), retained_bytes="0", disk_reserved_bytes="0", renewals="3" if cells else "0"))
        self.cells = [dict(entity=str(entity), cell=f"cell-{entity}", incarnation=f"incarnation-{entity}", sequence="2", root_sequence="2", root_digest="Digest(" + "ab" * 32 + ")") for entity in range(2000)]
        self.drained = [dict(active_cells="0", sqlite_descriptors="0", descriptors="20")]

    def verify(self):
        return verify_population(self.samples, self.cells, self.drained)

    def test_population_and_idle_cost(self):
        result = self.verify()
        self.assertEqual(result[-1]["cells"], 2000)
        self.assertEqual(result[-1]["rss_delta_bytes_per_cell"], 100)

    def test_missing_cell(self):
        self.cells.pop()
        with self.assertRaisesRegex(AssertionError, "missing resident"):
            self.verify()

    def test_duplicate_cell(self):
        self.cells[-1]["cell"] = self.cells[0]["cell"]
        with self.assertRaisesRegex(AssertionError, "duplicate Cell"):
            self.verify()

    def test_no_live_renewal(self):
        self.samples[-1]["renewals"] = "0"
        with self.assertRaisesRegex(AssertionError, "lease renewal"):
            self.verify()

    def test_uncovered_seed(self):
        self.cells[-1]["root_sequence"] = "1"
        with self.assertRaisesRegex(AssertionError, "uncovered"):
            self.verify()

    def test_sqlite_handle_leak(self):
        self.drained[0]["sqlite_descriptors"] = "1"
        with self.assertRaisesRegex(AssertionError, "undrained"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
