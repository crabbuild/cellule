"""The performance gate must refuse regressions and incomplete comparisons."""

import copy
import unittest

import routing


def measurements(unleased_reads=4096):
    return [dict(mode=mode, version=version, run=run, lane="local_query", concurrency=16,
                 throughput=1000.0, p50_ms=1.0, p95_ms=2.0, p99_ms=3.0,
                 reads=0 if mode == "leased" else unleased_reads)
            for mode in routing.SELECTORS for version in ("baseline", "candidate") for run in range(3)]


class GateTests(unittest.TestCase):
    def test_identical_complete_comparison_passes(self):
        self.assertEqual(routing.compare(measurements())["failures"], [])

    def test_each_latency_and_throughput_regression_fails(self):
        for metric, value in (("throughput", 899), ("p95_ms", 2.21), ("p99_ms", 3.31)):
            with self.subTest(metric=metric):
                rows = measurements()
                for row in rows:
                    if row["mode"] == "leased" and row["version"] == "candidate":
                        row[metric] = value
                self.assertEqual(routing.compare(rows)["failures"], ["leased/local_query/c16"])

    def test_missing_repeat_is_rejected(self):
        rows = measurements()
        rows.pop()
        with self.assertRaisesRegex(RuntimeError, "Incomplete repeats"):
            routing.compare(rows)

    def test_cold_forwarded_discovery_regression_fails(self):
        rows = measurements()
        for row in rows:
            row["lane"] = "forwarded_query_uncached_route"
            if row["mode"] == "leased" and row["version"] == "candidate":
                row["p99_ms"] = 4.0
        self.assertEqual(routing.compare(rows)["failures"], ["leased/forwarded_query_uncached_route/c16"])

    def test_one_outlier_cannot_hide_two_regressed_runs(self):
        rows = measurements()
        for row in rows:
            if row["mode"] == "leased" and row["version"] == "candidate":
                row["p99_ms"] = 1.0 if row["run"] == 0 else 4.0
        self.assertEqual(routing.compare(rows)["failures"], ["leased/local_query/c16"])

    def test_historical_zero_read_unleased_path_is_not_latency_target(self):
        rows = measurements(unleased_reads=0)
        for row in rows:
            if row["mode"] == "object_only" and row["version"] == "candidate":
                row["p95_ms"] = row["p99_ms"] = 100
                row["reads"] = 4096
        result = routing.compare(rows)
        self.assertEqual(result["failures"], [])
        self.assertFalse(next(row for row in result["comparisons"]
                              if row["mode"] == "object_only")["latency_gate"])
        # The same regression against fresh authority must fail qualification.
        fresh = copy.deepcopy(rows)
        for row in fresh:
            if row["mode"] == "object_only" and row["version"] == "baseline":
                row["reads"] = 4096
        self.assertEqual(routing.compare(fresh)["failures"], ["object_only/local_query/c16"])


if __name__ == "__main__":
    unittest.main()
