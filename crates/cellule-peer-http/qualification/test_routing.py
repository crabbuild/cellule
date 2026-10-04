"""The performance gate must refuse regressions and incomplete comparisons."""

import copy
import unittest

import routing


def measurements(unleased_reads=4096):
    return [dict(mode=mode, version=version, run=run, lane="local_query", concurrency=16,
                 throughput=1000.0, p50_ms=1.0, p95_ms=2.0, p99_ms=3.0,
                 reads=0 if mode == "leased" else unleased_reads)
            for mode in routing.SELECTORS for version in ("baseline", "candidate") for run in range(len(routing.PAIRS))]


class GateTests(unittest.TestCase):
    @staticmethod
    def publication(first, last):
        return dict(first_sequence=first, sequence=last, queue_wait_ns=0,
                    preparation_ns=10, authority_ns=20, total_ns=30)

    def test_grouped_and_single_command_roots_cover_the_same_workload(self):
        for first in (1, 1025, 2049, 3073):
            last = first + routing.COMMANDS - 1
            for group in (1, 4):
                with self.subTest(first=first, group=group):
                    roots = [self.publication(start, min(start + group - 1, last))
                             for start in range(first, last + 1, group)]
                    routing.verify_publications(roots, first, last)

    def test_publication_coverage_rejects_gaps_overlap_missing_tail_and_wrong_lane(self):
        valid = [self.publication(1025, 1028), self.publication(1029, 1032)]
        invalid = [[], valid[:-1], valid[::-1], valid + [valid[-1]],
                   [self.publication(1025, 1028), self.publication(1030, 1032)],
                   [self.publication(1025, 1028), self.publication(1028, 1032)],
                   [self.publication(1025, 1033)], [self.publication(1024, 1032)],
                   [self.publication(1025, 1024)]]
        for roots in invalid:
            with self.subTest(roots=roots), self.assertRaisesRegex(RuntimeError, "Incomplete publication evidence"):
                routing.verify_publications(roots, 1025, 1032)

    def test_publication_coverage_requires_every_nonnegative_phase(self):
        valid = self.publication(1, 4)
        for field in valid:
            for change in ("missing", "negative", "non_integer"):
                invalid = dict(valid)
                if change == "missing":
                    invalid.pop(field)
                else:
                    invalid[field] = -1 if change == "negative" else 1.5
                with self.subTest(field=field, change=change), self.assertRaisesRegex(RuntimeError, "Incomplete publication evidence"):
                    routing.verify_publications([invalid], 1, 4)

    def test_publication_summary_retains_physical_counts_and_phase_means(self):
        roots = [self.publication(1, 1), self.publication(2, 5)]
        summary = dict(calls=5, publications=2, preparation_mean_ms=0.00001,
                       authority_mean_ms=0.00002, total_mean_ms=0.00003)
        routing.verify_publication_summary(summary, roots, 5)
        for field in summary:
            invalid = dict(summary)
            invalid[field] += 1
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "Publication summary"):
                routing.verify_publication_summary(invalid, roots, 5)

    def test_identical_complete_comparison_passes(self):
        self.assertEqual(routing.compare(measurements())["failures"], [])

    def test_each_latency_and_throughput_regression_fails(self):
        for metric, value in (("throughput", 899), ("p95_ms", 3.01), ("p99_ms", 6.01)):
            with self.subTest(metric=metric):
                rows = measurements()
                for row in rows:
                    if row["mode"] == "leased" and row["version"] == "candidate":
                        row[metric] = value
                self.assertEqual(routing.compare(rows)["failures"], ["leased/local_query/c16"])

    def test_latency_limits_are_inclusive_and_keep_review_alerts(self):
        rows = measurements()
        for row in rows:
            if row["mode"] == "leased" and row["version"] == "candidate":
                row["p95_ms"] = 3.0
                row["p99_ms"] = 6.0
                row["throughput"] = 900.0
        result = routing.compare(rows)
        self.assertEqual(result["failures"], [])
        self.assertEqual(result["latency_alerts"], ["leased/local_query/c16"])
        self.assertEqual(result["gate_limits"],
                         dict(p95_max_ratio=1.5, p99_max_ratio=2.0, throughput_min_ratio=0.9))
        self.assertEqual(result["latency_alert_ratio"], 1.1)

    def test_old_latency_exceedances_remain_visible(self):
        rows = measurements()
        for row in rows:
            if row["mode"] == "object_only" and row["version"] == "candidate":
                row["p95_ms"] = 2.21
                row["p99_ms"] = 3.31
        result = routing.compare(rows)
        self.assertEqual(result["failures"], [])
        self.assertEqual(result["latency_alerts"], ["object_only/local_query/c16"])

    def test_missing_repeat_is_rejected(self):
        rows = measurements()
        rows.pop()
        with self.assertRaisesRegex(RuntimeError, "Incomplete repeats"):
            routing.compare(rows)

    def test_single_mode_requires_explicit_selection(self):
        rows = [row for row in measurements() if row["mode"] == "leased"]
        with self.assertRaisesRegex(RuntimeError, "Incomplete routing modes"):
            routing.compare(rows)

    def test_each_partition_retains_repeats_and_regression_gates(self):
        for mode in routing.SELECTORS:
            with self.subTest(mode=mode):
                rows = [row for row in measurements() if row["mode"] == mode]
                self.assertEqual(routing.compare(rows, (mode,))["failures"], [])
                for metric, value in (("throughput", 899), ("p95_ms", 3.01), ("p99_ms", 6.01)):
                    regressed = copy.deepcopy(rows)
                    for row in regressed:
                        if row["version"] == "candidate":
                            row[metric] = value
                    self.assertEqual(routing.compare(regressed, (mode,))["failures"],
                                     [f"{mode}/local_query/c16"])
                with self.assertRaisesRegex(RuntimeError, "Incomplete repeats"):
                    routing.compare(rows[:-1], (mode,))

    def test_partition_rejects_unexpected_or_invalid_modes(self):
        with self.assertRaisesRegex(RuntimeError, "Incomplete routing modes"):
            routing.compare(measurements(), ("leased",))
        for modes in ((), ("unknown",)):
            with self.assertRaisesRegex(RuntimeError, "Invalid routing modes"):
                routing.compare(measurements(), modes)

    def test_cold_forwarded_discovery_regression_fails(self):
        rows = measurements()
        for row in rows:
            row["lane"] = "forwarded_query_uncached_route"
            if row["mode"] == "leased" and row["version"] == "candidate":
                row["p99_ms"] = 6.01
        self.assertEqual(routing.compare(rows)["failures"], ["leased/forwarded_query_uncached_route/c16"])

    def test_one_outlier_cannot_hide_a_regressed_majority(self):
        rows = measurements()
        for row in rows:
            if row["mode"] == "leased" and row["version"] == "candidate":
                row["p99_ms"] = 1.0 if row["run"] == 0 else 6.01
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
