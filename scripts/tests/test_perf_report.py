"""High completion rates and fast failed responses cannot pass write gates."""
import importlib.util
import unittest
import json
import tempfile
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("perf_report", Path(__file__).parents[1] / "perf/report.py")
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class DeliveryGateTests(unittest.TestCase):
    def test_inactive_fleet_cannot_qualify_with_zero_publication_debt(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            names = ['metrics-window-start.json', 'metrics-window-minute-1.json',
                     'metrics-window-minute-2.json', 'metrics-window-end.json']
            for index, name in enumerate(names):
                sample = [{'metrics': {'schema_version': 2, 'sample_session': 'owner',
                    'sample_elapsed_ns': index * 60 * 10**9,
                    'runtime': {'unpublished_node_log_bytes': 0},
                    'publication_progress': {'oldest_unpublished_ms': None,
                        'pending_publications': 0, 'retained_capture_bytes': 0},
                    'node_log_progress': {'fleet_active': False, 'fenced': False,
                        'rotating': False, 'tiered_through': 0,
                        'issued_through': 0, 'follower_proven_through': 0}}}]
                (directory / name).write_text(json.dumps(sample))
            self.assertTrue(REPORT.stability_report(directory)['pass'])
            self.assertFalse(REPORT.fleet_mode_report(directory)['pass'])

    def test_active_fleet_requires_healthy_frontiers_throughout_the_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            healthy = {'fleet_active': True, 'fenced': False, 'rotating': False}
            paths = [directory / f'metrics-window-{label}.json'
                     for label in ('start', 'minute-1', 'end')]
            for index, path in enumerate(paths):
                path.write_text(json.dumps([{'metrics': {'node_log_progress':
                    dict(healthy, follower_proven_through=index)}}]))
            self.assertTrue(REPORT.fleet_mode_report(directory)['pass'])
            paths[-1].write_text(json.dumps([{'metrics': {'node_log_progress':
                dict(healthy, follower_proven_through=0)}}]))
            self.assertFalse(REPORT.fleet_mode_report(directory)['pass'])
            paths[-1].write_text(json.dumps([{'metrics': {'node_log_progress':
                dict(healthy, follower_proven_through=2)}}]))
            for bad in (dict(healthy, fenced=True), dict(healthy, rotating=True),
                        dict(healthy, error='lease expired')):
                paths[1].write_text(json.dumps([{'metrics': {'node_log_progress': bad}}]))
                self.assertFalse(REPORT.fleet_mode_report(directory)['pass'])

    def test_missing_fleet_frontiers_cannot_qualify(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.assertFalse(REPORT.fleet_mode_report(directory)['available'])
            for label in ('start', 'end'):
                (directory / f'metrics-window-{label}.json').write_text('[{"metrics": {}}]')
            result = REPORT.fleet_mode_report(directory)
            self.assertFalse(result['available'])
            self.assertFalse(result['pass'])

    def test_provider_oom_after_measurement_is_retained_as_a_cold_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            healthy = {'Running': True, 'OOMKilled': False, 'Paused': False,
                       'Restarting': False, 'Dead': False}
            for label in ('startup', 'before', 'after', 'cold', 'final'):
                (directory / f'{label}-provider-state.json').write_text(json.dumps(healthy))
            self.assertTrue(REPORT.provider_lifecycle(directory, True)['pass'])
            (directory / 'final-provider-state.json').write_text(json.dumps(
                dict(healthy, Running=False, OOMKilled=True, ExitCode=137)))
            result = REPORT.provider_lifecycle(directory, True)
            self.assertFalse(result['pass'])
            self.assertEqual(result['failed_phases'], ['final'])
            self.assertTrue(result['states']['final']['OOMKilled'])

    def test_missing_or_incomplete_lifecycle_evidence_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            result = REPORT.provider_lifecycle(directory, True)
            self.assertFalse(result['available'])
            self.assertFalse(result['pass'])
            self.assertIn('cold', result['missing_phases'])
            (directory / 'final-provider-state.json').write_text('{"Running": true}')
            self.assertEqual(REPORT.provider_lifecycle(directory)['failed_phases'], ['final'])

    def test_provider_health_preserves_failed_and_missing_inode_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.assertFalse(REPORT.provider_health(directory)['pass'])
            sample = {'measurement_usable': True, 'startup_usable': True,
                      'free_bytes': 100 * 1024**3, 'inodes': {'free': 7000000}}
            for label in ('startup', 'before', 'after'):
                (directory / f'{label}-provider-filesystem.json').write_text(json.dumps(sample))
            journal = directory / 'docker-stats.jsonl'
            journal.write_text(json.dumps({'provider_filesystem': sample}) + '\n')
            self.assertTrue(REPORT.provider_health(directory)['pass'])
            journal.write_text(json.dumps({'provider_filesystem': dict(sample, measurement_usable=False, inodes={'free': 5})}) + '\n')
            result = REPORT.provider_health(directory)
            self.assertFalse(result['pass'])
            self.assertEqual(result['minimum_free_inodes'], 5)
            journal.write_text('{}\n')
            self.assertFalse(REPORT.provider_health(directory)['pass'])

    def test_all_ack_count_includes_warmup_late_overload_and_recovery_successes(self):
        def phase(steady, warm, errors):
            return {'writes': {'successes_including_drain': steady,
                    'warmup_attempts': warm, 'warmup_errors': errors}}
        summary = {'writes': [phase(30000, 3000, 0)],
                   'overload': {'overload': phase(900, 0, 0), 'recovery': phase(200, 0, 0)}}
        self.assertEqual(REPORT.expected_acknowledgements({'resident_cells': 1000}, summary), 35101)
        summary['writes'][0]['writes']['warmup_errors'] = 3001
        with self.assertRaisesRegex(ValueError, 'invalid acknowledgement counters'):
            REPORT.expected_acknowledgements({'resident_cells': 1000}, summary)

    def point(self):
        return {"driver_exit_code": 0, "writes": {
            "errors": 0, "warmup_errors": 0, "queue_dropped": 0, "warmup_queue_dropped": 0,
            "generated_offers": 4500000, "planned_offers": 4500000,
            "successes_in_window": 4500000, "scheduled_latency_ms_all_attempts": {"p99": 10}}}

    def test_fast_error_responses_fail_even_when_latency_passes(self):
        point = self.point()
        point["writes"]["errors"] = 100
        self.assertIn("errors", REPORT.delivery_failures(point, "fleet"))

    def test_dropped_or_unissued_offers_cannot_be_hidden_by_successful_tps(self):
        for field in ("queue_dropped", "warmup_queue_dropped"):
            point = self.point()
            point["writes"][field] = 1
            self.assertIn(field, REPORT.delivery_failures(point, "fleet"))
        point = self.point()
        point["writes"]["generated_offers"] -= 1
        self.assertIn("unissued offers", REPORT.delivery_failures(point, "fleet"))

    def test_cumulative_histogram_reset_refuses_a_window(self):
        with self.assertRaises(ValueError):
            REPORT.subtract({"buckets": [3, 2]}, {"buckets": [3, 1]})

    def test_resolution_change_cannot_be_interpreted_as_a_latency_improvement(self):
        before = {'resolution_us': 100, 'total_ns': 100000, 'buckets': [0, 1, 0]}
        after = {'resolution_us': 10, 'total_ns': 200000, 'buckets': [0, 2, 0]}
        with self.assertRaisesRegex(ValueError, 'resolution changed'):
            REPORT.histogram_delta(before, after)

    def test_sparse_histogram_preserves_new_buckets_and_rejects_resets(self):
        before = {'bucket_count': 5, 'nonzero_buckets': [[1, 3], [4, 1]]}
        after = {'bucket_count': 5, 'nonzero_buckets': [[1, 4], [2, 2], [4, 1]]}
        self.assertEqual(REPORT.subtract(REPORT.histogram_buckets(before),
                                        REPORT.histogram_buckets(after)), [0, 1, 2, 0, 0])
        with self.assertRaises(ValueError):
            REPORT.histogram_buckets({'bucket_count': 5, 'nonzero_buckets': [[1, 3], [1, 2]]})
        with self.assertRaises(ValueError):
            REPORT.subtract(REPORT.histogram_buckets(before),
                            REPORT.histogram_buckets({'bucket_count': 5, 'nonzero_buckets': [[4, 1]]}))

    def test_read_errors_fail_a_mixed_point(self):
        point = self.point()
        point["reads"] = dict(point["writes"], errors=1)
        self.assertIn("reads: errors", REPORT.delivery_failures(point, "fleet"))

    def test_range_reads_and_multipart_work_are_not_hidden_by_full_get_put_counts(self):
        def operation(count):
            return {'started': count, 'outcomes': {'success': count},
                    'bytes_read': 0, 'bytes_written': 0}
        metrics = {'available': True, 'endpoints': [{
            'publication': {'selected_roots': 2, 'materialized_commits': 2},
            'storage': {'get': operation(2), 'range': operation(3), 'put': operation(4),
                        'multipart_part': operation(3)},
            'storage_families': {'immutable': {'get': operation(2), 'range': operation(3),
                                              'put': operation(4)}}}]}
        cost = REPORT.cost_report(metrics, 2)
        self.assertEqual(cost['get_attempts_including_ranges_per_command'], 2.5)
        self.assertEqual(cost['mutation_request_successes_per_command'], 3.5)
        self.assertEqual(cost['families']['immutable']['get_attempts'], 5)

    def test_positive_publication_age_or_debt_trend_cannot_pass_stability(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            names = ['metrics-window-start.json', 'metrics-window-minute-1.json',
                     'metrics-window-minute-2.json', 'metrics-window-end.json']
            for index, name in enumerate(names):
                sample = [{'metrics': {'schema_version': 2, 'sample_session': 'owner',
                    'sample_elapsed_ns': index * 60 * 10**9,
                    'runtime': {'unpublished_node_log_bytes': 1000 + index},
                    'publication_progress': {'oldest_unpublished_ms': 10,
                        'pending_publications': 1, 'retained_capture_bytes': 1000}}}]
                (directory / name).write_text(json.dumps(sample))
            result = REPORT.stability_report(directory)
            self.assertTrue(result['available'])
            self.assertFalse(result['pass'])
            self.assertGreater(result['slopes_per_second']['unpublished_node_log_bytes'], 0)
            for name in names:
                path = directory / name
                sample = json.loads(path.read_text())
                sample[0]['metrics']['runtime']['unpublished_node_log_bytes'] = 0
                sample[0]['metrics']['publication_progress']['oldest_unpublished_ms'] = None
                path.write_text(json.dumps(sample))
            self.assertTrue(REPORT.stability_report(directory)['pass'])
            (directory / names[1]).unlink()
            self.assertFalse(REPORT.stability_report(directory)['pass'])

    def test_window_reconciles_actual_storage_schema_and_preserves_resolution(self):
        def sample(count):
            operation = {"started": count, "outcomes": {"success": count},
                         "bytes_read": 0, "bytes_written": count * 64}
            return [{"url": "http://127.0.0.1:8080/debug/metrics",
                     "request_started_ms": count, "request_finished_ms": count + 1,
                     "metrics": {"schema_version": 1, "sample_session": "fixed",
                                 "sample_elapsed_ns": count * 1000,
                                 "storage_families": {"immutable": {"put": operation}},
                                 "storage_operations": {"put": operation},
                                 "shared_publication": {"cohorts": count,
                                     "queue": {"mean_ms": 10 / count, "p99_ms": 20 / count}},
                                 "histograms": {"worker": {"resolution_us": 100,
                                     "total_ns": count * 200000, "buckets": [0, 0, count]}},
                                 "writes": {"selected_roots": count,
                                     "materialized_commits": count * 2,
                                     "publication_failures": 0,
                                     "uploaded_objects": count, "uploaded_bytes": count * 64}}}]
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "metrics-window-start.json").write_text(json.dumps(sample(1)))
            (directory / "metrics-window-end.json").write_text(json.dumps(sample(5)))
            window = REPORT.metric_delta(directory)
            endpoint = window["endpoints"][0]
            self.assertEqual(endpoint["histograms"]["worker"]["resolution_us"], 100)
            self.assertEqual(endpoint["histograms"]["worker"]["buckets"], [0, 0, 4])
            self.assertEqual(endpoint["publication"]["materialized_commits"], 8)
            self.assertEqual(endpoint["shared_publication"], {"cohorts": 4})
            bad = sample(5)
            bad[0]["metrics"]["storage_operations"]["put"] = dict(
                bad[0]["metrics"]["storage_operations"]["put"], bytes_written=999)
            (directory / "metrics-window-end.json").write_text(json.dumps(bad))
            with self.assertRaisesRegex(ValueError, "unclassified"):
                REPORT.metric_delta(directory)


if __name__ == "__main__":
    unittest.main()
