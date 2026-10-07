"""A comparison must retain failed points and reject mismatched workloads."""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'perf'))
import compare as comparison


def point():
    return {'offered_writes_per_second': 0, 'offered_reads_per_second': 10000,
            'successful_writes_per_second': 0, 'successful_reads_per_second': 10000,
            'writes': {'scheduled_latency_ms_all_attempts': {'p99': None}},
            'reads': {'scheduled_latency_ms_all_attempts': {'p99': 2}},
            'load_contract': {'hot_read_cells': None, 'write_offset': 0},
            'delivery_latency_audit_pass': True, 'failures': [],
            'publication_stability': {'pass': True}, 'window_cost': {'available': False}}


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.matrix = {role: [role] for role in ('baseline', 'candidate', 'celld')}
        self.arms = {}
        for role in self.matrix:
            case = dict.fromkeys(comparison.CONTRACT, 'same')
            case['system'] = 'celld' if role == 'celld' else 'cellule'
            build = {'binaries': {name: {'sha256': name} for name in ('sql', 'http_capacity', 'http_audit')},
                     'images': {'store': 'pinned'}, 'framework_source_manifest_sha256': role,
                     'measurement_overlay': role == 'baseline'}
            report = {'points': [point()], 'completed': True, 'failures': []}
            self.arms[role] = (case, build, report)

    def compare(self):
        with patch.object(comparison, 'summarize', side_effect=lambda path: self.arms[path.name]):
            return comparison.compare(self.matrix)

    def test_identical_read_point_does_not_claim_capacity_or_full_qualification(self):
        result = self.compare()
        guardrail = result['points'][0]['read_guardrail_at_matched_point']
        self.assertTrue(guardrail['pass'])
        self.assertFalse(guardrail['baseline_capacity_search_verified'])
        self.assertFalse(result['qualification_pass'])
        self.assertEqual(result['points'][0]['arms']['candidate']['read_rates'], [10000])

    def test_read_errors_latency_and_throughput_regressions_fail(self):
        baseline = point()
        for rate, p99, delivered in ((8999, 2, True), (10000, 2.401, True), (10000, 1, False)):
            with self.subTest(rate=rate, p99=p99, delivered=delivered):
                candidate = point()
                candidate['successful_reads_per_second'] = rate
                candidate['reads']['scheduled_latency_ms_all_attempts']['p99'] = p99
                candidate['delivery_latency_audit_pass'] = delivered
                self.assertFalse(comparison.read_guardrail([baseline], [candidate])['pass'])
        self.assertFalse(comparison.read_guardrail([baseline], [])['pass'])

    def test_hot_distribution_and_preceding_load_must_match(self):
        for key, value in (('hot_read_cells', 10), ('write_offset', 100000000)):
            with self.subTest(key=key):
                self.setUp()
                self.arms['candidate'][2]['points'][0]['load_contract'][key] = value
                with self.assertRaisesRegex(ValueError, 'workload or hot-read distribution mismatch'):
                    self.compare()

    def test_different_client_resources_or_provider_cannot_compare(self):
        for kind in ('client', 'resources', 'image'):
            with self.subTest(kind=kind):
                self.setUp()
                case, build, _ = self.arms['candidate']
                if kind == 'client':
                    build['binaries']['http_capacity']['sha256'] = 'different'
                elif kind == 'resources':
                    case['owner_cpus'] = 4
                else:
                    build['images']['store'] = 'different'
                with self.assertRaisesRegex(ValueError, 'mismatch'):
                    self.compare()

    def test_unmatched_offered_points_cannot_disappear_from_comparison(self):
        extra = copy.deepcopy(point())
        extra['offered_reads_per_second'] = 20000
        self.arms['candidate'][2]['points'].append(extra)
        with self.assertRaisesRegex(ValueError, 'no case may be silently omitted'):
            self.compare()

    def test_failed_case_and_point_remain_visible(self):
        report = self.arms['candidate'][2]
        report['completed'] = False
        report['failures'] = ['cold audit failed']
        report['points'][0]['delivery_latency_audit_pass'] = False
        report['points'][0]['failures'] = ['cold audit failed']
        result = self.compare()
        self.assertIn('cold audit failed', result['evidence'][1]['failures'])
        self.assertFalse(result['points'][0]['read_guardrail_at_matched_point']['pass'])


if __name__ == '__main__':
    unittest.main()
