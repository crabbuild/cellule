"""The driver must serialize work and preserve failures from either process."""

import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from coordination import coordinate
import routing


WORKER = """
import os, sys, time
for stage in sys.argv[1:]:
    print('RUSTFS gate ready=' + stage, flush=True)
    assert input() == 'start ' + stage
    lock = os.open(os.environ['ACTIVE_WINDOW'], os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    time.sleep(0.01)
    os.close(lock)
    os.unlink(os.environ['ACTIVE_WINDOW'])
    print('RUSTFS gate done=' + stage, flush=True)
    assert input() == 'next ' + stage
"""


class CoordinationTests(unittest.TestCase):
    def test_workers_use_distinct_providers_balanced_across_order(self):
        providers = ('http://127.0.0.1:9000', 'http://127.0.0.1:9002')
        assignments = routing.provider_assignments(providers)
        seen = []

        async def capture(workers, schedule, evidence):
            endpoints = {v: worker[1]['CELLULE_TEST_ENDPOINT'] for v, worker in workers.items()}
            self.assertEqual(set(endpoints.values()), set(providers))
            first, second = schedule[0][1]
            seen.extend((v, endpoints[v], v == first) for v in (first, second))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = {v: root / v for v in ('baseline', 'candidate')}
            # An inherited common endpoint must not override fixture isolation.
            with mock.patch.dict(os.environ, CELLULE_TEST_ENDPOINT='http://shared'), \
                    mock.patch.object(routing, 'coordinate', side_effect=capture), \
                    mock.patch.object(routing, 'parse_measurement', return_value=[]) as parsed:
                for pair, endpoints in enumerate(assignments):
                    routing.measure_pair('leased', pair, binaries, root, endpoints)
                self.assertEqual([call.args[-1] for call in parsed.call_args_list],
                                 [assignments[pair][v] for pair, order in enumerate(routing.PAIRS) for v in order])
        self.assertEqual(len(seen), 8)
        for version in binaries:
            for endpoint in providers:
                self.assertEqual(sorted(first for v, p, first in seen if (v, p) == (version, endpoint)),
                                 [False, True])

    def test_incomplete_or_shared_provider_assignment_is_rejected(self):
        for providers in ([], ['http://one'], ['http://one', 'http://one'],
                          ['http://one/', 'http://one'], ['http://one', 'http://two', 'http://three']):
            with self.subTest(providers=providers), self.assertRaisesRegex(RuntimeError, 'two distinct'):
                routing.provider_assignments(providers)

    def test_windows_do_not_overlap_and_order_reverses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ, ACTIVE_WINDOW=str(root / 'active'))
            workers = {v: ([sys.executable, '-u', '-c', WORKER, 'one', 'two'], env, root / f'{v}.log')
                       for v in ('baseline', 'candidate')}
            schedule = [('one', ('baseline', 'candidate')), ('two', ('candidate', 'baseline'))]
            trace = asyncio.run(coordinate(workers, schedule, root / 'trace.json'))
            self.assertEqual([r['order'] for r in trace], [s[1] for s in schedule])
            for record in trace:
                first, second = (record['windows'][v] for v in record['order'])
                self.assertLessEqual(max(first['ready_ns'], second['ready_ns']), first['start_ns'])
                self.assertLessEqual(first['done_ns'], second['start_ns'])
                self.assertLessEqual(second['start_ns'], second['done_ns'])
            self.assertFalse((root / 'active').exists())

    def test_child_failure_is_not_treated_as_a_completed_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ, ACTIVE_WINDOW=str(root / 'active'))
            workers = {
                'baseline': ([sys.executable, '-u', '-c', 'import sys; sys.exit(7)'], env, root / 'baseline.log'),
                'candidate': ([sys.executable, '-u', '-c', WORKER, 'one'], env, root / 'candidate.log'),
            }
            with self.assertRaisesRegex(RuntimeError, "received.*exit.*7"):
                asyncio.run(coordinate(workers, [('one', ('baseline', 'candidate'))], root / 'trace.json'))
            self.assertTrue((root / 'baseline.log').exists())
            self.assertTrue((root / 'candidate.log').exists())

    def test_full_schedule_preserves_workload_and_balances_order(self):
        first_versions = [order[0] for order in routing.PAIRS]
        self.assertEqual(first_versions.count('baseline'), first_versions.count('candidate'))
        for order in routing.PAIRS:
            stages = routing.schedule(order)
            names = [stage for stage, _ in stages]
            expected = {f'{lane}-c{c}' for lane, c in routing.EXPECTED_ROWS
                        if not lane.endswith('_expired_bursts')}
            self.assertTrue(expected.issubset(names))
            self.assertEqual(len(names), len(set(names)))
            for route in ('local', 'forwarded'):
                paced = [pair for stage, pair in stages if stage.startswith(f'{route}_paced-')]
                self.assertEqual(len(paced), routing.PACED_BURSTS)
                self.assertEqual(sum(pair[0] == 'baseline' for pair in paced), routing.PACED_BURSTS // 2)
            self.assertEqual(names[-2:], ['write_route_control', 'recovery'])


if __name__ == '__main__':
    unittest.main()
