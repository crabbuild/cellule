"""Measured role isolation must be effective, not merely requested."""
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'perf'))
import run as runner


class ResourceTests(unittest.TestCase):
    def test_historical_profile_preserves_limits_and_unpinned_cpus(self):
        with patch.object(runner, 'ARGS', SimpleNamespace(resource_profile='shared-vm')):
            self.assertEqual(runner.resource_args('owner'),
                             ['--cpus', '8', '--memory', '16g', '--memory-swap', '16g'])
            self.assertEqual(runner.resource_args('follower'), runner.resource_args('owner'))
            self.assertEqual(runner.resource_args('client'), ['--cpus', '4', '--memory', '4g'])

    def test_owner_and_support_cpu_sets_are_disjoint(self):
        with patch.object(runner, 'ARGS', SimpleNamespace(resource_profile='isolated-owner')):
            self.assertEqual(runner.resource_limits('owner'), (8, 16, '0-7'))
            for role in ['follower', 'provider', 'client']:
                self.assertEqual(runner.resource_limits(role)[2], '8-11')
                self.assertIn('--memory-swap', runner.resource_args(role))
            for host in [{'NCPU': 8, 'MemTotal': 24 * 1024**3},
                         {'NCPU': 12, 'MemTotal': 8 * 1024**3}]:
                with self.assertRaises(ValueError):
                    runner.validate_resource_host(host)
            runner.validate_resource_host({'NCPU': 12, 'MemTotal': 24 * 1024**3})

    def test_effective_cgroups_must_match_requested_owner_limits(self):
        config = {'NanoCpus': 8 * 10**9, 'Memory': 16 * 1024**3,
                  'MemorySwap': 16 * 1024**3, 'CpusetCpus': '0-7'}
        valid = f'0-7\n800000 100000\n{16 * 1024**3}\n0\n'
        with patch.object(runner, 'ARGS', SimpleNamespace(resource_profile='isolated-owner')):
            for actual in [valid, valid.replace('0-7', '0-11'),
                           valid.replace('800000', '400000'),
                           valid.replace(str(16 * 1024**3), 'max'),
                           valid.replace('\n0\n', '\n1073741824\n'), '']:
                responses = [SimpleNamespace(stdout=json.dumps([{'HostConfig': config}])),
                             SimpleNamespace(stdout=actual)]
                with self.subTest(actual=actual), patch.object(runner, 'docker', side_effect=responses):
                    if actual == valid:
                        self.assertEqual(runner.verify_resource_role('owner', 'owner')['role'], 'owner')
                    else:
                        with self.assertRaises(RuntimeError):
                            runner.verify_resource_role('owner', 'owner')

    def test_incorrect_container_configuration_fails_before_sampling(self):
        with patch.object(runner, 'ARGS', SimpleNamespace(resource_profile='isolated-owner')):
            with patch.object(runner, 'docker', return_value=SimpleNamespace(
                    stdout=json.dumps([{'HostConfig': {'CpusetCpus': '0-11'}}]))):
                with self.assertRaisesRegex(RuntimeError, 'configuration differs'):
                    runner.verify_resource_role('owner', 'owner')


if __name__ == '__main__':
    unittest.main()
