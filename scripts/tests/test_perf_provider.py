"""A filesystem with free bytes can still fail through exhausted inodes."""
import sys
import unittest
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'perf'))
import run as runner


class ProviderTests(unittest.TestCase):
    def test_provider_oom_snapshot_is_written_before_the_case_is_rejected(self):
        state = {'Running': False, 'OOMKilled': True, 'ExitCode': 137,
                 'Paused': False, 'Restarting': False, 'Dead': False}
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch.object(runner, 'docker', return_value=SimpleNamespace(
                    stdout=json.dumps([{'State': state}]))):
                with self.assertRaisesRegex(RuntimeError, 'provider lifecycle failed'):
                    runner.record_provider_state(directory, 'final')
            self.assertEqual(json.loads((directory / 'final-provider-state.json').read_text()), state)

    def evidence(self, free=7864324, byte_blocks=164221168):
        return ('Filesystem 1024-blocks Used Available Capacity Mounted on\n'
                f'/dev/vdb1 205838168 32373416 {byte_blocks} 17% /data\n'
                'Filesystem Inodes IUsed IFree IUse% Mounted on\n'
                f'/dev/vdb1 13107200 5242876 {free} 40% /data\n')

    def test_free_bytes_do_not_hide_inode_exhaustion(self):
        result = runner.parse_provider_filesystem(self.evidence(free=5))
        self.assertGreater(result['free_bytes'], 100 * 1024**3)
        self.assertFalse(result['measurement_usable'])
        self.assertFalse(result['startup_usable'])

    def test_startup_requires_headroom_beyond_the_measurement_floor(self):
        result = runner.parse_provider_filesystem(self.evidence(free=500000))
        self.assertTrue(result['measurement_usable'])
        self.assertFalse(result['startup_usable'])
        self.assertTrue(runner.parse_provider_filesystem(self.evidence())['startup_usable'])
        self.assertFalse(runner.parse_provider_filesystem(self.evidence(byte_blocks=1))['measurement_usable'])

    def test_missing_changed_or_invalid_mount_evidence_fails(self):
        for evidence in ('', self.evidence().replace('/data', '/other'),
                         self.evidence().replace('5242876', '-1'),
                         self.evidence().replace('/dev/vdb1 13107200', '/dev/vdc1 13107200')):
            with self.subTest(evidence=evidence):
                with self.assertRaises(ValueError):
                    runner.parse_provider_filesystem(evidence)


if __name__ == '__main__':
    unittest.main()
