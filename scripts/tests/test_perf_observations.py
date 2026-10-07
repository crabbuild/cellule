"""Audits must retain every original request and fail duplicate ACK IDs."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / 'perf'))
from observations import collect


def observation(oid):
    return {'output': {'id': oid}, 'receipt': {'commit_sequence': oid},
            'request': {'request_id': str(oid), 'id': oid}}


class ObservationTests(unittest.TestCase):
    def write(self, directory, name, rows):
        path = directory / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        if path.parent.name == 'initialize':
            (path.parent / 'config.json').write_text(json.dumps({'cells': len(rows)}))
        else:
            successes = sum(row.get('error') is None for row in rows)
            (path.parent / 'summary.json').write_text(json.dumps({'writes': {
                'successes_including_drain': successes, 'warmup_attempts': 0, 'warmup_errors': 0}}))

    def row(self, oid):
        ack = observation(oid)
        return {'response': {key: value for key, value in ack.items() if key != 'request'},
                'request': ack['request'], 'error': None}

    def test_warmup_and_every_write_phase_enter_the_stream(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write(directory, 'initialize/setup.jsonl', [self.row(1)])
            self.write(directory, 'write-100/client-0.jsonl', [self.row(2), {'error': 'refused'}])
            self.write(directory, 'write-overload/client-0.jsonl', [self.row(3)])
            self.write(directory, 'write-recovery/client-0.jsonl', [self.row(4)])
            self.assertEqual(collect(directory, observation(0)), 5)
            rows = [json.loads(line) for line in (directory / 'acknowledged.jsonl').read_text().splitlines()]
            self.assertEqual({row['output']['id'] for row in rows}, set(range(5)))
            self.assertTrue(all(row['request']['request_id'] == str(row['output']['id']) for row in rows))
            self.assertFalse(list(directory.glob('ack-index-*')))
            manifest = json.loads((directory / 'acknowledged-manifest.json').read_text())
            self.assertEqual(manifest['checked_acks'], 5)
            self.assertEqual(len(manifest['sources']), 4)

    def test_duplicate_acknowledgements_and_existing_output_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write(directory, 'initialize/setup.jsonl', [self.row(1)])
            self.write(directory, 'write-100/client-0.jsonl', [self.row(1)])
            with self.assertRaisesRegex(ValueError, 'nonunique acknowledged id'):
                collect(directory, observation(0))
            self.assertFalse(list(directory.glob('ack-index-*')))
            with self.assertRaises(FileExistsError):
                collect(directory, observation(0))

    def test_missing_success_journal_cannot_reduce_the_audited_denominator(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write(directory, 'initialize/setup.jsonl', [self.row(1)])
            self.write(directory, 'write-100/client-0.jsonl', [self.row(2)])
            (directory / 'write-100/client-0.jsonl').unlink()
            with self.assertRaisesRegex(ValueError, 'ACK journal count differs from client totals'):
                collect(directory, observation(0))
            self.assertFalse((directory / 'acknowledged-manifest.json').exists())


if __name__ == '__main__':
    unittest.main()
