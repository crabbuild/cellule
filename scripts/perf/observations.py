"""Stream every ACK to disk with a disk-backed uniqueness check."""
import json
import hashlib
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


def collect(directory, extra):
    directory = Path(directory)
    initialize = directory / 'initialize'
    expected_seeds = json.loads((initialize / 'config.json').read_text())['cells']
    groups = [(initialize, [initialize / 'setup.jsonl'], expected_seeds)]
    for phase in sorted(directory.glob('write-*')):
        if not phase.is_dir():
            continue
        writes = json.loads((phase / 'summary.json').read_text())['writes']
        expected = writes['successes_including_drain'] + writes['warmup_attempts'] - writes['warmup_errors']
        groups.append((phase, sorted(phase.glob('client-*.jsonl')), expected))
    sources = []
    count = 0
    with tempfile.TemporaryDirectory(prefix='ack-index-', dir=directory) as scratch:
        with closing(sqlite3.connect(Path(scratch) / 'ids.sqlite')) as index, index:
            # This index is disposable audit work, never a durability proof.
            # Keep its cache bounded; a multi-million-command run cannot retain
            # every output and request identity in a Python dictionary.
            index.execute('PRAGMA cache_size=-2048')
            index.execute('CREATE TABLE seen(id INTEGER PRIMARY KEY)')
            with (directory / 'acknowledged.jsonl').open('x') as output:
                def append(observation):
                    nonlocal count
                    oid = observation['output']['id']
                    if type(oid) is not int or not isinstance(observation['request'], dict):
                        raise ValueError('ACK requires integer id and original request')
                    try:
                        index.execute('INSERT INTO seen VALUES (?)', (oid,))
                    except sqlite3.IntegrityError as error:
                        raise ValueError(f'nonunique acknowledged id {oid}') from error
                    output.write(json.dumps(observation, separators=(',', ':')) + '\n')
                    count += 1
                    if count % 10000 == 0:
                        index.commit()
                append(extra)
                for phase, paths, expected in groups:
                    before = count
                    for path in paths:
                        digest = hashlib.sha256()
                        with path.open('rb') as source:
                            for line in source:
                                digest.update(line)
                                row = json.loads(line)
                                if row.get('error') is None and row.get('response') and row.get('request'):
                                    append(dict(row['response'], request=row['request']))
                        sources.append({'path': str(path.relative_to(directory)), 'sha256': digest.hexdigest()})
                    if count - before != expected:
                        raise ValueError(f'ACK journal count differs from client totals in {phase.name}: '
                                         f'{count - before} != {expected}')
    with (directory / 'acknowledged.jsonl').open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    (directory / 'acknowledged-manifest.json').write_text(json.dumps(
        {'schema_version': 1, 'format': 'jsonl', 'checked_acks': count, 'sha256': digest,
         'sources': sources}, indent=2) + '\n')
    return count
