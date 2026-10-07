"""Compare retained cases without treating overload completions as capacity."""
import argparse
import hashlib
import json
from pathlib import Path

from report import case_report, read


CONTRACT = ('durability', 'resident_cells', 'concurrency', 'queue_capacity',
            'retained_budget_bytes', 'managed_disk_budget_bytes', 'value_bytes',
            'owner_cpus', 'owner_memory_bytes', 'followers', 'profile',
            'provider_storage', 'seconds', 'warmup_seconds')


def summarize(directory):
    case = read(directory / 'case.json')
    build_path = directory.parent / 'build.json'
    build = read(build_path)
    report = case_report(directory)
    if hashlib.sha256(build_path.read_bytes()).hexdigest() != report['build_manifest_sha256']:
        raise ValueError('build manifest changed after the run: ' + str(directory))
    return case, build, report


def compare(matrix):
    if set(matrix) != {'baseline', 'candidate', 'celld'}:
        raise ValueError('matrix needs baseline, candidate and celld case lists')
    rows = {}
    contract = binaries = images = None
    evidence = []
    for role, directories in matrix.items():
        if not directories:
            raise ValueError('missing cases for ' + role)
        rows[role] = []
        for name in directories:
            directory = Path(name).resolve()
            case, build, report = summarize(directory)
            if case['system'] != ('celld' if role == 'celld' else 'cellule'):
                raise ValueError('wrong system for ' + role)
            identity = {key: case[key] for key in CONTRACT}
            driver = {key: build['binaries'][key]['sha256']
                      for key in ('http_capacity', 'http_audit')}
            if contract is None:
                contract, binaries, images = identity, driver, build['images']
            if identity != contract or driver != binaries or build['images'] != images:
                raise ValueError('workload, resources, client or image mismatch: ' + str(directory))
            rows[role].append(report)
            evidence.append({'role': role, 'directory': str(directory),
                             'source_manifest_sha256': build['framework_source_manifest_sha256'],
                             'binary_sha256': build['binaries']['sql']['sha256'],
                             'measurement_overlay': bool(build['measurement_overlay']),
                             'completed': report['completed'], 'failures': report['failures']})
    points = {}
    for role, reports in rows.items():
        by_offer = {}
        for report in reports:
            for point in report['points']:
                key = (point['offered_writes_per_second'], point['offered_reads_per_second'])
                by_offer.setdefault(key, []).append(point)
        points[role] = by_offer
    common = set(points['baseline']) & set(points['candidate']) & set(points['celld'])
    if not common:
        raise ValueError('no identical offered write/read points')
    comparisons = []
    for key in sorted(common):
        arms = {}
        for role in rows:
            samples = points[role][key]
            arms[role] = {
                'repetitions': len(samples),
                'rates': [point['successful_writes_per_second'] for point in samples],
                'scheduled_p99_ms': [point['writes']['scheduled_latency_ms_all_attempts']['p99'] for point in samples],
                'delivery_latency_audit_pass': all(point['delivery_latency_audit_pass'] for point in samples),
                'failures': [point['failures'] for point in samples],
                'publication_stability': [point['publication_stability'] for point in samples],
                'provider_cost': [point['window_cost'] for point in samples],
            }
        comparisons.append({'offered_writes_per_second': key[0],
                            'offered_reads_per_second': key[1], 'arms': arms})
    return {'schema_version': 1, 'workload': 'sql-ledger-96', 'contract': contract,
            'evidence': evidence, 'points': comparisons,
            'qualification_pass': False,
            'unverified': ['A/A sustainable-capacity search', 'read-only and mixed capacity guardrails',
                           'safe overload refusals and recovery', 'owner loss before materialization',
                           'bounded KV workload', 'complete M0–M5 exit gates']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('matrix', type=Path, help='JSON with three lists of external case directories')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = compare(read(args.matrix))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'report': str(args.output), 'qualification_pass': False,
                      'matched_points': len(result['points'])}))
