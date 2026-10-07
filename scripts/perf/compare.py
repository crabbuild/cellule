"""Compare retained cases without treating overload completions as capacity."""
import argparse
import hashlib
import json
from pathlib import Path

from report import case_report, read


CONTRACT = ('durability', 'resident_cells', 'concurrency', 'queue_capacity',
            'retained_budget_bytes', 'managed_disk_budget_bytes', 'value_bytes',
            'owner_cpus', 'owner_memory_bytes', 'followers', 'profile',
            'provider_storage', 'seconds', 'warmup_seconds', 'telemetry')


def summarize(directory):
    case = read(directory / 'case.json')
    build_path = directory.parent / 'build.json'
    build = read(build_path)
    report = case_report(directory)
    if hashlib.sha256(build_path.read_bytes()).hexdigest() != report['build_manifest_sha256']:
        raise ValueError('build manifest changed after the run: ' + str(directory))
    if build.get('acknowledgement_format') == 'jsonl-v1':
        if not case.get('docker_host') or not case.get('runner_sha256'):
            raise ValueError('bounded-audit case lacks host or runner provenance')
        if hashlib.sha256((directory / 'runner.py').read_bytes()).hexdigest() != case['runner_sha256']:
            raise ValueError('runner source changed after the run: ' + str(directory))
    return case, build, report


def read_guardrail(baseline, candidate):
    """A matched point is not evidence of either arm's highest read capacity."""
    if not baseline or len(baseline) != len(candidate):
        return {'available': False, 'pass': False, 'reason': 'matched repeats missing'}
    results = []
    for before, after in zip(baseline, candidate):
        rate = before['successful_reads_per_second']
        p99 = before['reads']['scheduled_latency_ms_all_attempts']['p99']
        new_p99 = after['reads']['scheduled_latency_ms_all_attempts']['p99']
        valid = rate > 0 and p99 is not None and p99 > 0 and new_p99 is not None
        ratio_rate = after['successful_reads_per_second'] / rate if valid else None
        ratio_p99 = new_p99 / p99 if valid else None
        passed = bool(valid and before['delivery_latency_audit_pass']
                      and after['delivery_latency_audit_pass']
                      and ratio_rate >= 0.90 and ratio_p99 <= 1.20)
        results.append({'pass': passed, 'throughput_ratio': ratio_rate,
                        'scheduled_p99_ratio': ratio_p99})
    return {'available': True, 'pass': all(result['pass'] for result in results),
            'repetitions': results, 'baseline_capacity_search_verified': False}


def compare(matrix):
    if set(matrix) != {'baseline', 'candidate', 'celld'}:
        raise ValueError('matrix needs baseline, candidate and celld case lists')
    rows = {}
    contract = binaries = images = fixtures = execution = None
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
            # Historical array-audit cases lack the complete execution record.
            # Preserve them as evidence, but never label that provenance verified.
            current = build.get('acknowledgement_format') == 'jsonl-v1'
            environment = {'docker_host': case.get('docker_host'),
                           'runner_sha256': case.get('runner_sha256')} if current else None
            driver = {key: build['binaries'][key]['sha256']
                      for key in ('http_capacity', 'http_audit')}
            if contract is None:
                contract, binaries, images, fixtures = identity, driver, build['images'], build.get('fixture_sources')
                execution = environment
            if (identity != contract or driver != binaries or build['images'] != images
                    or build.get('fixture_sources') != fixtures or environment != execution):
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
    common = set(points['baseline'])
    if not common or any(set(arm) != common for arm in points.values()):
        raise ValueError('offered point sets differ; no case may be silently omitted')
    comparisons = []
    for key in sorted(common):
        arms = {}
        contracts = [sample['load_contract'] for role in rows for sample in points[role][key]]
        if any(contract != contracts[0] for contract in contracts):
            raise ValueError('point workload or hot-read distribution mismatch: ' + str(key))
        for role in rows:
            samples = points[role][key]
            arms[role] = {
                'repetitions': len(samples),
                'rates': [point['successful_writes_per_second'] for point in samples],
                'scheduled_p99_ms': [point['writes']['scheduled_latency_ms_all_attempts']['p99'] for point in samples],
                'read_rates': [point['successful_reads_per_second'] for point in samples],
                'read_scheduled_p99_ms': [point['reads']['scheduled_latency_ms_all_attempts']['p99'] for point in samples],
                'delivery_latency_audit_pass': all(point['delivery_latency_audit_pass'] for point in samples),
                'failures': [point['failures'] for point in samples],
                'publication_stability': [point['publication_stability'] for point in samples],
                'fleet_mode': [point.get('fleet_mode') for point in samples],
                'provider_cost': [point['window_cost'] for point in samples],
            }
        comparisons.append({'offered_writes_per_second': key[0],
                            'offered_reads_per_second': key[1], 'load_contract': contracts[0],
                            'read_guardrail_at_matched_point': read_guardrail(
                                points['baseline'][key], points['candidate'][key]) if key[1] else None,
                            'arms': arms})
    return {'schema_version': 1, 'workload': 'sql-ledger-96', 'contract': contract,
            'fixture_provenance_verified': fixtures is not None,
            'host_and_runner_provenance_verified': execution is not None,
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
