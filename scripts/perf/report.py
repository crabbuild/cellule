"""Produce explicit qualification failures, measured rates and window cost deltas."""
import argparse
import hashlib
import json
from pathlib import Path

REPORTER_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def provider_health(directory):
    paths = [directory / f'{label}-provider-filesystem.json' for label in ('startup', 'before', 'after')]
    if any(not path.exists() for path in paths):
        return {'available': False, 'pass': False, 'reason': 'provider byte/inode observations missing'}
    samples = [read(path) for path in paths]
    journal = directory / 'docker-stats.jsonl'
    if not journal.exists():
        return {'available': False, 'pass': False, 'reason': 'provider sampling journal missing'}
    with journal.open() as stream:
        for line in stream:
            samples.append(json.loads(line).get('provider_filesystem', {'error': 'missing filesystem sample'}))
    valid = all(sample.get('measurement_usable') is True for sample in samples)
    valid = valid and samples[0].get('startup_usable') is True
    return {'available': True, 'pass': valid, 'sample_count': len(samples),
            'minimum_free_bytes': min((sample.get('free_bytes', 0) for sample in samples)),
            'minimum_free_inodes': min((sample.get('inodes', {}).get('free', 0) for sample in samples)),
            'reason': None if valid else 'provider capacity or filesystem evidence failed'}

def provider_lifecycle(directory, required=False):
    """Filesystem headroom does not prove the provider survived cold restore."""
    labels = ('startup', 'before', 'after', 'cold', 'final')
    paths = [directory / f'{label}-provider-state.json' for label in labels]
    states = {label: read(path) for label, path in zip(labels, paths) if path.exists()}
    failed = [label for label, state in states.items()
              if state.get('Running') is not True or state.get('OOMKilled') is not False
              or any(state.get(flag) is not False for flag in ('Paused', 'Restarting', 'Dead'))]
    missing = [label for label in labels if label not in states]
    return {'available': not missing, 'pass': not failed and not missing,
            'required': required, 'failed_phases': failed, 'missing_phases': missing,
            'states': states, 'reason': 'provider exited, OOM or unavailable' if failed
            else 'provider lifecycle observations missing' if missing else None}

def expected_acknowledgements(case, summary):
    phases = list(summary.get('writes', []))
    phases.extend(summary.get('overload', {}).get(name) for name in ('overload', 'recovery'))
    total = case['resident_cells'] + 1  # Every seed plus the contract mutation.
    for phase in phases:
        if phase is None:
            continue
        writes = phase['writes']
        successes, attempts, errors = (writes[key] for key in (
            'successes_including_drain', 'warmup_attempts', 'warmup_errors'))
        if any(type(value) is not int for value in (successes, attempts, errors)) or not 0 <= errors <= attempts or successes < 0:
            raise ValueError('invalid acknowledgement counters')
        total += successes + attempts - errors
    return total

def subtract(before, after):
    if isinstance(after, dict):
        return {key: subtract(before[key], value) for key, value in after.items() if key in before and (isinstance(value, (dict, list)) or isinstance(value, (int, float)))}
    if isinstance(after, list):
        if len(before) != len(after):
            raise ValueError('metric bucket shape changed')
        return [subtract(a, b) for a, b in zip(before, after)]
    delta = after - before
    if delta < 0:
        raise ValueError('monotonic metric reset during the measurement window')
    return delta

def delivery_failures(point, durability):
    failures = []
    if point.get('driver_exit_code') != 0:
        failures.append('driver failed')
    for kind in ('writes', 'reads'):
        if kind not in point:
            continue
        result = point[kind]
        if result['planned_offers'] == 0:
            continue
        prefix = '' if kind == 'writes' else 'reads: '
        for name in ('errors', 'warmup_errors', 'queue_dropped', 'warmup_queue_dropped'):
            if result[name] != 0:
                failures.append(prefix + name)
        if result['generated_offers'] != result['planned_offers']:
            failures.append(prefix + 'unissued offers')
        if result['successes_in_window'] < result['planned_offers'] * 0.99:
            failures.append(prefix + 'less than 99% completed inside window')
        p99 = result['scheduled_latency_ms_all_attempts']['p99']
        if p99 is None or (kind == 'writes' and p99 > (50 if durability == 'fleet' else 200)):
            failures.append(prefix + 'scheduled p99')
    return failures

def histogram_buckets(histogram):
    if 'buckets' in histogram:
        return histogram['buckets']
    count = histogram['bucket_count']
    if not isinstance(count, int) or not 2 <= count <= 100002:
        raise ValueError('invalid raw histogram bound')
    buckets = [0] * count
    previous = -1
    for index, value in histogram['nonzero_buckets']:
        if not isinstance(index, int) or not previous < index < count or not isinstance(value, int) or value <= 0:
            raise ValueError('invalid raw histogram index or count')
        buckets[index] = value
        previous = index
    return buckets

def histogram_delta(before, after):
    if before['resolution_us'] != after['resolution_us']:
        raise ValueError('histogram resolution changed')
    return {'resolution_us': after['resolution_us'],
            'total_ns': subtract(before['total_ns'], after['total_ns']),
            'buckets': subtract(histogram_buckets(before), histogram_buckets(after))}

def metric_delta(directory):
    start = directory / 'metrics-window-start.json'
    end = directory / 'metrics-window-end.json'
    if not start.exists() or not end.exists():
        return {'available': False}
    before, after = (read(start), read(end))
    if [x['url'] for x in before] != [x['url'] for x in after]:
        raise ValueError('metric endpoints changed')
    output = []
    for a, b in zip(before, after):
        first, last = (a['metrics'], b['metrics'])
        if first['schema_version'] != last['schema_version'] or first['sample_session'] != last['sample_session']:
            raise ValueError('process or metric schema changed')
        if first['histograms'].keys() != last['histograms'].keys():
            raise ValueError('histogram phases changed')
        families = subtract(first['storage_families'], last['storage_families'])
        counters = lambda snapshot: {operation: {key: value for key, value in fields.items() if key in ('started', 'outcomes', 'bytes_read', 'bytes_written')} for operation, fields in snapshot['storage_operations'].items()}
        totals = subtract(counters(first), counters(last))
        for operation, values in totals.items():
            for field in ('started', 'bytes_read', 'bytes_written'):
                if field in values and sum((family[operation][field] for family in families.values())) != values[field]:
                    raise ValueError(f'unclassified {operation}.{field}')
            for outcome, count in values.get('outcomes', {}).items():
                if sum((family[operation]['outcomes'][outcome] for family in families.values())) != count:
                    raise ValueError(f'unclassified {operation}.{outcome}')
        # Summary percentiles/means can decrease as more work completes. The
        # raw shared_queue/shared_upload histograms below supply their deltas.
        shared_counters = lambda snapshot: {name: value for name, value in snapshot.get('shared_publication', {}).items()
            if name in ('cohorts', 'singletons', 'cells', 'rows', 'bytes', 'failures', 'large_fallbacks', 'pressure_fallbacks')}
        shared = subtract(shared_counters(first), shared_counters(last))
        output.append({'shared_publication': shared, 'url': a['url'], 'sample_elapsed_ns': subtract(first['sample_elapsed_ns'], last['sample_elapsed_ns']), 'start_request_ms': [a['request_started_ms'], a['request_finished_ms']], 'end_request_ms': [b['request_started_ms'], b['request_finished_ms']], 'storage_families': families, 'storage': totals, 'histograms': {name: histogram_delta(first['histograms'][name], value) for name, value in last['histograms'].items()}, 'publication': subtract({name: first['writes'][name] for name in ('selected_roots', 'materialized_commits', 'publication_failures', 'uploaded_objects', 'uploaded_bytes')}, {name: last['writes'][name] for name in ('selected_roots', 'materialized_commits', 'publication_failures', 'uploaded_objects', 'uploaded_bytes')}), 'runtime_start': first.get('runtime'), 'runtime_end': last.get('runtime')})
    return {'available': True, 'endpoints': output}

def cost_report(metrics, successes):
    if not metrics.get('available') or successes == 0:
        return {'available': False}
    endpoints = metrics['endpoints']
    operations = {name: {'attempts': 0, 'successes': 0, 'bytes_read': 0, 'bytes_written': 0}
                  for name in ('put', 'get', 'range', 'head', 'list', 'copy', 'delete',
                               'multipart_start', 'multipart_part', 'multipart_complete', 'multipart_abort')}
    families = {}
    roots = commits = 0
    shared = {}
    for endpoint in endpoints:
        for name, value in endpoint.get('shared_publication', {}).items():
            if isinstance(value, (int, float)):
                shared[name] = shared.get(name, 0) + value
        publication = endpoint['publication']
        roots += publication['selected_roots']
        commits += publication['materialized_commits']
        for name, total in operations.items():
            source = endpoint['storage'].get(name, {})
            total['attempts'] += source.get('started', 0)
            total['successes'] += source.get('outcomes', {}).get('success', 0)
            for field in ('bytes_read', 'bytes_written'):
                total[field] += source.get(field, 0)
        for name, source in endpoint['storage_families'].items():
            family = families.setdefault(name, {'put_attempts': 0, 'put_successes': 0, 'get_attempts': 0,
                                               'range_get_attempts': 0})
            family['put_attempts'] += source['put']['started']
            family['put_successes'] += source['put']['outcomes']['success']
            family['get_attempts'] += source['get']['started'] + source.get('range', {}).get('started', 0)
            family['range_get_attempts'] += source.get('range', {}).get('started', 0)
    return {'available': True, 'denominator': 'successful logical commands inside the window',
            'operations': operations, 'families': families,
            'all_provider_put_successes_per_command': operations['put']['successes'] / successes,
            'all_provider_put_attempts_per_command': operations['put']['attempts'] / successes,
            'get_attempts_including_ranges_per_command': (operations['get']['attempts'] + operations['range']['attempts']) / successes,
            'mutation_request_successes_per_command': sum(operations[name]['successes'] for name in ('put', 'copy', 'multipart_start', 'multipart_part', 'multipart_complete', 'multipart_abort')) / successes,
            'observation_scope': 'storage API calls; provider SDK internal retries are not individually instrumented',
            'shared_publication': shared,
            'shared_cells_per_cohort': shared.get('cells', 0) / shared['cohorts'] if shared.get('cohorts') else None,
            'shared_bytes_per_cohort': shared.get('bytes', 0) / shared['cohorts'] if shared.get('cohorts') else None,
            'selected_roots': roots, 'materialized_commits': commits,
            'materialized_commits_per_selected_root': commits / roots if roots else None,
            'trailing_publication_included': False,
            'enrollment_gets_separately_attributed': all('owner_enrollment' in endpoint['storage_families'] and 'receiver_enrollment' in endpoint['storage_families'] for endpoint in endpoints),
            'fresh_enrollment_get_attempts_per_command': sum(families.get(name, {}).get('get_attempts', 0) for name in ('owner_enrollment', 'receiver_enrollment')) / successes if all('owner_enrollment' in endpoint['storage_families'] for endpoint in endpoints) else None}

def stability_report(directory):
    """Fit the last three minute segments; absent/reset evidence cannot pass."""
    paths = [directory / 'metrics-window-start.json',
             *sorted(directory.glob('metrics-window-minute-*.json'),
                     key=lambda path: int(path.stem.rsplit('-', 1)[1])),
             directory / 'metrics-window-end.json']
    if any(not path.exists() for path in paths) or len(paths) < 4:
        return {'available': False, 'pass': False, 'reason': 'three minute segments missing'}
    samples = [read(path) for path in paths[-4:]]
    owners = [[item['metrics'] for item in sample
               if item['metrics'].get('runtime') is not None] for sample in samples]
    if any(len(owner) != 1 for owner in owners):
        return {'available': False, 'pass': False, 'reason': 'one owner observation required'}
    values = [owner[0] for owner in owners]
    if len({(value['sample_session'], value['schema_version']) for value in values}) != 1:
        return {'available': False, 'pass': False, 'reason': 'process or schema changed'}
    times = [value['sample_elapsed_ns'] / 1e9 for value in values]
    if any(b <= a or b - a < 50 or b - a > 70 for a, b in zip(times, times[1:])):
        return {'available': False, 'pass': False, 'reason': 'minute observations missing or late'}
    if any('error' in value.get('publication_progress', {'error': 'missing'}) for value in values):
        return {'available': False, 'pass': False, 'reason': 'publication progress unavailable'}
    series = {
        'unpublished_node_log_bytes': [value['runtime']['unpublished_node_log_bytes'] for value in values],
        'oldest_unpublished_ms': [value['publication_progress']['oldest_unpublished_ms'] or 0 for value in values],
        'pending_publications': [value['publication_progress']['pending_publications'] for value in values],
        'retained_capture_bytes': [value['publication_progress']['retained_capture_bytes'] for value in values],
    }
    center = sum(times) / len(times)
    denominator = sum((time - center) ** 2 for time in times)
    slopes = {name: sum((time - center) * value for time, value in zip(times, data)) / denominator
              for name, data in series.items()}
    frontiers = [value.get('node_log_progress') for value in values]
    frontier_valid = all(frontier is None or ('error' not in frontier and not frontier['fenced']
                         and frontier['tiered_through'] <= frontier['issued_through']
                         and frontier['follower_proven_through'] <= frontier['issued_through'])
                         for frontier in frontiers)
    return {'available': True,
            'pass': frontier_valid and slopes['unpublished_node_log_bytes'] <= 0
                    and slopes['oldest_unpublished_ms'] <= 0,
            'method': 'least-squares slope over the last three one-minute segments; positive debt or age fails',
            'sample_elapsed_seconds': times, 'series': series, 'slopes_per_second': slopes,
            'frontiers': frontiers, 'frontier_valid': frontier_valid}

def fleet_mode_report(directory, *, writes_offered=True):
    """Require live Fleet and new proofs for writes, healthy fixed proofs for reads."""
    paths = [directory / 'metrics-window-start.json',
             *sorted(directory.glob('metrics-window-minute-*.json'),
                     key=lambda path: int(path.stem.rsplit('-', 1)[1])),
             directory / 'metrics-window-end.json']
    if not (directory / 'metrics-window-start.json').exists() or not (directory / 'metrics-window-end.json').exists():
        return {'available': False, 'pass': False, 'reason': 'fleet window observations missing'}
    frontiers = []
    for path in paths:
        sample = read(path)
        if not sample or not isinstance(sample[0].get('metrics'), dict):
            return {'available': False, 'pass': False, 'reason': 'fleet window owner observation missing'}
        frontier = sample[0]['metrics'].get('node_log_progress')
        if not isinstance(frontier, dict):
            return {'available': False, 'pass': False, 'reason': 'fleet window frontier missing'}
        frontiers.append(frontier)
    healthy = all('error' not in frontier and frontier.get('fleet_active') is True
                 and frontier.get('fenced') is False and frontier.get('rotating') is False
                 for frontier in frontiers)
    # A read-only window need not append. It still cannot hide epoch changes,
    # reset counters or an invalid frontier. Positive offered writes always
    # require advancement, even when every write failed before receiving proof.
    valid = all(type(frontier.get('log_epoch')) is int and frontier['log_epoch'] > 0
                and all(type(frontier.get(key)) is int and frontier[key] >= 0
                        for key in ('issued_through', 'tiered_through', 'follower_proven_through'))
                and frontier['tiered_through'] <= frontier['issued_through']
                and frontier['follower_proven_through'] <= frontier['issued_through']
                for frontier in frontiers)
    if valid:
        valid = len({frontier['log_epoch'] for frontier in frontiers}) == 1 and all(
            after[key] >= before[key]
            for before, after in zip(frontiers, frontiers[1:])
            for key in ('issued_through', 'tiered_through', 'follower_proven_through'))
    first, last = frontiers[0], frontiers[-1]
    proven_start, proven_end = first.get('follower_proven_through'), last.get('follower_proven_through')
    advancing = (type(proven_start) is int and type(proven_end) is int
                 and proven_end > proven_start)
    passed = healthy and valid and (advancing or not writes_offered)
    return {'available': True, 'pass': passed, 'frontiers': frontiers,
            'follower_proof_advanced': advancing,
            'follower_proof_required': writes_offered, 'frontier_valid': valid,
            'reason': None if passed else 'follower shipping inactive, fenced, rotating, invalid or required proof did not advance'}


def case_report(directory):
    case, summary = (read(directory / 'case.json'), read(directory / 'summary.json'))
    failures = []
    health = provider_health(directory)
    if case.get('provider_filesystem_required') and not health['pass']:
        failures.append('provider byte/inode health failed: ' + str(health['reason']))
    lifecycle = provider_lifecycle(directory, case.get('provider_lifecycle_required', False))
    if lifecycle['failed_phases'] or (lifecycle['required'] and not lifecycle['pass']):
        failures.append('provider lifecycle failed: ' + str(lifecycle['reason']))
    exclusions = directory / 'qualification-exclusions.json'
    if exclusions.exists():
        failures.append('explicit evidence exclusion: ' + json.dumps(read(exclusions), sort_keys=True))
    if not summary['completed']:
        failures.append(summary.get('failure') or 'case incomplete')
    try:
        expected_acks = expected_acknowledgements(case, summary)
    except (KeyError, ValueError):
        expected_acks = None
    if expected_acks is None or summary.get('acknowledged_rows') != expected_acks:
        failures.append('acknowledgement journals do not reconcile with client totals')
    for label in ('warm_audit', 'cold_audit'):
        audit = summary.get(label, {})
        expected = summary.get('acknowledged_rows')
        if expected is None or audit.get('checked') != expected or audit.get('retries_checked') != expected or (audit.get('errors') != 0) or (audit.get('exit_code') != 0):
            failures.append(label + ' incomplete or failed')
    if not summary.get('cold_retry_pass'):
        failures.append('cold contract retry missing')
    if summary.get('cleanup_failures'):
        failures.append('drain or cleanup failed')
    if case.get('telemetry') == 'off':
        failures.append('exporter-off diagnostic cannot qualify')
    if case.get('diagnostic'):
        failures.append('diagnostic profile')
    points = []
    for path in sorted(directory.glob('write-*/summary.json')):
        config, data = (read(path.parent / 'config.json'), read(path))
        if config.get('phase') in ('overload', 'recovery'):
            continue
        point_failures = delivery_failures(data, case['durability']) + failures
        if config['seconds'] < 300 or config['warmup_seconds'] < 30:
            point_failures.append('short diagnostic window')
        try:
            metrics = metric_delta(path.parent)
        except (KeyError, ValueError) as error:
            metrics = {'available': False, 'error': str(error)}
            point_failures.append('metric evidence invalid')
        writes = data['writes']
        fleet_mode = fleet_mode_report(path.parent, writes_offered=writes['planned_offers'] > 0) if case['system'] == 'cellule' and case['durability'] == 'fleet' else {'available': False, 'pass': False, 'reason': 'not a Cellule Fleet window'}
        if case['system'] == 'cellule' and case['durability'] == 'fleet' and not fleet_mode['pass']:
            point_failures.append('active follower durability unverified')
        stability = stability_report(path.parent) if case['system'] == 'cellule' else {'available': False, 'pass': False, 'reason': 'celld publication age not exposed by this fixture'}
        load_contract = {key: config.get(key) for key in ('cells', 'concurrency', 'queue_capacity',
            'write_offset', 'seconds', 'warmup_seconds', 'hot_read_cells')}
        points.append({'offered_writes_per_second': config['write_rate'], 'offered_reads_per_second': config['read_rate'], 'successful_writes_per_second': writes['successful_requests_per_second'], 'successful_reads_per_second': data['reads']['successful_requests_per_second'], 'load_contract': load_contract, 'logical_value_bytes_per_second': writes['successful_requests_per_second'] * 96, 'writes': writes, 'reads': data['reads'], 'window_metrics': metrics, 'window_cost': cost_report(metrics, writes['successes_in_window']), 'fleet_mode': fleet_mode, 'publication_stability': stability, 'delivery_latency_audit_pass': not point_failures, 'failures': point_failures})
    overload = summary.get('overload', {})
    recovery = overload.get('recovery')
    recovery_failures = (delivery_failures(recovery, case['durability']) if recovery else ['recovery phase missing']) + failures
    return {'schema_version': 1, 'reporter_sha256': REPORTER_SHA256, 'provider_health': health, 'provider_lifecycle': lifecycle, 'case': summary['case'], 'system': case['system'], 'durability': case['durability'], 'workload': 'sql-ledger-96', 'seconds': case['seconds'], 'warmup_seconds': case['warmup_seconds'], 'build_manifest_sha256': summary['build_manifest_sha256'], 'completed': summary['completed'], 'expected_acknowledged_rows': expected_acks, 'acknowledgement_count_reconciled': expected_acks is not None and summary.get('acknowledged_rows') == expected_acks, 'points': points, 'failures': failures, 'overload': {'available': bool(overload), 'reference_capacity': overload.get('reference_capacity'), 'reference_requires_paired_qualification': True, 'recovery_30_second_delivery_pass': bool(recovery) and not recovery_failures, 'recovery_failures': recovery_failures, 'drain_seconds': summary.get('drain_seconds'), 'safe_refusals_before_sql_verified': False}, 'qualification_pass': False, 'qualification_unverified': ['three paired repetitions', 'A/A variance', 'sustained debt slopes and age', 'read-only and mixed guardrails', 'safe overload refusals and qualified reference capacity']}
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    report = case_report(args.directory)
    destination = args.directory / 'report.json'
    destination.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'report': str(destination), 'qualification_pass': report['qualification_pass'], 'points': [{key: point[key] for key in ('offered_writes_per_second', 'successful_writes_per_second', 'delivery_latency_audit_pass', 'failures')} for point in report['points']]}))
