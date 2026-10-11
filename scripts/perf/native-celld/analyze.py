#!/usr/bin/env python3
"""Attribute retained HTTP measurements without mixing cumulative phase means."""
import argparse
import collections
import hashlib
import json
import math
from pathlib import Path
import re


def load(path):
    return json.loads(path.read_text())


def numbers(raw):
    return {parts[0]: int(parts[1]) for line in raw.splitlines() if len(parts := line.split()) == 2}


def percentile(values, percent):
    return values[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else None


def histogram_delta(samples, key):
    buckets = collections.Counter()
    ns = 0
    for before, after in samples:
        a, b = before['metrics']['histograms'][key], after['metrics']['histograms'][key]
        buckets.update(dict(b['nonzero_buckets']))
        buckets.subtract(dict(a['nonzero_buckets']))
        ns += b['total_ns'] - a['total_ns']
    if any(n < 0 for n in buckets.values()):
        raise RuntimeError('histogram counter regressed: ' + key)
    count = sum(buckets.values())
    bound = samples[0][0]['metrics']['histograms'][key]
    def quantile(percent):
        rank, seen = math.ceil(count * percent / 100), 0
        for index, n in sorted(buckets.items()):
            seen += n
            if count and seen >= rank:
                return index * bound['resolution_us'] / 1000 if index < bound['bucket_count'] - 1 else None
        return None
    return dict(count=count, total_ns=ns, mean_ms=ns/count/1e6 if count else None,
                p50_ms=quantile(50), p95_ms=quantile(95), p99_ms=quantile(99),
                overflow=buckets[bound['bucket_count'] - 1])


def resources(path, label):
    value = load(path / (label + '-resources.json'))
    out = {}
    for name, before in value['before'].items():
        after = value['after'][name]
        a, b = numbers(before['cgroup']['cpu.stat']), numbers(after['cgroup']['cpu.stat'])
        seconds = after['timestamp'] - before['timestamp']
        role = name.rsplit('-', 1)[-1] if '-node-' not in name else 'node-' + name.rsplit('-', 1)[-1]
        out[role] = dict(scope='entire point including setup, warmup and drain', seconds=seconds,
                         cpu_cores=(b['usage_usec']-a['usage_usec']) / seconds / 1e6,
                         throttled_periods=b['nr_throttled']-a['nr_throttled'],
                         elapsed_periods=b['nr_periods']-a['nr_periods'],
                         memory_current_bytes=int(after['cgroup']['memory.current']),
                         memory_peak_bytes=int(after['cgroup']['memory.peak']),
                         process_peak_rss_bytes=after['process_peak_rss_bytes'],
                         memory_events=numbers(after['cgroup']['memory.events']))
        if 'memory.stat' in after['cgroup']:
            out[role]['memory_stat'] = numbers(after['cgroup']['memory.stat'])
            out[role]['pressure_before'] = {k:before['cgroup'][k] for k in ('memory.pressure','io.pressure','cpu.pressure')}
            out[role]['pressure_after'] = {k:after['cgroup'][k] for k in ('memory.pressure','io.pressure','cpu.pressure')}
            out[role]['pressure_fraction'] = {}
            for field in ('memory.pressure', 'io.pressure', 'cpu.pressure'):
                for kind in ('some', 'full'):
                    pattern = r'^' + kind + r' .*total=(\d+)'
                    start = re.search(pattern, before['cgroup'][field], re.M)
                    finish = re.search(pattern, after['cgroup'][field], re.M)
                    if start is not None and finish is not None:
                        out[role]['pressure_fraction'][field + ':' + kind] = (
                            int(finish.group(1)) - int(start.group(1))) / seconds / 1e6
            def io_counters(raw):
                return {parts[0]: {key:int(value) for item in parts[1:]
                                   for key,value in [item.split('=', 1)]}
                        for line in raw.splitlines() if (parts := line.split())}
            io_start = io_counters(before['cgroup']['io.stat'])
            io_end = io_counters(after['cgroup']['io.stat'])
            out[role]['io_device_deltas'] = {device: {key:value-io_start.get(device,{}).get(key,0)
                                                     for key,value in counters.items()}
                                             for device,counters in io_end.items()}
    return out


def analyze(path):
    result = load(path / 'result.json')
    output = dict(path=str(path), implementation=result['implementation'],
                  passed=result['passed'], failure=result.get('failure'),
                  binaries=result['binaries'], source_manifest_sha256=result['source_manifest_sha256'],
                  diagnostic=result.get('diagnostic'), points=[],
                  all_load_requests_succeeded=result.get('all_load_requests_succeeded'),
                  all_offers_delivered=result.get('all_offers_delivered'),
                  warm_audit=result.get('warm_audit'), cold_audit=result.get('cold_audit'),
                  cold_startup_seconds=result.get('cold_startup_seconds'),
                  disaster_recovery_seconds=result.get('disaster_recovery_seconds'),
                  disaster_audit_seconds=result.get('disaster_audit_seconds'),
                  disaster_audit=result.get('disaster_audit'),
                  disaster_resumed_writes=result.get('disaster_resumed_writes'),
                  read_points=[],
                  retained_provider_volume=result.get('retained_provider_volume'),
                  result_sha256=hashlib.sha256((path / 'result.json').read_bytes()).hexdigest(),
                  retained_helpers_sha256={f.name:hashlib.sha256(f.read_bytes()).hexdigest()
                                           for name in ('diagnose.py','inventory.py','three-node.py','three-node-control.py')
                                           if (f := path / name).exists()})
    for point in result['points']:
        p = path / point['label']
        config = load(path / (point['label'] + '-config.json'))
        warm, seconds = config['warmup_seconds'], config['seconds']
        end = warm + seconds
        timeline = collections.Counter()
        all_window, warm_in_window = 0, 0
        request, scheduled = [], []
        journals = {}
        for file in sorted(p.glob('client-*.jsonl')):
            journals[file.name] = hashlib.sha256(file.read_bytes()).hexdigest()
            for line in file.open():
                row = json.loads(line)
                if (row['request'] is None if point['kind'] == 'write' else row['read_id'] is None):
                    continue
                if row['error'] is None:
                    completion = row['completed_seconds']
                    timeline[int(completion // 5)] += 1
                    if warm <= completion < end:
                        all_window += 1
                        warm_in_window += int(not row['measured'])
                if row['measured']:
                    request.append(row['request_latency_ms'])
                    scheduled.append(row['scheduled_latency_ms'])
        request.sort()
        scheduled.sort()
        metrics = point['metrics']
        writes = metrics['writes' if point['kind'] == 'write' else 'reads']
        analyzed = dict(label=point['label'], offered_rate=point['offered_rate'],
                        window_seconds=seconds, warmup_seconds=warm, drain_seconds=metrics['drain_seconds'],
                        reported_measured_cohort_rate=writes['successful_requests_per_second'],
                        all_completions_in_window_rate=all_window/seconds,
                        warmup_completions_in_window=warm_in_window,
                        whole_point_completion_rate=(writes['successes_including_drain'] + writes['warmup_attempts'] - writes['warmup_errors']) /
                                                    (warm + seconds + metrics['drain_seconds']),
                        queue_dropped=writes['queue_dropped'], late_measured_completions=writes['successes_after_deadline'],
                        errors=writes['errors']+writes['warmup_errors'],
                        timeline_5s=[timeline[i] for i in range(max(timeline, default=-1)+1)],
                        request_latency_ms={f'p{n}':percentile(request,n) for n in (50,95,99)},
                        scheduled_latency_ms={f'p{n}':percentile(scheduled,n) for n in (50,95,99)},
                        resource_point=resources(path, point['label']), journals_sha256=journals)
        analyzed['request_latency_ms']['mean'] = sum(request)/len(request) if request else None
        analyzed['scheduled_latency_ms']['mean'] = sum(scheduled)/len(scheduled) if scheduled else None
        starts, ends = load(p/'metrics-window-start.json'), load(p/'metrics-window-end.json')
        if point['kind'] == 'write' and starts[0]['metrics'].get('histograms'):
            if [s['url'] for s in starts] != [e['url'] for e in ends]:
                raise RuntimeError('metrics endpoints changed')
            samples = list(zip(starts, ends))
            for a,b in samples:
                if a['metrics']['sample_session'] != b['metrics']['sample_session']:
                    raise RuntimeError('metrics session changed')
            analyzed['window_phases'] = {key:histogram_delta(samples,key) for key in
                ('actor_queue','worker','capture_total','capture_checkpoint','dirty_admission',
                 'publication','proof_object','response_object','response_confirmation','compaction','shared_upload')}
            publications = sum(e['metrics']['writes']['materialized_commits'] - a['metrics']['writes']['materialized_commits']
                               for a,e in samples)
            analyzed['window_publications'] = publications
            analyzed['window_storage'] = {}
            for op in ('put','get','head'):
                count=ns=read=written=0
                for a,e in samples:
                    a,e=a['metrics']['storage_operations'][op],e['metrics']['storage_operations'][op]
                    count += e['duration']['count']-a['duration']['count']
                    ns += e['duration']['total_ns']-a['duration']['total_ns']
                    read += e['bytes_read']-a['bytes_read']
                    written += e['bytes_written']-a['bytes_written']
                analyzed['window_storage'][op] = dict(completions=count, mean_ms=ns/count/1e6 if count else None,
                                                      per_publication=count/publications if publications else None,
                                                      bytes_read=read, bytes_written=written)
            analyzed['window_families'] = {family:{op:sum(
                e['metrics']['storage_families'][family][op]['outcomes']['success'] -
                a['metrics']['storage_families'][family][op]['outcomes']['success'] for a,e in samples)
                for op in ('put','get','head')} for family in ('immutable','cell_authority','node_authority','other')}
            analyzed['host_capacity'] = starts[0]['metrics']['host_capacity']
            analyzed['retained_bytes_window_end'] = [e['metrics']['runtime']['retained_bytes'] for e in ends]
            analyzed['publication_progress_window_end'] = [e['metrics']['publication_progress'] for e in ends]
        if point['kind'] == 'read':
            analyzed['driver_exit_code'] = point['driver_exit_code']
            analyzed['metrics'] = writes
            output['read_points'].append(analyzed)
        else:
            output['points'].append(analyzed)
    for name, field in (('provider-inventory.json', 'inventory'),
                        ('provider-inventory-complete.json', 'inventory_complete')):
        inventory_path = path / name
        if inventory_path.exists():
            value = load(inventory_path)
            summary = {key:item for key,item in value.items() if key != 'objects'}
            summary['sha256'] = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
            levels = collections.Counter()
            for obj in value['objects']:
                match = re.search(r'/ltx/e\d+/(\d{4})/', obj['key'])
                if match:
                    levels[match.group(1)] += 1
            if levels:
                summary['native_ltx_levels'] = dict(levels)
            if result['implementation'] == 'celld-native' and value['prefix'].endswith('/'):
                summary['namespace_scope'] = 'ownership only; native fixture LTX prefix lacks this slash'
            output[field] = summary
    archive_path = path / 'provider-volume-archive.json'
    if archive_path.exists():
        output['provider_volume_archive'] = load(archive_path)
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runs', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(dict(scope='causal analysis of short local HTTP runs',
                                          runs=[analyze(p.resolve()) for p in args.runs]),indent=2)+'\n')
