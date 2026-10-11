#!/usr/bin/env python3
"""Summarize matched native HTTP runs; preserve overload and latency evidence."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()


def read(directory):
    raw = json.loads((directory/'result.json').read_text())
    result = dict(directory=str(directory.resolve()), result_sha256=sha(directory/'result.json'),
                  implementation=raw['implementation'], passed=raw['passed'], failure=raw.get('failure'),
                  framework_revision=raw['framework_revision'], source_manifest_sha256=raw['source_manifest_sha256'],
                  binaries=raw['binaries'], driver_sha256=raw['binaries']['http_capacity'],
                  cells=raw['cells'], node_cpus=raw['node_cpus'], node_memory_bytes=raw['node_memory_bytes'],
                  images=raw['images'], docker_host=raw.get('docker_host'),
                  startup_seconds=raw.get('startup_seconds'), startups=raw.get('startups'),
                  disaster_recovery_seconds=raw.get('disaster_recovery_seconds'),
                  disaster_audit_seconds=raw.get('disaster_audit_seconds'),
                  audits={key:raw.get(key) for key in ('seed','warm_audit','cold_audit','disaster_audit','disaster_resumed_writes')},
                  all_load_requests_succeeded=raw.get('all_load_requests_succeeded'),
                  all_offers_delivered=raw.get('all_offers_delivered'), points=[])
    for point in raw['points']:
        metric=point['metrics']['writes' if point['kind']=='write' else 'reads']
        config=json.loads((directory/(point['label']+'-config.json')).read_text())
        result['points'].append(dict(label=point['label'],kind=point['kind'],offered_rate=point['offered_rate'],
                                    driver_exit_code=point['driver_exit_code'],seconds=config['seconds'],
                                    warmup_seconds=config['warmup_seconds'],concurrency=config['concurrency'],
                                    queue_capacity=config['queue_capacity'],
                                    metrics={key:value for key,value in metric.items() if not isinstance(value,list)}))
    peak_nodes, peak_provider = [0,0,0], 0
    for line in (directory/'resources.jsonl').read_text().splitlines():
        for name, sample in json.loads(line).items():
            peak=int(sample['cgroup']['memory.peak'])
            if '-rustfs' in name:
                peak_provider=max(peak_provider,peak)
            for node in range(3):
                if name.endswith('-node-'+str(node)):
                    peak_nodes[node]=max(peak_nodes[node],peak)
    result['peak_node_memory_bytes']=peak_nodes
    result['peak_provider_memory_bytes']=peak_provider
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cellule',type=Path,nargs='+',required=True)
    parser.add_argument('--celld',type=Path,nargs='+',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    runs=[read(path) for path in args.cellule+args.celld]
    assert len(args.cellule)==len(args.celld)>=2, 'retain at least two independent pairs'
    for run in runs:
        assert run['passed'], 'incomplete/correctness-failing run: '+run['directory']
        assert run['cells']==2000 and run['node_cpus']==1 and run['node_memory_bytes']==2*1024**3
        assert run['audits']['disaster_resumed_writes']==dict(acknowledged_writes=2000,distinct_cells=2000)
    reference=runs[0]
    def profile(run):
        return [(p['kind'],p['offered_rate'],p['seconds'],p['warmup_seconds'],p['concurrency'],p['queue_capacity']) for p in run['points']]
    assert all(run['driver_sha256']==reference['driver_sha256'] and run['images']==reference['images']
               and run['docker_host']==reference['docker_host'] and profile(run)==profile(reference) for run in runs), 'unmatched benchmark profile'
    args.output.write_text(json.dumps(dict(scope='native production storage/replication with fixed placement HTTP embeddings; single Linux host; small integer orders; not full celld fleet/V8 or throughput qualification',
                                          paired_runs=len(args.cellule),runs=runs),indent=2)+'\n')
