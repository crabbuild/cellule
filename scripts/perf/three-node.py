#!/usr/bin/env python3
"""Build and measure three static HTTP writer nodes with real RustFS durability.

Artifacts and Linux build caches must live outside the checkout. Each node has
one exclusive guest CPU, 2 GiB memory, no swap, and disk-backed temporary state.
The provider and client use separate CPUs. This is local characterization,
not fleet replication, crash qualification, or a production capacity claim.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
RUST = 'rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e'
RUSTFS = 'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def put(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def build(args):
    directory = args.artifacts.resolve()
    require(directory != ROOT and ROOT not in directory.parents, 'artifacts must be outside the checkout')
    directory.mkdir(parents=True, exist_ok=False)
    source = directory / 'source'
    files = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT).decode().split('\0')
    for name in files:
        if name and (ROOT / name).is_file():
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, target)
    put(directory / 'source.json', dict(
        revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        files={str(path.relative_to(source)): sha(path) for path in source.rglob('*') if path.is_file()}))
    with (directory / 'build.log').open('x') as log:
        subprocess.run(['docker', '--context', args.context, 'run', '--rm', '--cpus', '8', '--memory', '12g',
                        '-v', f'{directory}:/work', '-w', '/work/source', '-e', 'CARGO_TARGET_DIR=/work/target',
                        RUST, 'cargo', 'build', '--release', '--locked', '-p', 'cellule-axum',
                        '--example', 'sql', '--example', 'http_capacity'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    print(directory, flush=True)


class Run:
    def __init__(self, args):
        self.args = args
        self.artifacts = args.artifacts.resolve(strict=True)
        self.output = args.output.resolve()
        require(self.output != ROOT and ROOT not in self.output.parents, 'evidence must be outside the checkout')
        self.output.mkdir(parents=True, exist_ok=False)
        self.tag = 'cellule-http-' + uuid.uuid4().hex[:10]
        self.client = self.tag + '-client'
        self.store = self.tag + '-rustfs'
        self.nodes = [self.tag + f'-node-{index}' for index in range(3)]
        self.containers, self.scratch = [], []
        self.client_volume = None
        self.current_point = None
        self.generation = 0
        self.addresses = [f'127.0.0.1:{18080 + index}' for index in range(3)]
        self.stopping = threading.Event()
        self.sampling_errors = []
        self.binary = self.artifacts / 'target/release/examples/sql'
        self.driver = self.artifacts / 'target/release/examples/http_capacity'
        require(self.binary.is_file() and self.driver.is_file(), 'build both release examples first')
        self.summary = dict(passed=False, topology='three static writers; S3 object-durable ACKs',
                            implementation=args.implementation,
                            cells=args.cells, per_node_cells=[len(range(i, args.cells, 3)) for i in range(3)],
                            node_cpus=1, node_memory_bytes=2 * 1024**3, swap_bytes=0,
                            sql='INSERT and SELECT in one durable command; two integer columns plus request ledger',
                            images=dict(compiler=RUST, provider=RUSTFS),
                            source_manifest_sha256=sha(self.artifacts / 'source.json'),
                            framework_revision=json.loads((self.artifacts / 'source.json').read_text())['revision'],
                            binaries=dict(sql=sha(self.binary), http_capacity=sha(self.driver)),
                            client_journal_storage='private Linux Docker volume; exported after each load window',
                            points=[], scope='single-host Linux characterization; no crash/follower qualification')
        if args.implementation == 'celld-native':
            self.summary['native_scope'] = 'celld production SQLite/LtxRepl/BucketOwnership; fixed placement Rust embedding; no V8/Node dispatch or dynamic fleet executor'
            self.summary['operator_settings'] = dict(CELLD_LOG_FLUSH_MS=0, lease_ttl_ms=30000,
                                                     activation_concurrency=8, tokio_workers=1, blocking_workers=1)
        # Retain the exact orchestration and audit sources used for this run.
        for name in ('three-node.py', 'three-node-control.py'):
            shutil.copy2(Path(__file__).with_name(name), self.output / name)
        self.summary['runner_sha256'] = sha(self.output / 'three-node.py')
        self.summary['control_sha256'] = sha(self.output / 'three-node-control.py')

    def docker(self, *arguments, timeout=60, check=True):
        result = subprocess.run(['docker', '--context', self.args.context, *arguments],
                                capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f'Docker {arguments[0]} failed ({result.returncode}): {result.stdout[-4000:]} {result.stderr[-4000:]}')
        return result

    def launch(self, name, cpus, memory, cpuset, extra, image, command):
        self.docker('run', '-d', '--name', name, '--label', 'cellule.three-node=' + self.tag,
                    '--network', 'host', '--cpus', str(cpus), '--cpuset-cpus', cpuset,
                    '--memory', f'{memory}g', '--memory-swap', f'{memory}g',
                    '--ulimit', 'nofile=65536:65536', *extra, image, *command)
        self.containers.append(name)
        evidence = self.snapshot(name)
        config = evidence['host_config']
        require(config['NanoCpus'] == cpus * 10**9 and config['Memory'] == memory * 1024**3
                and config['MemorySwap'] == config['Memory'] and config['CpusetCpus'] == cpuset,
                'Docker did not enforce requested resources')
        quota, period = evidence['cgroup']['cpu.max'].split()
        require(quota != 'max' and int(quota) == cpus * int(period)
                and evidence['cgroup']['memory.max'] == str(memory * 1024**3)
                and evidence['cgroup']['memory.swap.max'] == '0'
                and evidence['cgroup']['cpuset.cpus.effective'] == cpuset,
                'effective cgroup limits differ from requested resources')
        path = self.output / f'{name}-placement.json'
        if path.exists():
            path = self.output / f'{name}-placement-{self.generation}.json'
        put(path, evidence)

    def snapshot(self, name):
        inspected = json.loads(self.docker('inspect', name).stdout)[0]
        require(inspected['State']['Running'] and not inspected['State']['OOMKilled'], name + ' exited or was OOM-killed')
        fields = ['cpu.max', 'cpu.stat', 'memory.max', 'memory.swap.max', 'memory.current',
                  'memory.peak', 'memory.events', 'cpuset.cpus.effective']
        # RustFS does not bundle Python. Its shell can emit each cgroup file.
        raw = self.docker('exec', name, 'sh', '-c',
                          'for n in ' + ' '.join(fields) + '; do echo "$n"; cat "/sys/fs/cgroup/$n"; echo END; done; cat /proc/1/status').stdout
        cgroup = {}
        for field in fields:
            match = re.search(r'(?:^|\n)' + re.escape(field) + r'\n(.*?)\nEND', raw, re.S)
            require(match is not None, 'missing cgroup evidence: ' + field)
            cgroup[field] = match.group(1)
        require(re.search(r'^oom_kill 0$', cgroup['memory.events'], re.M) is not None, name + ' has an OOM event')
        rss = re.search(r'^VmHWM:\s+(\d+) kB$', raw, re.M)
        return dict(timestamp=time.time(), state=inspected['State'], host_config={k:inspected['HostConfig'][k]
                    for k in ('NanoCpus', 'Memory', 'MemorySwap', 'CpusetCpus')}, cgroup=cgroup,
                    process_peak_rss_bytes=int(rss.group(1))*1024 if rss else None)

    def capture(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            result = dict(zip(self.containers, pool.map(self.snapshot, self.containers)))
        if self.store in result:
            raw = self.docker('exec', self.store, 'sh', '-c', 'df -Pk /data; df -Pi /data').stdout
            lines = raw.splitlines()
            require(len(lines) == 4 and int(lines[1].split()[3])*1024 >= 2 * 1024**3
                    and int(lines[3].split()[3]) >= 200_000, 'provider filesystem below measurement headroom')
            result[self.store]['filesystem'] = raw
        return result

    def sample(self):
        try:
            with (self.output / 'resources.jsonl').open('a') as journal:
                while not self.stopping.is_set():
                    journal.write(json.dumps(self.capture()) + '\n')
                    journal.flush()
                    self.stopping.wait(5)
        except Exception as error:
            self.sampling_errors.append(str(error))

    def http(self, method, address, path):
        result = self.docker('exec', self.client, 'python3', '/source/scripts/perf/control.py',
                             method, 'http://' + address + path, timeout=100)
        return json.loads(result.stdout)

    def start_nodes(self, cold=False):
        started = time.monotonic()
        self.generation += 1
        details = []
        for index, name in enumerate(self.nodes):
            volume = name + f'-state-{self.generation}'
            self.docker('volume', 'create', '--label', 'cellule.three-node=' + self.tag, volume)
            self.scratch.append(volume)
            env = dict(CELLULE_TEST_ENDPOINT='http://127.0.0.1:9000', CELLULE_TEST_BUCKET='comparison',
                       CELLULE_TEST_PREFIX=self.tag, AWS_ACCESS_KEY_ID='benchmark_access',
                       AWS_SECRET_ACCESS_KEY='benchmark_secret_private', AWS_DEFAULT_REGION='us-east-1',
                       CELLULE_AXUM_CELLS=str(self.args.cells), CELLULE_AXUM_WORKERS='1',
                       CELLULE_AXUM_NODE_COUNT='3', CELLULE_AXUM_NODE_INDEX=str(index),
                       CELLULE_AXUM_FLEET_DIR='/scratch/fleet',
                       CELLULE_AXUM_BIND=self.addresses[index], TOKIO_WORKER_THREADS='1', TMPDIR='/scratch')
            if self.args.implementation == 'celld-native':
                # Match Cellule's immediate bucket publication and 30-second
                # authority lifetime using celld's supported operator setting.
                env['CELLD_LOG_FLUSH_MS'] = '0'
            extra = ['-v', f'{self.artifacts}:/work:ro', '-v', f'{volume}:/scratch']
            for key, value in env.items():
                extra += ['-e', key + '=' + value]
            command = f'mkdir -m 700 /scratch/fleet && cp /evidence/tls/ca.crt /evidence/tls/node-{index}.crt /evidence/tls/node-{index}.key /scratch/fleet/ && exec /work/target/release/examples/sql'
            extra += ['-v', f'{self.output}:/evidence:ro']
            self.launch(name, 1, 2, str(index), extra, RUST, ['sh', '-c', command])
        deadline = started + self.args.startup_timeout
        for index, name in enumerate(self.nodes):
            while True:
                inspected = json.loads(self.docker('inspect', name).stdout)[0]
                logs = self.docker('logs', name).stdout
                if 'Orders service: http://' in logs:
                    match = re.search(r'Cells startup: (\d+); restored: (\d+)', logs)
                    require(match is not None and int(match.group(1)) == self.summary['per_node_cells'][index],
                            'wrong per-node Cell population')
                    require(int(match.group(2)) == (int(match.group(1)) if cold else 0), 'incomplete fresh/cold activation')
                    if self.generation == 3 and self.args.disaster_recovery:
                        require(f'Cells taken over: {self.summary["per_node_cells"][index]}\n' in logs,
                                'crash startup did not take over every Cell')
                    topology = self.http('GET', self.addresses[index], '/debug/topology')
                    require(topology == dict(status=200, body=dict(total_cells=self.args.cells, node_count=3, node_index=index)),
                            'HTTP topology differs from requested placement')
                    activation = re.search(r'Cells activation milliseconds: (\d+)', logs)
                    require(activation is not None, 'missing activation timing')
                    details.append(dict(node=index, cells=int(match.group(1)), restored=int(match.group(2)),
                                        activation_ms=int(activation.group(1)),
                                        fences=[json.loads(line.removeprefix('Node takeover: '))
                                                for line in logs.splitlines() if line.startswith('Node takeover: ')]))
                    break
                require(inspected['State']['Running'], name + ' failed startup: ' + logs[-3000:] + self.docker('logs', name).stderr[-3000:])
                require(time.monotonic() < deadline, name + ' startup timeout')
                time.sleep(1)
        self.stopping.clear()
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampler.start()
        elapsed = time.monotonic() - started
        self.summary.setdefault('startups', []).append(dict(generation=self.generation, seconds=elapsed, nodes=details))
        return elapsed

    def stop_sampling(self, label):
        self.stopping.set()
        if hasattr(self, 'sampler'):
            self.sampler.join(timeout=60)
            require(not self.sampler.is_alive() and not self.sampling_errors, 'resource sampling failed: ' + repr(self.sampling_errors))
        put(self.output / f'{label}-resources.json', self.capture())

    def stop_nodes(self, label):
        self.stop_sampling(label)
        for name in self.nodes:
            self.docker('kill', '--signal', 'SIGINT', name)
        cells = []
        for name in self.nodes:
            waited = self.docker('wait', name, timeout=600)
            logs = self.docker('logs', name)
            (self.output / f'{label}-{name}.log').write_text(logs.stdout + logs.stderr)
            state = json.loads(self.docker('inspect', name).stdout)[0]['State']
            require(waited.stdout.strip() == '0' and state['ExitCode'] == 0 and not state['OOMKilled'], 'node did not drain cleanly')
            cells.extend(json.loads(line.removeprefix('Cell drained: ')) for line in logs.stdout.splitlines()
                         if line.startswith('Cell drained: '))
            self.docker('rm', name)
            self.containers.remove(name)
        require(len(cells) == self.args.cells and len({cell['cell'] for cell in cells}) == self.args.cells
                and sorted(cell['shard'] for cell in cells) == list(range(self.args.cells)), 'drain missed global Cells')
        put(self.output / f'{label}-cells.json', cells)
        # Cold nodes receive new local volumes; bucket state is the sole recovery source.
        for volume in self.scratch:
            self.docker('volume', 'rm', volume)
        self.scratch.clear()
        return {cell['cell']: cell for cell in cells}

    def crash_nodes(self):
        self.stop_sampling('before-disaster')
        started = time.monotonic()
        for name in self.nodes:
            self.docker('kill', '--signal', 'SIGKILL', name)
        for name in self.nodes:
            waited = self.docker('wait', name)
            logs = self.docker('logs', name)
            (self.output / f'crashed-{name}.log').write_text(logs.stdout + logs.stderr)
            state = json.loads(self.docker('inspect', name).stdout)[0]['State']
            put(self.output / f'crashed-{name}-state.json', state)
            require(waited.stdout.strip() == '137' and state['ExitCode'] == 137
                    and not state['OOMKilled'] and 'Cell drained: ' not in logs.stdout,
                    'disaster requires abrupt process loss without drain or OOM')
            self.docker('rm', name)
            self.containers.remove(name)
        for volume in self.scratch:
            self.docker('volume', 'rm', volume)
        self.scratch.clear()
        return started

    def control(self, operation, expected_writes=0, write_offset=0):
        config = dict(addresses=self.addresses, cells=self.args.cells, directory='/evidence',
                      expected_writes=expected_writes, write_offset=write_offset)
        put(self.output / 'control-config.json', config)
        result = self.docker('exec', self.client, 'python3', '/evidence/three-node-control.py',
                             operation, '/evidence/control-config.json', timeout=3600)
        return json.loads(result.stdout)

    def execute(self):
        host = json.loads(self.docker('info', '--format', '{{json .}}').stdout)
        require(host['NCPU'] >= 9 and host['MemTotal'] >= 12 * 1024**3, 'requires nine guest CPUs and 12 GiB memory')
        deadline = time.monotonic() + 300
        announced = False
        while self.docker('ps', '-q').stdout.strip():
            require(time.monotonic() < deadline, 'Docker context remained busy; use an idle dedicated context')
            if not announced:
                print(json.dumps(dict(event='waiting-for-idle-context')), flush=True)
                announced = True
            time.sleep(5)
        subprocess.run(['python3', str(self.artifacts / 'source/scripts/generate-capacity-tls.py'),
                        str(self.output / 'tls')], check=True)
        self.summary['docker_host'] = {k:host[k] for k in ('ID', 'NCPU', 'MemTotal', 'Architecture', 'ServerVersion')}
        self.client_volume = self.client + '-journals'
        self.docker('volume', 'create', '--label', 'cellule.three-node=' + self.tag, self.client_volume)
        self.launch(self.client, 4, 2, '5-8', ['-v', f'{self.artifacts}:/work:ro',
                    '-v', f'{self.artifacts / "source"}:/source:ro', '-v', f'{self.output}:/evidence',
                    '-v', f'{self.client_volume}:/client-evidence'], RUST, ['sleep', 'infinity'])
        # The shared host network preserves the example's loopback-only HTTP contract.
        code = 'import socket; ports=[9000,9001,18080,18081,18082]; sockets=[socket.socket() for p in ports]; [s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1) for s in sockets]; [s.bind(("127.0.0.1",p)) for s,p in zip(sockets,ports)]'
        self.docker('exec', self.client, 'python3', '-c', code)
        volume = self.store + '-data'
        self.docker('volume', 'create', '--label', 'cellule.three-node=' + self.tag, volume)
        self.summary['retained_provider_volume'] = volume
        self.launch(self.store, 2, 2, '3-4', ['-v', f'{volume}:/data', '-e', 'RUSTFS_ACCESS_KEY=benchmark_access',
                    '-e', 'RUSTFS_SECRET_KEY=benchmark_secret_private', '-e', 'MIMALLOC_ALLOW_THP=0'], RUSTFS, ['/data'])
        for attempt in range(60):
            result = self.docker('exec', self.client, 'python3', '/source/scripts/perf/control.py', 'bucket', check=False)
            if result.returncode == 0:
                break
            time.sleep(1)
        require(result.returncode == 0, 'RustFS bucket initialization failed: ' + result.stderr)
        filesystem = self.docker('exec', self.store, 'sh', '-c', 'df -Pk /data; df -Pi /data').stdout
        (self.output / 'provider-filesystem.txt').write_text(filesystem)
        lines = filesystem.splitlines()
        require(len(lines) == 4 and int(lines[1].split()[3])*1024 >= 10 * 1024**3
                and int(lines[3].split()[3]) >= 1_000_000, 'insufficient provider filesystem headroom')
        self.summary['startup_seconds'] = self.start_nodes()
        self.summary['seed'] = self.control('seed')
        print(json.dumps(dict(event='seeded', **self.summary['seed'])), flush=True)
        offset = 0
        expected_writes = self.args.cells
        for repetition in range(1, self.args.repetitions + 1):
            # Finish the write sweep before measuring the read-only workload.
            for kind, rates in [('write', self.args.write_rates), ('read', self.args.read_rates)]:
                for rate in rates:
                    label = f'point-r{repetition}-{kind}-{rate}'
                    self.current_point = label
                    config = dict(address=self.addresses[0], node_addresses=self.addresses, cells=self.args.cells,
                                  concurrency=self.args.concurrency, queue_capacity=4096,
                                  write_rate=rate if kind == 'write' else 0, read_rate=rate if kind == 'read' else 0,
                                  warmup_seconds=self.args.warmup, seconds=self.args.seconds,
                                  evidence_directory='/client-evidence/' + label, seed_file='/evidence/seeds.json',
                                  write_offset=offset, metrics_urls=['http://' + address + '/debug/metrics' for address in self.addresses])
                    put(self.output / f'{label}-config.json', config)
                    before = self.capture()
                    result = self.docker('exec', self.client, '/work/target/release/examples/http_capacity',
                                         '/evidence/' + label + '-config.json', timeout=self.args.seconds+self.args.warmup+600, check=False)
                    (self.output / f'{label}.log').write_text(result.stdout + result.stderr)
                    self.docker('cp', self.client + ':/client-evidence/' + label, str(self.output), timeout=600)
                    self.current_point = None
                    path = self.output / label / 'summary.json'
                    require(path.is_file(), 'HTTP driver produced no summary: ' + result.stderr[-2000:])
                    metrics = json.loads(path.read_text())
                    put(self.output / f'{label}-resources.json', dict(before=before, after=self.capture()))
                    point = dict(label=label, kind=kind, offered_rate=rate, driver_exit_code=result.returncode, metrics=metrics)
                    self.summary['points'].append(point)
                    expected_writes += metrics['writes']['successes_including_drain'] + metrics['writes']['warmup_attempts'] - metrics['writes']['warmup_errors']
                    offset += ((config['write_rate']*(self.args.seconds+self.args.warmup) + self.args.cells - 1)//self.args.cells)*self.args.cells
                    put(self.output / 'result.json', self.summary)
                    measured = metrics['writes' if kind == 'write' else 'reads']
                    print(json.dumps(dict(event=label, offered=rate, achieved=measured['successful_requests_per_second'],
                                          errors=measured['errors'], dropped=measured['queue_dropped'],
                                          latency=measured['scheduled_latency_ms_all_attempts'])), flush=True)
        self.summary['warm_audit'] = self.control('audit', expected_writes)
        original = self.stop_nodes('initial')
        self.summary['cold_startup_seconds'] = self.start_nodes(cold=True)
        self.summary['cold_audit'] = self.control('audit', expected_writes)
        if self.args.disaster_recovery:
            crash_started = self.crash_nodes()
            self.summary['disaster_boot_seconds'] = self.start_nodes(cold=True)
            self.summary['disaster_recovery_seconds'] = time.monotonic() - crash_started
            audit_started = time.monotonic()
            self.summary['disaster_resumed_writes'] = self.control('write-recovered', write_offset=offset)
            expected_writes += self.args.cells
            self.summary['disaster_audit'] = self.control('audit', expected_writes)
            self.summary['disaster_audit_seconds'] = time.monotonic() - audit_started
            recovered = self.stop_nodes('disaster')
        else:
            recovered = self.stop_nodes('cold')
        put(self.output / 'provider-after-cold-drain.json', self.snapshot(self.store))
        require(original.keys() == recovered.keys() and all(recovered[cell]['epoch'] > original[cell]['epoch']
                and recovered[cell]['commit_sequence'] == original[cell]['commit_sequence'] + int(self.args.disaster_recovery)
                for cell in original),
                'cold restore changed Cell identity/sequence or did not advance fencing epoch')
        if self.args.disaster_recovery:
            require(all(recovered[cell]['epoch'] >= original[cell]['epoch'] + 2 for cell in original),
                    'disaster recovery did not acquire a second successor epoch')
        self.summary['journal_sha256'] = {str(path.relative_to(self.output)): sha(path)
            for path in [self.output / 'seed.jsonl', *sorted(self.output.glob('point-*/client-*.jsonl')),
                         *self.output.glob('recovered-writes.jsonl')]}
        self.summary['passed'] = True
        self.summary['all_load_requests_succeeded'] = all(point['driver_exit_code'] == 0 for point in self.summary['points'])
        self.summary['all_offers_delivered'] = all(all(point['metrics'][kind][field] == 0
            for field in ('errors', 'warmup_errors', 'producer_unissued', 'queue_dropped', 'warmup_queue_dropped', 'successes_after_deadline'))
            for point in self.summary['points'] for kind in ('writes', 'reads'))
        print(json.dumps(dict(event='verified', acknowledged_writes=expected_writes, restored_cells=self.args.cells)), flush=True)

    def cleanup(self):
        self.stopping.set()
        if hasattr(self, 'sampler'):
            self.sampler.join(timeout=60)
        if self.current_point and not (self.output / self.current_point).exists():
            self.docker('cp', self.client + ':/client-evidence/' + self.current_point,
                        str(self.output), timeout=600, check=False)
        for name in reversed(self.containers):
            logs = self.docker('logs', name, check=False)
            if name in self.nodes and 'Orders service: http://' in logs.stdout:
                self.docker('stop', '--signal', 'SIGINT', '--timeout', '600', name, timeout=620, check=False)
            logs = self.docker('logs', name, check=False)
            (self.output / f'final-{name}.log').write_text(logs.stdout + logs.stderr)
            inspected = self.docker('inspect', name, check=False)
            if inspected.returncode == 0:
                put(self.output / f'final-{name}-state.json', json.loads(inspected.stdout)[0]['State'])
            self.docker('rm', '-f', name, check=False)
        for volume in self.scratch:
            self.docker('volume', 'rm', volume, check=False)
        if self.client_volume:
            self.docker('volume', 'rm', self.client_volume, check=False)
        put(self.output / 'result.json', self.summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['build', 'run'])
    parser.add_argument('--context', required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--cells', type=int, default=2000)
    parser.add_argument('--write-rates', type=int, nargs='+', default=[100, 300, 600])
    parser.add_argument('--read-rates', type=int, nargs='+', default=[1000, 3000, 6000])
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--warmup', type=int, default=10)
    parser.add_argument('--repetitions', type=int, default=1)
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--startup-timeout', type=int, default=1800)
    parser.add_argument('--disaster-recovery', action='store_true',
                        help='also SIGKILL all writers, discard local volumes and audit fenced bucket takeover')
    parser.add_argument('--implementation', choices=['cellule', 'celld-native'], default='cellule')
    args = parser.parse_args()
    require(3 <= args.cells <= 2000 and 1 <= args.seconds <= 3600 and 0 <= args.warmup <= 60
            and 1 <= args.repetitions <= 10 and 1 <= args.concurrency <= 1024
            and all(1 <= rate <= 100000 for rate in args.write_rates + args.read_rates), 'invalid load bounds')
    if args.operation == 'build':
        build(args)
        return
    require(args.output is not None, 'run requires --output')
    # Share the existing comparison harness lock so both runners serialize.
    lock_path = Path('/tmp') / ('cellule-perf-' + hashlib.sha256(args.context.encode()).hexdigest()[:16] + '.lock')
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps(dict(event='waiting-for-comparison-lock')), flush=True)
            fcntl.flock(lock, fcntl.LOCK_EX)
        run = Run(args)
        try:
            run.execute()
        except BaseException as error:
            run.summary['failure'] = str(error) or type(error).__name__
            raise
        finally:
            run.cleanup()


if __name__ == '__main__':
    main()
