import os, sys, json, subprocess, time, uuid, threading, traceback, hashlib, math, shutil, fcntl, re
from pathlib import Path
BASE = Path(__file__).resolve().parent
CTX = None
RUST = 'rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e'
CELLD = 'ghcr.io/denoland/celld@sha256:acb0bc2dca628f0dac7c6b43eea42615a79f4943f316f6df7c1da0b86d52150b'
DOCKER = []
CONTROL = None
STORE = None
ARGS = None
MANIFEST = None
RUNNER_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
CREDS = ['-e', 'AWS_ACCESS_KEY_ID=benchmark_access', '-e', 'AWS_SECRET_ACCESS_KEY=benchmark_secret_private', '-e', 'AWS_DEFAULT_REGION=us-east-1']

def docker(*args, check=True, timeout=700):
    p = subprocess.run(DOCKER + list(map(str, args)), capture_output=True, text=True, timeout=timeout)
    if check and p.returncode:
        raise RuntimeError(f'docker {args[0]} failed: {p.stdout} {p.stderr}')
    return p

def put(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')

def http(method, path, body=None, port=8080):
    args = ['exec', CONTROL, 'python3', '/work/control.py', method, f'http://127.0.0.1:{port}{path}']
    if body is not None:
        args.append(json.dumps(body))
    return json.loads(docker(*args, timeout=120).stdout)

def storage_format_smoke(directory, prefix):
    # Check the provider's actual persisted codec before measuring. A source
    # manifest alone cannot detect an incorrectly reused Cargo library.
    name = 'crates/cellule-ltx/src/replica/root.rs'
    source = (BASE / 'source' / name).read_bytes()
    if hashlib.sha256(source).hexdigest() != json.loads((BASE / 'framework-source.json').read_text())[name]:
        raise RuntimeError('root codec source differs from the immutable manifest')
    versions = re.findall(rb'version: ([0-9]+),', source)
    if len(versions) != 1:
        raise RuntimeError('cannot establish expected root format')
    expected = int(versions[0])
    script = r'''
import datetime, hashlib, hmac, json, sys, urllib.request, urllib.parse, xml.etree.ElementTree as ET
def get(path, query=''):
    now = datetime.datetime.now(datetime.timezone.utc)
    day, stamp = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
    payload = hashlib.sha256(b'').hexdigest()
    host = '127.0.0.1:9000'
    headers = f'host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n'
    signed = 'host;x-amz-content-sha256;x-amz-date'
    canonical = f'GET\n{path}\n{query}\n{headers}\n{signed}\n{payload}'
    scope = f'{day}/us-east-1/s3/aws4_request'
    message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n' + hashlib.sha256(canonical.encode()).hexdigest()
    key = b'AWS4benchmark_secret_private'
    for part in (day, 'us-east-1', 's3', 'aws4_request'):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
    auth = f'AWS4-HMAC-SHA256 Credential=benchmark_access/{scope}, SignedHeaders={signed}, Signature={signature}'
    request = urllib.request.Request('http://' + host + path + ('?' + query if query else ''), headers={'Authorization': auth, 'x-amz-date': stamp, 'x-amz-content-sha256': payload})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()
cell = sys.argv[3]
cell_prefix = sys.argv[1] + '/cells/v1/apps/' + '03' * 16 + '/cells/' + cell
control = json.loads(get('/comparison/' + cell_prefix + '/control.json'))
objects = cell_prefix + '/inc/' + control['incarnation'] + '/objects/'
root_path = objects + control['root']['digest'] + '.root'
raw = get('/comparison/' + root_path)
root = json.loads(raw)
expected = int(sys.argv[2])
if root['version'] != expected or root['cell'] != cell or root['incarnation'] != control['incarnation']:
    raise SystemExit('persisted root codec or scope differs from source and control')
packed = [segment for segment in root['segments'] if segment.get('packed')]
if expected == 2:
    if not packed:
        raise SystemExit('small-root fixture did not persist a packed dependency')
    body = get('/comparison/' + objects + packed[0]['object_digest'] + '.pack')
    if body[:8] != b'CRBPACK1' or len(body) > 256 * 1024:
        raise SystemExit('packed dependency has an invalid header or bound')
print(json.dumps({'root_path': root_path, 'root_sha256': hashlib.sha256(raw).hexdigest(), 'version': root['version'], 'packed_segments': len(packed), 'inline_directory': root.get('directory_inline') is not None, 'pass': True}))
'''
    cell = json.loads((directory / 'seeds.json').read_text())[0]['receipt']['cell']
    result = docker('exec', CONTROL, 'python3', '-c', script, prefix, expected, cell, timeout=90)
    put(directory / 'storage-format-smoke.json', json.loads(result.stdout))

def logs(name):
    return docker('logs', name, check=False).stdout + docker('logs', name, check=False).stderr

def wait_ready(name, marker=None, port=8080, limit=600):
    started = time.monotonic()
    while time.monotonic() - started < limit:
        state = json.loads(docker('inspect', name).stdout)[0]['State']
        if not state['Running']:
            raise RuntimeError(f'{name} exited: {logs(name)[-12000:]}')
        if marker:
            if marker in logs(name):
                return time.monotonic() - started
        else:
            try:
                if http('GET', '/.well-known/celld/health', port=port)['status'] == 200:
                    return time.monotonic() - started
            except Exception:
                pass
        time.sleep(1)
    raise RuntimeError(f'{name} readiness timeout: {logs(name)[-12000:]}')

def snapshot(directory, label, names):
    data = {}
    failures = []
    for name in names + [STORE, CONTROL]:
        data[name] = {'inspect': json.loads(docker('inspect', name).stdout)[0]}
        p = docker('exec', name, 'sh', '-c', 'cat /proc/1/limits; cat /proc/1/status; ls /proc/1/fd | wc -l; cat /sys/fs/cgroup/memory.events; cat /sys/fs/cgroup/cpu.stat', check=False)
        data[name]['process'] = p.stdout + p.stderr
        state = data[name]['inspect']['State']
        if state['OOMKilled'] or not state['Running']:
            failures.append(f'{name}: {state}')
    put(directory / f'{label}-resources.json', data)
    if failures:
        raise RuntimeError('infrastructure failed: ' + '; '.join(failures))

def sampler(directory, stop):
    with (directory / 'docker-stats.jsonl').open('w') as f:
        while not stop.is_set():
            try:
                p = docker('stats', '--no-stream', '--format', '{{json .}}', check=False, timeout=30)
            except Exception as error:
                f.write(json.dumps({'timestamp': time.time(), 'error': str(error)}) + '\n')
                f.flush()
                continue
            f.write(json.dumps({'timestamp': time.time(), 'stats': [json.loads(x) for x in p.stdout.splitlines() if x]}) + '\n')
            f.flush()
            stop.wait(3)

def start_node(system, durability, index, prefix, name):
    port = 8080 + index * 10
    args = ['run', '-d', '--name', name, '--network', 'host', '--cpus', '8', '--memory', '16g', '--memory-swap', '16g', '--ulimit', 'nofile=65536:65536', '--tmpfs', '/scratch:rw,size=4g', *CREDS]
    if system == 'cellule':
        args += ['-e', f'PARITY_RETAINED_BYTES={ARGS.retained_bytes}', '-e', f'PARITY_DISK_BYTES={ARGS.disk_bytes}', '-v', f'{BASE}:/work:ro', '-e', 'TMPDIR=/scratch', '-e', 'CELLULE_AXUM_CELLS=1000', '-e', 'CELLULE_AXUM_WORKERS=8', '-e', f'CELLULE_AXUM_BIND=127.0.0.1:{port}', '-e', 'CELLULE_TEST_ENDPOINT=http://127.0.0.1:9000', '-e', 'CELLULE_TEST_BUCKET=comparison', '-e', f'CELLULE_TEST_PREFIX={prefix}']
        if durability == 'fleet' or index == 0:
            args += ['-e', 'CELLULE_AXUM_FLEET_DIR=/scratch/fleet']
        if durability == 'bucket' and index == 0:
            args += ['-e', 'CELLULE_AXUM_BUCKET_ONLY=1']
        if index:
            args += ['-e', f'CELLULE_AXUM_FOLLOWER={index}']
        args += [RUST, 'sh', '-c', f"mkdir -m 700 /scratch/fleet && cp /work/tls/ca.crt /work/tls/node-{index}.crt /work/tls/node-{index}.key /scratch/fleet/ && exec /work/{MANIFEST['binaries']['sql']['path']}"]
    else:
        args += ['-e', f'CELLD_PLACEMENT_WEIGHT={(1000000000 if index == 0 else 1)}', '-e', 'CELLD_WATCH=/scratch', '-e', f'CELLD_DURABILITY={durability}', '-e', 'CELLD_SHUTDOWN_TOTAL_MS=600000', '-e', 'CELLD_TOKIO_THREADS=8', '-e', 'CELLD_REBALANCE_INTERVAL_MS=0', '-e', f"CELLD_NODE={name.replace('-cold', '-owner')}", '-e', 'RUST_LOG=warn,celld::node_log=info', CELLD, '--bucket', f's3://comparison/{prefix}', '--endpoint', 'http://127.0.0.1:9000', '--region', 'us-east-1', '--listen', f'127.0.0.1:{port}', '--internal-listen', f'127.0.0.1:{port + 1}', '--advertise', f'127.0.0.1:{port + 1}']
    docker(*args)
    return wait_ready(name, 'Follower service:' if index else 'Orders service:') if system == 'cellule' else wait_ready(name, port=port)

def stop_node(name, directory):
    started = time.monotonic()
    state = json.loads(docker('inspect', name).stdout)[0]['State']
    if state['Running']:
        if 'celld' in name:
            port = 8091 if name.endswith('peer-1') else 8101 if name.endswith('peer-2') else 8081
            response = http('POST', '/shutdown?handoff=preserve', port=port)
            put(directory / f'{name}-shutdown-response.json', response)
            if response['status'] != 200:
                raise RuntimeError('celld preserve shutdown was rejected')
        else:
            docker('kill', '--signal', 'SIGINT', name)
        p = docker('wait', name, timeout=120)
        (directory / f'{name}-exit.txt').write_text(p.stdout + p.stderr)
    (directory / f'{name}.log').write_text(logs(name))
    state = json.loads(docker('inspect', name).stdout)[0]['State']
    put(directory / f'{name}-final-state.json', state)
    if state['OOMKilled'] or state['ExitCode'] != 0:
        raise RuntimeError(f'{name} shutdown failed: {state}')
    return time.monotonic() - started

def driver(directory, label, config):
    configpath = directory / f'{label}-config.json'
    put(configpath, config)
    p = docker('exec', CONTROL, '/work/' + MANIFEST['binaries']['http_capacity']['path'], f'/work/{configpath.relative_to(BASE)}', check=False, timeout=int(config['seconds'] + config['warmup_seconds'] + 300))
    (directory / f'{label}-driver.log').write_text(p.stdout + p.stderr)
    dest = directory / label
    copy = docker('cp', f'{CONTROL}:/tmp/comparison-evidence', dest, check=False, timeout=300)
    if copy.returncode:
        raise RuntimeError(f'no capacity evidence: {copy.stderr}; {p.stdout} {p.stderr}')
    docker('exec', CONTROL, 'rm', '-rf', '/tmp/comparison-evidence')
    result = json.loads((dest / 'summary.json').read_text())
    result['driver_exit_code'] = p.returncode
    put(dest / 'summary.json', result)
    return result

def config(directory, label, write_rate, read_rate, offset=0, seed=True, seconds=30):
    return {'address': '127.0.0.1:8080', 'cells': 1000, 'concurrency': int(ARGS.concurrency), 'queue_capacity': int(ARGS.queue_capacity), 'write_rate': write_rate, 'read_rate': read_rate, 'warmup_seconds': ARGS.warmup if seed else 1, 'seconds': ARGS.seconds if seed else seconds, 'evidence_directory': '/tmp/comparison-evidence', 'seed_file': f'/work/{directory.relative_to(BASE)}/seeds.json' if seed else None, 'write_offset': offset, 'metrics_urls': [], 'metrics_tls_directory': None}

def contract(directory):
    now = int(time.time() * 1000)
    body = {'request_id': str(uuid.uuid4()), 'issued_at_ms': now, 'expires_at_ms': now + 7200000, 'id': 1000000000, 'total_cents': 13000000099, 'value': 'p' * 96}
    first = http('POST', '/orders', body)
    retry = http('POST', '/orders', body)
    conflict = http('POST', '/orders', dict(body, total_cents=1))
    expired = dict(body, request_id=str(uuid.uuid4()), id=1000001000, issued_at_ms=now - 10000, expires_at_ms=now - 1000)
    reject = http('POST', '/orders', expired)
    missing = http('GET', f"/orders/{expired['id']}")
    result = {'request': body, 'first': first, 'retry': retry, 'conflict': conflict, 'expired': reject, 'expired_read': missing}
    put(directory / 'contract.json', result)
    if first['status'] != 201 or retry != first or 200 <= conflict['status'] < 300 or (200 <= reject['status'] < 300) or (missing['body']['output'] is not None):
        raise RuntimeError(f'contract failed: {result}')
    return dict(first['body'], request=body)

def collect_observations(directory, extra):
    observations = {extra['output']['id']: extra}
    for path in [directory / 'initialize' / 'setup.jsonl', *sorted(directory.glob('write-*/client-*.jsonl'))]:
        for line in path.open():
            row = json.loads(line)
            if row.get('error') is None and row.get('response') and row.get('request'):
                response = row['response']
                oid = response['output']['id']
                if oid in observations:
                    raise RuntimeError(f'nonunique acknowledged id {oid}')
                observations[oid] = dict(response, request=row['request'])
    put(directory / 'acknowledged.json', list(observations.values()))
    return len(observations)

def audit(directory, label, cold):
    path = directory / f'{label}-config.json'
    put(path, {'address': '127.0.0.1:8080', 'observations': f'/work/{directory.relative_to(BASE)}/acknowledged.json', 'cold': cold})
    p = docker('exec', CONTROL, '/work/' + MANIFEST['binaries']['http_audit']['path'], f'/work/{path.relative_to(BASE)}', check=False, timeout=1200)
    (directory / f'{label}.log').write_text(p.stdout + p.stderr)
    try:
        result = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception:
        result = {'error': p.stdout + p.stderr}
    result['exit_code'] = p.returncode
    put(directory / f'{label}.json', result)
    if p.returncode:
        raise RuntimeError(f'{label} failed: {result}')
    return result

def run_case(system, durability):
    label = f'{system}-{durability}-parity-' + ARGS.tag
    directory = BASE / label
    directory.mkdir(exist_ok=False)
    prefix = label + '-' + uuid.uuid4().hex[:12]
    put(directory / 'case.json', {'system': system, 'durability': durability, 'prefix': prefix, 'framework_commit': MANIFEST['framework_revision'] if system == 'cellule' else 'f2bf648663a610eefde71f3547ad61e9b896b1f0', 'resident_cells': 1000, 'concurrency': int(ARGS.concurrency), 'queue_capacity': int(ARGS.queue_capacity), 'candidate_binary': MANIFEST['binaries']['sql']['path'], 'celld_application': 'celld-app', 'retained_budget_bytes': int(ARGS.retained_bytes), 'managed_disk_budget_bytes': int(ARGS.disk_bytes), 'value_bytes': 96, 'owner_cpus': 8, 'owner_memory_bytes': 16 * 1024 ** 3, 'followers': 2 if durability == 'fleet' else 0, 'profile': 'shared-vm-sql-ledger-96', 'provider_storage': 'fresh Linux Docker volume', 'telemetry': ARGS.telemetry, 'warmup_seconds': ARGS.warmup, 'seconds': ARGS.seconds, 'runner_sha256': RUNNER_SHA256, 'diagnostic': ARGS.seconds < 300 or ARGS.warmup < 30 or ARGS.telemetry == 'off'})
    names = []
    stop = threading.Event()
    sampling = None
    summary = {'schema_version': 1, 'case': label, 'writes': [], 'reads': [], 'completed': False, 'failure': None, 'build_manifest_sha256': hashlib.sha256((BASE / 'build.json').read_bytes()).hexdigest()}
    try:
        docker('restart', CONTROL)
        start_provider(directory / 'store-data')
        create_bucket()
        for attempt in range(60):
            try:
                if http('GET', '/health', port=9000)['status'] == 200:
                    break
            except Exception:
                pass
            time.sleep(1)
        ready = docker('exec', CONTROL, 'python3', '/work/wait-store-ready.py', timeout=70)
        put(directory / 'store-readiness.json', json.loads(ready.stdout))
        if system == 'celld':
            p = docker('run', '--rm', '--network', 'host', '--cpus', '2', '--memory', '2g', '--memory-swap', '2g', '--ulimit', 'nofile=65536:65536', '-v', f"{BASE}/{'celld-app'}:/app:ro", *CREDS, CELLD, 'deploy', '/app', '--bucket', f's3://comparison/{prefix}', '--endpoint', 'http://127.0.0.1:9000', '--region', 'us-east-1')
            (directory / 'deploy.log').write_text(p.stdout + p.stderr)
        elif durability == 'fleet':
            for i in [1, 2]:
                name = f'comparison-{label}-peer-{i}'
                names.append(name)
                start_node(system, durability, i, prefix, name)
        if system == 'celld' and durability == 'fleet':
            for i in [1, 2]:
                name = f'comparison-{label}-peer-{i}'
                names.append(name)
                start_node(system, durability, i, prefix, name)
        owner = f'comparison-{label}-owner'
        names.insert(0, owner)
        summary['startup_seconds'] = start_node(system, durability, 0, prefix, owner)
        sampling = threading.Thread(target=sampler, args=(directory, stop), daemon=True)
        sampling.start()
        initialize = driver(directory, 'initialize', config(directory, 'initialize', 0, 1, seed=False, seconds=1))
        summary['initialize'] = initialize
        seeds = []
        for line in (directory / 'initialize' / 'setup.jsonl').open():
            seeds.append(json.loads(line)['response'])
        put(directory / 'seeds.json', seeds)
        if system == 'cellule':
            storage_format_smoke(directory, prefix)
        if system == 'celld' and durability == 'fleet':
            end = time.monotonic() + 60
            while time.monotonic() < end:
                if any(('log ensemble open; fleet acks enabled' in line and names[1] in line and (names[2] in line) for line in logs(owner).splitlines())):
                    break
                time.sleep(1)
            (directory / 'ensemble-ready.log').write_text(logs(owner))
            if not any(('log ensemble open; fleet acks enabled' in line and names[1] in line and (names[2] in line) for line in logs(owner).splitlines())):
                raise RuntimeError('celld fleet ensemble did not form with both selected followers')
        extra = contract(directory)
        if system == 'celld':
            states = {str(i): http('GET', '/state', port=8081 + i * 10) for i in range(3 if durability == 'fleet' else 1)}
            put(directory / 'placement-before.json', states)
            if states['0']['body']['owned_cells'] != 1000 or any((states[str(i)]['body']['owned_cells'] for i in range(1, 3 if durability == 'fleet' else 1))):
                raise RuntimeError('celld placement is not one owner of 1000 Cells')
        snapshot(directory, 'before', names)
        for n, rate in enumerate(ARGS.write_rates or [30, 2000 if durability == 'bucket' else 15000]):
            point = config(directory, f'write-{rate}', rate, ARGS.read_rate, offset=n * 100000000)
            if system == 'cellule' and ARGS.telemetry == 'window':
                point['metrics_urls'] = ['http://127.0.0.1:8080/debug/metrics'] + ([f'https://127.0.0.1:{port}/debug/metrics' for port in (8090, 8100)] if durability == 'fleet' else [])
                point['metrics_tls_directory'] = '/work/tls'
            point['hot_read_cells'] = ARGS.hot_read_cells
            r = driver(directory, f'write-{rate}', point)
            summary['writes'].append(r)
            put(directory / 'progress.json', summary)
            w = r['writes']
            p99 = w['scheduled_latency_ms_all_attempts']['p99']
            slo = 50 if durability == 'fleet' else 200
            passed = r['driver_exit_code'] == 0 and w['errors'] == 0 and (w['warmup_errors'] == 0) and (w['generated_offers'] == w['planned_offers']) and (w['queue_dropped'] == 0) and (w['warmup_queue_dropped'] == 0) and (w['successes_in_window'] >= w['planned_offers'] * 0.99) and (p99 is not None) and (p99 <= slo)
            r['delivery_latency_pass'] = passed
            put(directory / 'progress.json', summary)
        if ARGS.overload_capacity:
            # No intervening warmup: the recovery window starts immediately
            # after the offered overload. Audit journals include both cohorts.
            phases = [('overload', math.ceil(ARGS.overload_capacity * 1.5), 60),
                      ('recovery', max(1, ARGS.overload_capacity // 2), 30)]
            summary['overload'] = {'reference_capacity': ARGS.overload_capacity,
                                   'reference_requires_paired_qualification': True}
            for index, (phase, rate, seconds) in enumerate(phases):
                point = config(directory, phase, rate, 0,
                               offset=(100 + index) * 100000000, seconds=seconds)
                point['seconds'] = seconds
                point['warmup_seconds'] = 0
                point['phase'] = phase
                r = driver(directory, 'write-' + phase, point)
                summary['overload'][phase] = r
                put(directory / 'progress.json', summary)
        summary['acknowledged_rows'] = collect_observations(directory, extra)
        summary['warm_audit'] = audit(directory, 'warm-audit', False)
        snapshot(directory, 'after', names)
        if system == 'celld':
            put(directory / 'placement-after.json', {str(i): http('GET', '/state', port=8081 + i * 10) for i in range(3 if durability == 'fleet' else 1)})
        stop.set()
        sampling.join(timeout=40)
        drain_started = time.monotonic()
        summary['drain_node_seconds'] = {name: stop_node(name, directory) for name in names}
        summary['drain_seconds'] = time.monotonic() - drain_started
        if summary['drain_seconds'] > 120:
            raise RuntimeError('original fleet drain exceeded 120 seconds')
        for name in names:
            docker('rm', name)
        names = []
        owner = f'comparison-{label}-cold'
        names.append(owner)
        summary['cold_startup_seconds'] = start_node(system, 'bucket', 0, prefix, owner)
        summary['cold_audit'] = audit(directory, 'cold-audit', True)
        saved = json.loads((directory / 'contract.json').read_text())
        retry = http('POST', '/orders', saved['request'])
        put(directory / 'cold-retry.json', retry)
        if retry['status'] != 201 or retry['body']['output'] != saved['first']['body']['output']:
            raise RuntimeError('cold retry did not preserve acknowledged output')
        summary['cold_retry_pass'] = True
        stop_node(owner, directory)
        docker('rm', owner)
        names = []
        summary['completed'] = True
    except Exception as e:
        summary['completed'] = False
        summary['failure'] = str(e)[:2000]
        (directory / 'failure.txt').write_text(traceback.format_exc())
        print(f'FAILED {label}: {str(e)[:500]}', flush=True)
    finally:
        stop.set()
        if sampling:
            sampling.join(timeout=40)
        for name in names:
            try:
                stop_node(name, directory)
            except Exception as e:
                summary.setdefault('cleanup_failures', []).append(str(e))
                (directory / f'{name}.log').write_text(logs(name))
                docker('stop', '--time', '2', name, check=False)
        (directory / 'store.log').write_text(logs(STORE))
        put(directory / 'summary.json', summary)
        print(json.dumps({'case': label, 'completed': summary['completed'], 'failure': str(summary.get('failure'))[:500], 'write_rates': [x['writes']['successful_requests_per_second'] for x in summary['writes']], 'read_rates': [x.get('successful_requests_per_second') for x in summary['reads']]}), flush=True)
    return summary

def remove_owned(name):
    previous = docker('inspect', name, check=False)
    if previous.returncode == 0:
        value = json.loads(previous.stdout)[0]
        if value['Config'].get('Labels', {}).get('cellule.perf.artifacts') != str(BASE):
            raise RuntimeError(f'{name} belongs to another run')
        docker('rm', '-f', name)


def start_provider(data_directory):
    # A fresh prefix does not reset a provider's old object inventory. Preserve
    # each case's origin data outside the repository and reset the provider's
    # backing directory before the next case, including A/A repetitions.
    remove_owned(STORE)
    data_directory.mkdir(parents=True, exist_ok=False)
    volume = STORE + '-data-' + hashlib.sha256(str(data_directory).encode()).hexdigest()[:12]
    if docker('volume', 'inspect', volume, check=False).returncode == 0:
        raise RuntimeError('provider volume is not fresh: ' + volume)
    docker('volume', 'create', '--label', 'cellule.perf.artifacts=' + str(BASE), volume)
    put(data_directory / 'volume.json', {'name': volume, 'storage': 'Docker Linux filesystem', 'retained': True})
    docker('run', '-d', '--name', STORE, '--label', 'cellule.perf.artifacts=' + str(BASE),
           '--network', 'host', '--cpus', '2', '--memory', '2g', '--memory-swap', '2g',
           '--ulimit', 'nofile=65536:65536', '-e', 'RUSTFS_ACCESS_KEY=benchmark_access',
           '-e', 'RUSTFS_SECRET_KEY=benchmark_secret_private',
           '-v', f'{volume}:/data', MANIFEST['images']['store'], '/data')


def create_bucket():
    for _ in range(60):
        try:
            result = docker('exec', CONTROL, 'python3', '/work/control.py', 'bucket', check=False, timeout=10)
            if result.returncode == 0:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('fixture bucket creation failed')


def provision():
    remove_owned(CONTROL)
    docker('run', '-d', '--name', CONTROL, '--label', 'cellule.perf.artifacts=' + str(BASE),
           '--network', 'host', '--cpus', '4', '--memory', '4g', '-v', f'{BASE}:/work',
           RUST, 'sleep', 'infinity')
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Paired, pinned SQL durability verification; raw evidence stays outside the repository.')
    parser.add_argument('--context', required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--tag', default=uuid.uuid4().hex[:10])
    parser.add_argument('--seconds', type=int, default=300)
    parser.add_argument('--warmup', type=int, default=30)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--write-rates', type=int, nargs='+')
    parser.add_argument('--read-rate', type=int, default=0)
    parser.add_argument('--hot-read-cells', type=int)
    parser.add_argument('--telemetry', choices=['window', 'off'], default='window',
                        help='off is an exporter-overhead diagnostic and cannot qualify')
    parser.add_argument('--concurrency', type=int, default=128)
    parser.add_argument('--queue-capacity', type=int, default=128)
    parser.add_argument('--retained-bytes', type=int, default=67108864)
    parser.add_argument('--disk-bytes', type=int, default=1073741824)
    parser.add_argument('--overload-capacity', type=int,
                        help='after steady points offer 1.5x this qualified capacity for 60s, then 0.5x for 30s')
    parser.add_argument('cases', nargs='*', choices=['cellule-fleet', 'celld-fleet', 'cellule-bucket', 'celld-bucket'])
    ARGS = parser.parse_args()
    BASE = ARGS.artifacts.resolve()
    CTX = ARGS.context
    if BASE == Path(__file__).resolve().parents[2] or Path(__file__).resolve().parents[2] in BASE.parents:
        parser.error('artifacts must stay outside the repository')
    if not 1 <= ARGS.repetitions <= 10 or not 1 <= ARGS.seconds <= 3600 or (not 1 <= ARGS.warmup <= 60):
        parser.error('invalid bounded duration/repetition')
    if ARGS.overload_capacity is not None and ARGS.overload_capacity <= 0:
        parser.error('overload capacity must be positive')
    DOCKER = ['docker', '--context', CTX]
    MANIFEST = json.loads((BASE / 'build.json').read_text())
    if MANIFEST.get('schema_version') != 2 or not MANIFEST.get('build_source_sha256'):
        parser.error('rebuild with source-content-isolated caches before running this harness')
    for binary in MANIFEST['binaries'].values():
        if hashlib.sha256((BASE / binary['path']).read_bytes()).hexdigest() != binary['sha256']:
            parser.error('binary hash differs from build manifest')
    prefix = 'cellule-perf-' + hashlib.sha256(str(BASE).encode()).hexdigest()[:10]
    CONTROL = prefix + '-control'
    STORE = prefix + '-store'
    lock_path = Path('/tmp') / ('cellule-perf-' + hashlib.sha256(CTX.encode()).hexdigest()[:16] + '.lock')
    benchmark_lock = lock_path.open('a')
    try:
        fcntl.flock(benchmark_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Another comparison owns this Docker context')
    provision()
    original_tag = ARGS.tag
    results = []
    try:
        for repeat in range(ARGS.repetitions):
            ARGS.tag = original_tag + '-r' + str(repeat + 1)
            cases = ARGS.cases or ['cellule-fleet', 'celld-fleet', 'cellule-bucket', 'celld-bucket']
            if repeat % 2:
                cases = list(reversed(cases))
            for spec in cases:
                system, durability = spec.split('-')
                results.append(run_case(system, durability))
    finally:
        for name in (CONTROL, STORE):
            docker('stop', '--time', '10', name, check=False)
    put(BASE / (original_tag + '-runs.json'), results)
    if any((not result['completed'] for result in results)):
        raise SystemExit(1)
