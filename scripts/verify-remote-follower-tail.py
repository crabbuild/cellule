#!/usr/bin/env python3
import argparse
import hashlib
import json
import pathlib
import subprocess
import shutil
import shlex
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description='Verify a remote unpublished ACK after owner loss or follower TCP partition on three separate 1-vCPU/2-GiB Docker hosts.')
parser.add_argument('--artifacts', type=pathlib.Path, required=True)
parser.add_argument('--binary', type=pathlib.Path, required=True, help='release SQL example test executable, not the ordinary SQL server')
parser.add_argument('--context', required=True, help='separate provider/client support Docker context')
parser.add_argument('--node-contexts', nargs=3, required=True, metavar=('OWNER', 'FOLLOWER1', 'FOLLOWER2'))
parser.add_argument('--tag', required=True, help='new isolated object prefix and container-name suffix')
parser.add_argument('--fault', choices=['owner-process-loss', 'peer-partition'], default='owner-process-loss')
parser.add_argument('--owner-ssh-config', type=pathlib.Path, help='SSH configuration for the owner VM; required for peer-partition')
parser.add_argument('--owner-ssh-host', help='owner VM SSH alias; required for peer-partition')
args = parser.parse_args()
if args.fault == 'peer-partition' and (not args.owner_ssh_config or not args.owner_ssh_host):
    parser.error('peer-partition requires owner VM SSH configuration and alias')
BASE = args.artifacts.resolve()
if BASE.is_relative_to(ROOT):
    parser.error('evidence must be outside the source checkout')
BASE.mkdir(parents=True, exist_ok=True)
TAG = args.tag
if not TAG or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-' for c in TAG):
    parser.error('tag must contain lowercase letters, digits and hyphens')
CASE = BASE / TAG
CASE.mkdir(exist_ok=False)
SUPPORT = args.context
CONTEXTS = args.node_contexts
(BASE / 'bin').mkdir(exist_ok=True)
if args.binary.resolve() != BASE / 'bin/sql_fault':
    shutil.copy2(args.binary, BASE / 'bin/sql_fault')
if not (BASE / 'tls').exists():
    subprocess.run(['python3', str(ROOT / 'scripts/generate-capacity-tls.py'), str(BASE / 'tls')], check=True)
for name in ['control.py', 'wait-store-ready.py']:
    shutil.copy2(ROOT / 'scripts/perf' / name, BASE / name)
RUST = 'rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e'
STORE = 'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858'
SELECTOR = 'fleet::remote_fault_tests::remote_crash_fixture_child'
owned = []
partition_rules = set()
status = dict(phase='setup', started_at=time.time(), performance_qualification=False,
              fault=args.fault,
              transport='original pinned mTLS + signed grants + sealed remote tail',
              scope='three independent 1-CPU/2-GiB VMs, native ext4; not physical power loss',
              joins=[], ack_count=0, audit_count=0)


def save():
    temporary = CASE / 'lifecycle.json.tmp'
    temporary.write_text(json.dumps(status, indent=2) + '\n')
    temporary.replace(CASE / 'lifecycle.json')


def docker(context, *arguments, check=True, timeout=25):
    result = subprocess.run(['docker', '--context', context, *arguments], text=True,
                            capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'Docker {context} {arguments[:2]} failed: {result.stderr}')
    return result


def wait_until(predicate, seconds, label):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise RuntimeError('Fixture guard expired: ' + label)


def owner_ssh(*command):
    result = subprocess.run(['ssh', '-F', str(args.owner_ssh_config.resolve()),
                             '-o', 'BatchMode=yes', args.owner_ssh_host, shlex.join(command)],
                            text=True, capture_output=True, timeout=25)
    if result.returncode:
        raise RuntimeError('Owner VM SSH command failed: ' + result.stderr)
    return result.stdout


def partition_rule(port, install):
    rule = ['OUTPUT', '-p', 'tcp', '-d', '127.0.0.1', '--dport', str(port),
            '-m', 'comment', '--comment', 'cellule-' + TAG + '-partition', '-j', 'DROP']
    if install:
        # Record before the command: an SSH reply loss may leave the rule installed.
        partition_rules.add(port)
        owner_ssh('sudo', '-n', 'iptables', '-I', *rule)
    else:
        owner_ssh('sudo', '-n', 'iptables', '-D', *rule)
        partition_rules.remove(port)


def partition_snapshot(label, ports):
    raw = owner_ssh('sudo', '-n', 'iptables', '-L', 'OUTPUT', '-n', '-v', '-x')
    (CASE / (label + '-iptables.txt')).write_text(raw)
    counters = {}
    for line in raw.splitlines():
        if 'cellule-' + TAG + '-partition' not in line:
            continue
        for port in ports:
            if 'dpt:' + str(port) in line:
                counters[port] = int(line.split()[0])
    assert all(counters.get(port, 0) > 0 for port in ports), 'no actual dropped TCP packets'
    status.setdefault('partition_observations', []).append(dict(label=label, dropped_packets=counters))
    save()


def signal(context, name, file):
    code = "import os,pathlib; p=pathlib.Path('/scratch')/" + repr(file) + "; f=p.open('x'); f.write('ready\\n'); f.flush(); os.fsync(f.fileno()); f.close()"
    docker(context, 'exec', name, 'python3', '-c', code)


def native_snapshot(context, name):
    code = """import pathlib,json,subprocess
p=pathlib.Path('/proc')
print(json.dumps(dict(boot_id=(p/'sys/kernel/random/boot_id').read_text().strip(),
memory=(p/'meminfo').read_text(),swaps=(p/'swaps').read_text(),
oom_kill=dict(x.split() for x in (p/'vmstat').read_text().splitlines())['oom_kill'],
filesystem=subprocess.check_output(['df','-PT','/scratch'],text=True),
memory_max=pathlib.Path('/sys/fs/cgroup/memory.max').read_text().strip(),
cpu_max=pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
memory_events=pathlib.Path('/sys/fs/cgroup/memory.events').read_text())))
"""
    data = json.loads(docker(context, 'exec', name, 'python3', '-c', code).stdout)
    assert ' ext4 ' in data['filesystem'] and 'virtiofs' not in data['filesystem']
    assert data['memory_max'] == str(2 << 30)
    assert data['cpu_max'].split() == ['100000', '100000']
    assert len(data['swaps'].splitlines()) == 1
    assert 'oom_kill 0' in data['memory_events']
    return data


def start_role(role, index):
    context = CONTEXTS[index]
    name = f'cellule-{TAG}-{role}-{index}'
    volume = name + '-native'
    if docker(context, 'volume', 'inspect', volume, check=False).returncode == 0:
        raise RuntimeError('Native state volume is not fresh: ' + volume)
    docker(context, 'volume', 'create', '--label', 'cellule.fault.artifacts=' + str(CASE), volume)
    run_arguments = ['run', '-d', '--name', name, '--label', 'cellule.fault.artifacts=' + str(CASE),
            '--network', 'host', '--cpus', '1', '--memory', '2g', '--memory-swap', '2g',
            '--ulimit', 'nofile=65536:65536', '-v', volume + ':/scratch', '-v', str(BASE) + ':/work:ro',
            '-e', 'AWS_ACCESS_KEY_ID=benchmark_access', '-e', 'AWS_SECRET_ACCESS_KEY=benchmark_secret_private',
            '-e', 'AWS_DEFAULT_REGION=us-east-1', '-e', 'CELLULE_TEST_ENDPOINT=http://127.0.0.1:9000',
            '-e', 'CELLULE_TEST_BUCKET=comparison', '-e', 'CELLULE_TEST_PREFIX=' + TAG,
            '-e', 'CELLULE_TEST_DEVICE_PATH=/scratch', '-e', 'TMPDIR=/scratch',
            '-e', 'CELLULE_AXUM_FLEET_DIR=/scratch/fleet', '-e', 'CELLULE_REMOTE_FAULT_ROLE=' + role,
            '-e', 'CELLULE_REMOTE_FAULT_KIND=' + args.fault,
            '-e', 'TOKIO_WORKER_THREADS=1', '-e', 'RUST_BACKTRACE=1']
    if index:
        run_arguments += ['-e', 'CELLULE_AXUM_FOLLOWER=' + str(index)]
    if role == 'recover':
        run_arguments += ['-e', 'CELLULE_REMOTE_FAULT_ACK=' + json.dumps(status['ack'])]
    run_arguments += [RUST, 'sh', '-c', f'mkdir -m 700 /scratch/fleet && cp /work/tls/ca.crt /work/tls/node-{index}.crt /work/tls/node-{index}.key /scratch/fleet/ && exec /work/bin/sql_fault {SELECTOR} --exact --ignored --nocapture']
    # Track ownership before the RPC: a failed Docker response can be ambiguous.
    owned.append((context, name))
    docker(context, *run_arguments)
    status.setdefault('roles', []).append(dict(role=role, index=index, context=context, name=name, volume=volume))
    save()
    return context, name


def marker(context, name, file):
    result = docker(context, 'exec', name, 'cat', '/scratch/' + file, check=False)
    if result.returncode == 0:
        return json.loads(result.stdout)
    state = json.loads(docker(context, 'inspect', name).stdout)[0]['State']
    if not state['Running']:
        raise RuntimeError(f'Fixture role exited before {file}: {name}, exit {state["ExitCode"]}')
    return None


def joined(context, name, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        info = json.loads(docker(context, 'inspect', name).stdout)[0]
        if not info['State']['Running']:
            status['joins'].append(dict(context=context, name=name, state=info['State']))
            assert not info['State']['OOMKilled']
            return info['State']['ExitCode']
        time.sleep(0.5)
    raise RuntimeError('Process did not join: ' + name)


try:
    save()
    listed = docker(SUPPORT, 'run', '--rm', '-v', str(BASE) + ':/work:ro', RUST, '/work/bin/sql_fault', SELECTOR, '--exact', '--ignored', '--list').stdout
    assert SELECTOR + ': test' in listed and '1 test, 0 benchmarks' in listed
    status['binary_sha256'] = hashlib.sha256((BASE / 'bin/sql_fault').read_bytes()).hexdigest()
    info = {context: json.loads(docker(context, 'info', '--format', '{{json .}}').stdout)
            for context in CONTEXTS}
    assert len({value['ID'] for value in info.values()}) == 3
    assert all(value['NCPU'] == 1 and 1.8 * (1 << 30) < value['MemTotal'] <= 2 << 30
               for value in info.values())
    (CASE / 'node-hosts.json').write_text(json.dumps(info, indent=2) + '\n')
    store = 'cellule-' + TAG + '-store'
    volume = store + '-native'
    assert docker(SUPPORT, 'volume', 'inspect', volume, check=False).returncode != 0
    docker(SUPPORT, 'volume', 'create', '--label', 'cellule.fault.artifacts=' + str(CASE), volume)
    owned.append((SUPPORT, store))
    docker(SUPPORT, 'run', '-d', '--name', store, '--label', 'cellule.fault.artifacts=' + str(CASE),
           '--network', 'host', '--cpus', '2', '--cpuset-cpus', '0-1', '--memory', '2g',
           '--memory-swap', '2g', '-v', volume + ':/data', '-e', 'RUSTFS_ACCESS_KEY=benchmark_access',
           '-e', 'RUSTFS_SECRET_KEY=benchmark_secret_private', '-e', 'MIMALLOC_ALLOW_THP=0', STORE, '/data')
    control = BASE / 'control.py'
    wait_until(lambda: docker(SUPPORT, 'run', '--rm', '--network', 'host', '-v',
                              str(control) + ':/control.py:ro', RUST, 'python3', '/control.py',
                              'bucket', check=False).returncode == 0, 60, 'real bucket creation')
    ready = BASE / 'wait-store-ready.py'
    result = docker(SUPPORT, 'run', '--rm', '--network', 'host', '-v', str(ready) + ':/ready.py:ro',
                    RUST, 'python3', '/ready.py', timeout=70)
    (CASE / 'provider-ready.json').write_text(result.stdout)
    followers = [start_role('follower', index) for index in (1, 2)]
    for context, name in followers:
        wait_until(lambda: 'Follower service:' in docker(context, 'logs', name).stdout,
                   60, 'follower service ready')
    owner = start_role('owner', 0)
    status['phase'] = 'awaiting_unpublished_remote_ack'
    save()
    wait_until(lambda: marker(*owner, 'acknowledged.json') is not None, 45, 'remote ACK')
    status['ack'] = marker(*owner, 'acknowledged.json')
    assert status['ack']['commit_sequence'] == 1 and status['ack']['bucket_commit_sequence'] == 0
    assert status['ack']['required_followers'] == 2 and status['ack']['held_bundle_upload']
    status['ack_count'] = 1
    before = {name: native_snapshot(context, name) for context, name in [owner, *followers]}
    assert len({value['boot_id'] for value in before.values()}) == 3
    (CASE / 'native-before-crash.json').write_text(json.dumps(before, indent=2) + '\n')
    if args.fault == 'peer-partition':
        assert owner_ssh('cat', '/proc/sys/kernel/random/boot_id').strip() == before[owner[1]]['boot_id']
        partition_rule(8090, True)
        partition_rule(8100, True)
        signal(*owner, 'partition-installed')
        status['phase'] = 'checking_partition_output_gate'
        save()
        wait_until(lambda: marker(*owner, 'partition-gated.json') is not None, 10, 'partitioned output gate')
        gated = marker(*owner, 'partition-gated.json')
        assert gated['second_handler_executed'] and gated['second_capture_assigned']
        assert not gated['second_success'] and not gated['read_success']
        assert gated['bucket_commit_sequence'] == 0
        (CASE / 'partition-gated.json').write_text(json.dumps(gated, indent=2) + '\n')
    status['phase'] = 'killing_owner'
    save()
    docker(*owner[:1], 'kill', '--signal', 'KILL', owner[1])
    assert joined(*owner, 10) == 137
    if args.fault == 'peer-partition':
        partition_snapshot('both-followers-blocked', [8090, 8100])
    # The embedding example bounds its live inventory to the three-node fleet.
    # Enroll the replacement only after actual old-boot lease expiry, using
    # wall time; never fake a future recovery timestamp or relax that bound.
    status['phase'] = 'waiting_real_owner_lease_expiry'
    save()
    wait_until(lambda: time.time() * 1000 > status['ack']['owner_expires_at_ms'] + 1000,
               40, 'real predecessor lease expiry before successor enrollment')
    successor = start_role('recover', 0)
    successor_native = native_snapshot(*successor)
    assert successor_native['boot_id'] == before[owner[1]]['boot_id']
    assert successor_native['oom_kill'] == before[owner[1]]['oom_kill']
    (CASE / 'native-successor.json').write_text(json.dumps(successor_native, indent=2) + '\n')
    status['phase'] = 'recovering_remote_tail'
    save()
    if args.fault == 'peer-partition':
        wait_until(lambda: marker(*successor, 'recovery-refused.json') is not None,
                   65, 'recovery refuses missing complete follower witness')
        refused = marker(*successor, 'recovery-refused.json')
        assert refused['missing_witness_refused'] and refused['no_takeover'] and refused['no_overlay']
        assert refused['bucket_commit_sequence'] == 0
        (CASE / 'recovery-refused.json').write_text(json.dumps(refused, indent=2) + '\n')
        partition_snapshot('recovery-refused', [8090, 8100])
        partition_rule(8090, False)
        signal(*successor, 'one-witness-reachable')
        wait_until(lambda: marker(*successor, 'recovered-before-drain.json') is not None,
                   45, 'one complete reachable follower recovers original ACK')
        recovered = marker(*successor, 'recovered-before-drain.json')
        assert recovered['value'] == recovered['commit_sequence'] == 1
        assert recovered['exact_retry'] and recovered['single_reachable_witness']
        (CASE / 'recovered-before-drain.json').write_text(json.dumps(recovered, indent=2) + '\n')
        partition_snapshot('one-follower-still-blocked', [8100])
        partition_rule(8100, False)
        signal(*successor, 'partition-healed')
    assert joined(*successor, 100) == 0
    # Read the marker from the stopped container without reusing its database.
    docker(successor[0], 'cp', successor[1] + ':/scratch/recovered.json', str(CASE / 'recovered.json'))
    recovered = json.loads((CASE / 'recovered.json').read_text())
    assert recovered['commit_sequence'] == recovered['value'] == 1
    assert recovered['exact_retry'] and not recovered['reexecution']
    assert recovered['runtime_joined'] and recovered['owner_joined']
    status['audit_count'] = 1
    for context, name in followers:
        after = native_snapshot(context, name)
        assert after['boot_id'] == before[name]['boot_id']
        assert after['oom_kill'] == before[name]['oom_kill']
        status.setdefault('native_followers_after_recovery', {})[name] = after
        docker(context, 'kill', '--signal', 'INT', name)
        assert joined(context, name, 90) == 0
    status['phase'] = 'passed'
except Exception as error:
    status.update(failure_phase=status['phase'], phase='failed', error=repr(error))
finally:
    # Even a failed scenario has to join every accepted process and retain logs.
    cleanup_errors = []
    for port in sorted(partition_rules.copy()):
        try:
            partition_rule(port, False)
        except Exception as error:
            cleanup_errors.append(dict(partition_port=port, error=repr(error)))
    # Keep the real provider alive until node shutdown has joined its I/O.
    for context, name in reversed(owned):
        try:
            inspection = docker(context, 'inspect', name, check=False)
            if inspection.returncode:
                if 'No such object' in inspection.stderr or 'No such container' in inspection.stderr:
                    continue
                raise RuntimeError(inspection.stderr)
            info = json.loads(inspection.stdout)[0]
            if info['State']['Running']:
                docker(context, 'stop', '--time', '10', name, timeout=25)
            result = docker(context, 'logs', name, check=False)
            (CASE / (name + '.log')).write_text(result.stdout + result.stderr)
            terminal = json.loads(docker(context, 'inspect', name).stdout)[0]
            assert not terminal['State']['Running'] and not terminal['State']['OOMKilled']
            (CASE / (name + '-terminal.json')).write_text(json.dumps(terminal, indent=2) + '\n')
            if status['phase'] == 'passed':
                expected_exit = 137 if name == owner[1] else 0
                assert terminal['State']['ExitCode'] == expected_exit
        except Exception as error:
            cleanup_errors.append(dict(context=context, name=name, error=repr(error)))
    if cleanup_errors:
        status.update(phase='failed', cleanup_errors=cleanup_errors)
    status.update(finished_at=time.time(), all_owned_containers_terminal=not cleanup_errors)
    save()
print(json.dumps(status, indent=2))
raise SystemExit(0 if status['phase'] == 'passed' else 1)
