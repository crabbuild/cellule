#!/usr/bin/env python3
"""Isolated causal probes; never changes the canonical qualification profile."""
import argparse
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('three_node', ROOT / 'scripts/perf/three-node.py')
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def build(args):
    reference = args.reference.resolve(strict=True)
    output = args.artifacts.resolve()
    bench.require(output != ROOT and ROOT not in output.parents, 'use external artifacts')
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(reference / 'source', output / 'source')
    # Independent copies: Cargo must never overwrite a measured reference binary.
    shutil.copytree(reference / 'target', output / 'target')
    path = output / 'source/crates/cellule-axum/examples/sql.rs'
    original = path.read_text()
    old = 'let host = Host::default().with_local_disk_budget(DiskBudget::new(1 << 30));'
    new = ('let host = Host::default()\n'
           '        .with_dirty_slots(Arc::new(tokio::sync::Semaphore::new(usize::try_from(\n'
           '            example_count("CELLULE_DIAGNOSTIC_DIRTY_SLOTS", 1, 64)?,\n'
           '        )?)))\n'
           '        .with_local_disk_budget(DiskBudget::new(1 << 30));')
    bench.require(original.count(old) == 1, 'reference host construction changed')
    path.write_text(original.replace(old, new))
    proof = json.loads((reference / 'source.json').read_text())
    proof['diagnostic'] = dict(reference_manifest_sha256=bench.sha(reference / 'source.json'),
                              source_patch=dict(path=str(path.relative_to(output / 'source')),
                                                before=old, after=new),
                              scope='bounded admission probe; production algorithms and proof gates unchanged')
    proof['files'] = {str(f.relative_to(output / 'source')): bench.sha(f)
                      for f in (output / 'source').rglob('*') if f.is_file()}
    bench.put(output / 'source.json', proof)
    command = ['docker', '--context', args.context, 'run', '--rm', '--cpus', '8', '--memory', '12g',
               '-v', f'{output}:/work', '-v', f'{args.registry.resolve()}:/usr/local/cargo/registry',
               '-w', '/work/source', '-e', 'CARGO_TARGET_DIR=/work/target', bench.RUST,
               'cargo', 'build', '--release', '--locked', '-p', 'cellule-axum', '--example', 'sql']
    bench.put(output / 'build-command.json', command)
    with (output / 'build.log').open('x') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    bench.require(bench.sha(reference / 'target/release/examples/http_capacity') ==
                  bench.sha(output / 'target/release/examples/http_capacity'), 'driver changed')
    bench.put(output / 'binaries.json', {name: bench.sha(output / 'target/release/examples' / name)
                                       for name in ('sql', 'http_capacity')})


class Probe(bench.Run):
    def __init__(self, args):
        super().__init__(args)
        self.summary['scope'] = 'causal diagnostic; not sustainable capacity or qualification'
        self.summary['diagnostic'] = dict(provider_memory_gib=args.provider_memory,
                                          dirty_slots=args.dirty_slots,
                                          native_compaction_txids=args.native_compaction_txids,
                                          changed_variable=args.changed_variable)
        self.inventoried = False
        for name in ('diagnose.py', 'inventory.py'):
            shutil.copy2(Path(__file__).with_name(name), self.output / name)
        self.summary['diagnostic_helpers_sha256'] = {
            name: bench.sha(self.output / name) for name in ('diagnose.py', 'inventory.py')}

    def launch(self, name, cpus, memory, cpuset, extra, image, command):
        if name == self.store:
            memory = self.args.provider_memory
        if name in self.nodes and self.args.dirty_slots is not None:
            extra = [*extra, '-e', f'CELLULE_DIAGNOSTIC_DIRTY_SLOTS={self.args.dirty_slots}']
        if name in self.nodes and self.args.native_compaction_txids is not None:
            extra = [*extra, '-e', f'CELLD_LTX_COMPACTION_MIN_TXIDS={self.args.native_compaction_txids}']
        return super().launch(name, cpus, memory, cpuset, extra, image, command)

    def snapshot(self, name):
        result = super().snapshot(name)
        # Separate anon/file/slab and expose reclaim/IO pressure. CPU/memory caps
        # and all canonical correctness/resource assertions still apply.
        fields = ['memory.stat', 'memory.pressure', 'io.pressure', 'cpu.pressure', 'io.stat']
        raw = self.docker('exec', name, 'sh', '-c',
                          'for n in ' + ' '.join(fields) +
                          '; do echo "$n"; cat "/sys/fs/cgroup/$n"; echo; echo END; done').stdout
        for field in fields:
            match = bench.re.search(r'(?:^|\n)' + bench.re.escape(field) + r'\n(.*?)\nEND', raw, bench.re.S)
            bench.require(match is not None, 'missing diagnostic resource evidence: ' + field)
            result['cgroup'][field] = match.group(1).strip()
        return result

    def control(self, operation, expected_writes=0, write_offset=0):
        if operation == 'audit' and not self.inventoried:
            # Load has drained; inventory requests never run in a load window.
            # Native replication appends `cells/` to the fixture prefix while
            # ownership normalizes a trailing slash. Cover both object namespaces.
            raw = self.docker('exec', self.client, 'python3', '/evidence/inventory.py',
                              self.tag, timeout=300).stdout
            bench.put(self.output / 'provider-inventory.json', json.loads(raw))
            self.inventoried = True
        return super().control(operation, expected_writes, write_offset)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['build', 'run'])
    parser.add_argument('--context', required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--registry', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--implementation', choices=['cellule', 'celld-native'], default='cellule')
    parser.add_argument('--provider-memory', type=int, choices=[2, 4], default=2)
    parser.add_argument('--dirty-slots', type=int, choices=[1, 4, 8])
    parser.add_argument('--native-compaction-txids', type=int, choices=[8, 256])
    parser.add_argument('--changed-variable', required=True)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--write-rates', type=int, nargs='+', default=[600])
    parser.add_argument('--read-rates', type=int, nargs='+', default=[])
    parser.add_argument('--disaster-recovery', action='store_true')
    args = parser.parse_args()
    bench.require(1 <= args.seconds <= 3600 and 0 <= args.warmup <= 60 and
                  all(1 <= r <= 100000 for r in args.write_rates + args.read_rates), 'invalid load bounds')
    lock_path = Path('/tmp') / ('cellule-perf-' + hashlib.sha256(args.context.encode()).hexdigest()[:16] + '.lock')
    with lock_path.open('a') as lock:
        print(json.dumps(dict(event='waiting-for-comparison-lock')), flush=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.operation == 'build':
            bench.require(args.reference is not None and args.registry is not None, 'build needs reference and registry')
            bench.require(not subprocess.check_output(['docker', '--context', args.context, 'ps', '-q']).strip(),
                          'build requires idle context')
            build(args)
        else:
            bench.require(args.output is not None, 'run needs output')
            bench.require(sum((args.provider_memory != 2,
                               args.dirty_slots is not None and args.dirty_slots != 1,
                               args.native_compaction_txids is not None and args.native_compaction_txids != 256)) <= 1,
                          'change only one variable per causal probe')
            if args.dirty_slots is not None:
                bench.require(args.implementation == 'cellule' and
                              'diagnostic' in json.loads((args.artifacts / 'source.json').read_text()),
                              'dirty override requires isolated diagnostic build')
            bench.require(args.native_compaction_txids is None or args.implementation == 'celld-native',
                          'native compaction setting requires native celld')
            args.cells, args.concurrency, args.repetitions = 2000, 64, 1
            args.startup_timeout = 1800
            run = Probe(args)
            try:
                run.execute()
            except BaseException as error:
                run.summary['failure'] = str(error) or type(error).__name__
                raise
            finally:
                run.cleanup()


if __name__ == '__main__':
    main()
