"""Export, adapt and build the paired 96-byte SQL workload outside the checkout."""
import argparse
import hashlib
import json
import shutil
import io
import tarfile
import subprocess
import fcntl
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
RUST = 'rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e'
CELLD = 'ghcr.io/denoland/celld@sha256:acb0bc2dca628f0dac7c6b43eea42615a79f4943f316f6df7c1da0b86d52150b'
RUSTFS = 'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858'

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def replace(path, before, after, count=1):
    text = path.read_text()
    if text.count(before) != count:
        raise RuntimeError(f'workload adaptation no longer matches {path.name}: {before!r}')
    path.write_text(text.replace(before, after))

def adapt(source):
    sql = source / 'crates/cellule-axum/examples/sql.rs'
    replace(sql, 'total_cents INTEGER NOT NULL)', 'total_cents INTEGER NOT NULL, value TEXT NOT NULL)')
    replace(sql, '    total_cents: i64,', '    total_cents: i64,\n    value: String,', 2)
    replace(sql, 'INSERT INTO orders (id, total_cents) VALUES (?1, ?2)', 'INSERT INTO orders (id, total_cents, value) VALUES (?1, ?2, ?3)')
    replace(sql, 'SqlValue::Integer(input.total_cents),', 'SqlValue::Integer(input.total_cents),\n                            SqlValue::Text(input.value),')
    replace(sql, 'SELECT total_cents FROM orders', 'SELECT total_cents, value FROM orders', 2)
    replace(sql, 'let [SqlValue::Integer(total_cents)]', 'let [SqlValue::Integer(total_cents), SqlValue::Text(value)]')
    replace(sql, 'total_cents: *total_cents,', 'total_cents: *total_cents,\n                value: value.clone(),')
    replace(sql, 'DiskBudget::new(1 << 30)', 'DiskBudget::new(std::env::var("PARITY_DISK_BYTES").unwrap_or_else(|_| "1073741824".into()).parse()?)')
    replace(sql, '16 * 1024 * 1024', 'std::env::var("PARITY_RETAINED_BYTES").unwrap_or_else(|_| "67108864".into()).parse()?', 2)
    fleet = source / 'crates/cellule-axum/examples/fleet/mod.rs'
    replace(fleet, '            let peers = enrollment.authority.recruit().await?;', '            // Fixture policy: keep real node authority for bucket-only runs.\n            if std::env::var_os("CELLULE_AXUM_BUCKET_ONLY").is_some() { return Ok(()); }\n            let peers = enrollment.authority.recruit().await?;')
    replace(fleet, 'println!("Follower durability: enrolled two original boots over pinned mTLS");',
            'if std::env::var_os("CELLULE_AXUM_BUCKET_ONLY").is_some() { println!("Follower durability: bucket-only fixture; node lease installed"); } else { println!("Follower durability: enrolled two original boots over pinned mTLS"); }')
    request = source / 'crates/cellule-axum/examples/capacity/request.rs'
    replace(request, '    pub total_cents: i64,', '    pub total_cents: i64,\n    pub value: String,')
    replace(request, '    total_cents: i64,', '    total_cents: i64,\n    value: String,')
    replace(request, '            id,\n            total_cents:', '            id,\n            value: "p".repeat(96),\n            total_cents:')
    replace(request, '            total_cents: body.total_cents,', '            total_cents: body.total_cents,\n            value: body.value.clone(),')

def build(args):
    destination = args.artifacts.resolve()
    if destination == ROOT or ROOT in destination.parents:
        raise RuntimeError('performance artifacts must be outside the repository')
    destination.mkdir(parents=True, exist_ok=True)
    cache = (args.target_cache or destination / 'target').resolve()
    if cache == ROOT or ROOT in cache.parents:
        raise RuntimeError('build cache must be outside the repository')
    cache.mkdir(parents=True, exist_ok=True)
    source = destination / 'source'
    if source.exists():
        raise RuntimeError('use a fresh artifact directory; source manifests are immutable')
    revision = args.framework_ref or 'HEAD'
    resolved = subprocess.check_output(['git', 'rev-parse', revision], cwd=ROOT, text=True).strip()
    if args.framework_ref:
        source.mkdir()
        archive = subprocess.check_output(['git', 'archive', resolved], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
            stream.extractall(source, filter='data')
    else:
        files = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT).decode().split('\x00')
        for name in files:
            if name and (ROOT / name).is_file():
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / name, path)
    overlay = {}
    if args.measurement_overlay:
        if not args.framework_ref:
            raise RuntimeError('measurement overlay requires a pinned baseline ref')
        audited = '397f500a39d0cd3a84724a82fe66d6a56e76f726'
        if resolved not in (audited, '18eff0f7af47fac09b993157bb444e582072d7cf'):
            raise RuntimeError('the measurement overlay was audited only against the PR 65 foundation')
        if subprocess.check_output(['git', 'diff', '--name-only', audited, resolved, '--',
                                    'crates', 'Cargo.toml', 'Cargo.lock'], cwd=ROOT).strip():
            raise RuntimeError('the merged baseline differs from the audited production source')
        # These files contain measurement hooks and the identical driver only.
        # The actor files differ from the baseline only in publication timing fields.
        # Never overlay codecs, retention, or command execution behavior.
        names = [
            'crates/cellule-store/src/observation/mod.rs',
            'crates/cellule-store/src/observation/tests.rs',
            'crates/cellule-runtime/src/fleet/telemetry.rs',
            'crates/cellule-runtime/src/cell/actor/requests.rs',
            'crates/cellule-runtime/src/cell/actor/mod.rs',
            'crates/cellule-runtime/src/cell/actor/runtime.rs',
            'crates/cellule-runtime/src/cell/actor/state.rs',
            'crates/cellule-runtime/src/cell/actor/task.rs',
            'crates/cellule-runtime/src/cell/actor/tasks/activation.rs',
            'crates/cellule-runtime/src/cell/actor/tasks/publication.rs',
            'crates/cellule-runtime/src/cell/actor/inventory/tests.rs',
            'crates/cellule-runtime/src/lib.rs',
            'crates/cellule-runtime/api-prelude.txt',
            'crates/cellule-runtime/src/node/log/mod.rs',
            'crates/cellule-runtime/src/node/log/tests.rs',
            'crates/cellule-runtime/src/node/durability/mod.rs',
            'crates/cellule-runtime/src/follower/mod.rs',
            'crates/cellule-runtime/src/follower/records/append.rs',
            'crates/cellule-runtime/src/follower/tests/append.rs',
            'crates/cellule-axum/examples/capacity/mod.rs',
            'crates/cellule-axum/examples/sql.rs',
            'crates/cellule-axum/examples/sql_metrics/mod.rs',
            'crates/cellule-axum/examples/sql_metrics/capture.rs',
            'crates/cellule-axum/examples/sql_metrics/storage.rs',
            'crates/cellule-axum/examples/sql_metrics/tests.rs',
            'crates/cellule-axum/examples/fleet/mod.rs',
            'crates/cellule-axum/examples/fleet/authority.rs',
            'crates/cellule-axum/examples/fleet/server.rs',
            'crates/cellule-axum/examples/fleet/transport.rs',
        ]
        for name in names:
            shutil.copy2(ROOT / name, source / name)
            overlay[name] = sha(source / name)
    manifest = {str(path.relative_to(source)): sha(path) for path in source.rglob('*') if path.is_file()}
    (destination / 'framework-source.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    adapt(source)
    examples = source / 'crates/cellule-axum/examples'
    shutil.copy2(ROOT / 'scripts/perf/http_audit.rs', examples / 'http_audit.rs')
    # Cargo uses mtimes for local package freshness. Re-exporting a different
    # revision to /work/source can otherwise reuse newer artifacts from the
    # previous revision. Namespace the cache by every adapted source byte and
    # the pinned compiler image; never rely on export timestamps for identity.
    build_source = {str(path.relative_to(source)): sha(path)
                    for path in sorted(source.rglob('*')) if path.is_file()}
    cache_key = hashlib.sha256(json.dumps({'rust': RUST, 'source': build_source},
                                          sort_keys=True).encode()).hexdigest()
    target = cache / cache_key
    target.mkdir(exist_ok=True)
    for name in ('control.py', 'wait-store-ready.py'):
        shutil.copy2(ROOT / 'scripts/perf' / name, destination / name)
    shutil.copytree(ROOT / 'scripts/perf/celld', destination / 'celld-app')
    subprocess.run(['python3', str(ROOT / 'scripts/generate-capacity-tls.py'), str(destination / 'tls')], check=True)
    docker = ['docker', '--context', args.context]
    with (target / '.build-lock').open('a') as lock, (destination / 'build.log').open('x') as log:
        fcntl.flock(lock, fcntl.LOCK_EX)
        subprocess.run(docker + ['run', '--rm', '--cpus', '8', '--memory', '12g', '-v', f'{destination}:/work', '-v', f'{target}:/work/target', '-w', '/work/source', '-e', 'CARGO_TARGET_DIR=/work/target', RUST, 'cargo', 'build', '--release', '--locked', '-p', 'cellule-axum', '--example', 'sql', '--example', 'http_capacity', '--example', 'http_audit'], stdout=log, stderr=subprocess.STDOUT, check=True)
    (destination / 'bin').mkdir()
    for name in ('sql', 'http_capacity', 'http_audit'):
        shutil.copy2(target / 'release/examples' / name, destination / 'bin' / name)
    binaries = {name: {'path': f'bin/{name}', 'sha256': sha(destination / f'bin/{name}')} for name in ('sql', 'http_capacity', 'http_audit')}
    data = {'schema_version': 2, 'build_source_sha256': cache_key, 'framework_revision': resolved, 'measurement_overlay': overlay, 'framework_source_manifest_sha256': sha(destination / 'framework-source.json'), 'adapted_source': {str(p.relative_to(source)): sha(p) for p in sorted(examples.rglob('*.rs'))}, 'images': {'rust': RUST, 'celld': CELLD, 'store': RUSTFS}, 'celld_revision': 'f2bf648663a610eefde71f3547ad61e9b896b1f0', 'workload': 'sql-ledger-96', 'binaries': binaries}
    (destination / 'build.json').write_text(json.dumps(data, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'manifest': str(destination / 'build.json'), 'binaries': binaries}))
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='dedicated Linux Docker context')
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--framework-ref', help='export a pinned Git revision instead of the working tree')
    parser.add_argument('--measurement-overlay', action='store_true', help='apply the current measurement-only files to the pinned baseline')
    parser.add_argument('--target-cache', type=Path, help='optional external Linux build cache; binaries are copied into each run')
    build(parser.parse_args())
