#!/usr/bin/env python3
"""Freeze and build an append-only native HTTP fixture over pinned celld Rust."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import io
import importlib.util

ROOT = Path(__file__).resolve().parents[3]
PIN = '90b43017241f81189453d326d05948f388b34652'
URL = 'https://github.com/denoland/celld'
spec = importlib.util.spec_from_file_location('three_node', ROOT / 'scripts/perf/three-node.py')
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def build(args):
    output = args.artifacts.resolve()
    bench.require(ROOT not in output.parents and output != ROOT, 'use external build artifacts')
    output.mkdir(parents=True, exist_ok=False)
    upstream = args.upstream.resolve(strict=True)
    bench.require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=upstream).decode().strip() == PIN,
                  'wrong celld revision')
    bench.require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=upstream), 'upstream must be pristine')
    original = output / 'upstream'
    original.mkdir()
    archive = subprocess.check_output(['git', 'archive', PIN], cwd=upstream)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(original, filter='data')
    hashes = {str(path.relative_to(original)):bench.sha(path) for path in original.rglob('*') if path.is_file()}
    fixture = original / 'crates/celld/examples/native_http'
    fixture.mkdir(parents=True)
    for name in ('main.rs', 'sql.rs', 'ownership.rs'):
        shutil.copy2(Path(__file__).with_name(name), fixture / name)
    shutil.copy2(Path(__file__).with_name('hooks.rs'), original / 'crates/celld/native_http_hooks.rs')
    additions = {
        'crates/celld/lib.rs': '\npub fn native_http_open(scope: &str, path: &str, epoch: u64, vfs: Option<&str>) -> anyhow::Result<()> { storage::open_at_epoch(scope, path, epoch, false, vfs, false) }\n',
        'crates/celld/ltx_repl.rs': '\ninclude!("native_http_hooks.rs");\n',
        'crates/celld/Cargo.toml': '\n[[example]]\nname = "native_http"\npath = "examples/native_http/main.rs"\n',
    }
    for name, addition in additions.items():
        path = original / name
        before = path.read_bytes()
        path.write_bytes(before + addition.encode())
        bench.require(path.read_bytes()[:len(before)] == before, 'upstream body changed')
    source = output / 'source'
    files = subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=ROOT).decode().split('\0')
    for name in files:
        if name and (ROOT/name).is_file():
            target=source/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(ROOT/name,target)
    proof = dict(revision=PIN, upstream_url=URL, original_files=hashes, append_only=additions,
                 adapted_files={str(path.relative_to(original)):bench.sha(path) for path in original.rglob('*') if path.is_file()},
                 profile='unmodified upstream release: opt-level s, fat LTO, codegen-units 1, panic abort',
                 native_dispatch='production storage, LtxRepl, BucketOwnership; no V8 isolate/Node; fixed placement embedding',
                 http_driver_sha256=bench.sha(args.cellule_build/'target/release/examples/http_capacity'))
    bench.put(output/'source.json',proof)
    binary_dir = output/'target/release/examples'
    binary_dir.mkdir(parents=True)
    shutil.copy2(args.cellule_build/'target/release/examples/http_capacity',binary_dir/'http_capacity')
    command=['docker','--context',args.context,'run','--rm','--name','cellule-native-celld-build',
             '--cpus','8','--memory','12g','-v',f'{output}:/work',
             '-v',f'{output}/cargo-registry:/usr/local/cargo/registry','-w','/work/upstream',
             '-e','CARGO_TARGET_DIR=/work/native-target',bench.RUST,'cargo','build','--release','--locked','-p','celld','--example','native_http']
    bench.put(output/'build-command.json',command)
    with (output/'build.log').open('x') as log:
        subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
    # Check the entire upstream tree, not just the exposed entry points.
    for name, digest in hashes.items():
        contents = (original/name).read_bytes()
        if name in additions:
            suffix = additions[name].encode()
            bench.require(contents.endswith(suffix), 'missing append-only hook: '+name)
            contents = contents[:-len(suffix)]
        bench.require(hashlib.sha256(contents).hexdigest() == digest, 'production source changed: '+name)
    shutil.copy2(output/'native-target/release/examples/native_http',binary_dir/'sql')
    bench.put(output/'binaries.json',{name:bench.sha(binary_dir/name) for name in ('sql','http_capacity')})
    print(output,flush=True)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',required=True)
    parser.add_argument('--upstream',type=Path,required=True)
    parser.add_argument('--cellule-build',type=Path,required=True)
    parser.add_argument('--artifacts',type=Path,required=True)
    args=parser.parse_args()
    lock_path=Path('/tmp')/('cellule-perf-'+hashlib.sha256(args.context.encode()).hexdigest()[:16]+'.lock')
    with lock_path.open('a') as lock:
        print('Waiting for exclusive comparison context',flush=True)
        fcntl.flock(lock,fcntl.LOCK_EX)
        bench.require(not subprocess.check_output(['docker','--context',args.context,'ps','-q']).strip(),'build requires idle context')
        build(args)
