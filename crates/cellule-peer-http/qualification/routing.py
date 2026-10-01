#!/usr/bin/env python3
"""Matched RustFS routing runs; keep source, binary and raw-sample provenance."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import tarfile

QUERIES = 4096
COMMANDS = 1024
PACED_BURSTS = 48
ORDER = ("baseline", "candidate", "candidate", "baseline", "baseline", "candidate")
SELECTORS = {
    "leased": "performance_tests::rustfs_owner_routing_latency_throughput",
    "object_only": "performance_tests::rustfs_object_only_routing_latency_throughput",
}
PEER = Path("crates/cellule-peer-http")
EXPECTED_ROWS = {(f"{route}_{action}", concurrency)
                 for route in ("local", "forwarded") for action in ("query", "command")
                 for concurrency in (1, 16)} | {
    ("local_query_cold", 1), ("forwarded_query_cold", 1),
    ("local_query_fresh_client", 1), ("local_query_fresh_client", 16),
    ("local_query_expired_bursts", 16), ("forwarded_query_expired_bursts", 16),
    ("forwarded_query_uncached_route", 16),
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def output(*command, cwd=None):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def build(revision, name, state, evidence, harness):
    source = state / f"{name}-source"
    source.mkdir()
    archive = subprocess.check_output(["git", "archive", revision])
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        files.extractall(source, filter="data")
    # Only test wiring is transplanted. Older revisions predate this harness.
    (source / PEER / "src/performance_tests.rs").write_bytes(harness)
    entry = source / PEER / "src/lib.rs"
    if "mod performance_tests;" not in entry.read_text():
        entry.write_text(entry.read_text() + "\n#[cfg(test)]\nmod performance_tests;\n")
    fixture = source / PEER / "src/tests.rs"
    fixture.write_text(fixture.read_text().replace(
        "\nfn generate_peer_identity(", "\npub(super) fn generate_peer_identity("
    ))
    manifest = source / PEER / "Cargo.toml"
    text = manifest.read_text()
    for dependency in ("blake3", "tempfile"):
        if f"{dependency}.workspace = true" not in text:
            text = text.replace("[dev-dependencies]\n", f"[dev-dependencies]\n{dependency}.workspace = true\n")
    manifest.write_text(text)
    # Keep every resolved dependency version at the baseline revision. Older
    # locks need only these two existing packages added to this test crate.
    lock = source / "Cargo.lock"
    text = lock.read_text()
    pattern = r'(\[\[package\]\]\nname = "cellule-peer-http"\n.*?dependencies = \[\n)(.*?)(\n\])'
    def test_dependencies(match):
        dependencies = set(match[2].splitlines()) | {'  "blake3",', '  "tempfile",'}
        return match[1] + "\n".join(sorted(dependencies)) + match[3]
    text, replacements = re.subn(pattern, test_dependencies, text, count=1, flags=re.S)
    if replacements != 1:
        raise RuntimeError("Missing peer test crate in the baseline lock")
    lock.write_text(text)
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(state / f"{name}-target")
    env["CARGO_BUILD_JOBS"] = "4"
    with (evidence / f"{name}-build.jsonl").open("w") as log:
        subprocess.run([
            "cargo", "test", "-p", "cellule-peer-http", "--lib", "--release",
            "--locked", "--no-run", "--message-format=json",
        ], cwd=source, env=env, stdout=log, check=True)
    artifacts = [json.loads(line) for line in (evidence / f"{name}-build.jsonl").read_text().splitlines()]
    binaries = {row["executable"] for row in artifacts
                if row.get("reason") == "compiler-artifact" and row.get("executable")
                and row.get("profile", {}).get("test") and row["target"]["name"] == "cellule_peer_http"}
    if len(binaries) != 1:
        raise RuntimeError(f"Expected one {name} test binary, got {binaries}")
    binary = evidence / name
    shutil.copy2(binaries.pop(), binary)
    for selector in SELECTORS.values():
        listed = output(str(binary), selector, "--exact", "--ignored", "--list")
        if listed.count(f"{selector}: test") != 1 or "1 test, 0 benchmarks" not in listed:
            raise RuntimeError(f"Missing exact benchmark: {listed}")
    return {"revision": revision, "binary": str(binary), "binary_sha256": digest(binary),
            "source_archive_sha256": hashlib.sha256(archive).hexdigest(),
            "harness_sha256": hashlib.sha256(harness).hexdigest()}


def measure(version, mode, index, binary, evidence):
    directory = evidence / f"{mode}-{index}-{version}"
    directory.mkdir()
    env = os.environ.copy()
    env.update(CELLULE_TEST_PREFIX=directory.name, CELLULE_PERF_EVIDENCE=str(directory),
               CELLULE_PERF_QUERIES=str(QUERIES), CELLULE_PERF_COMMANDS=str(COMMANDS),
               CELLULE_PERF_BURSTS=str(PACED_BURSTS))
    print(f"START {directory.name}", flush=True)
    with (directory / "run.log").open("w") as log:
        result = subprocess.run([binary, SELECTORS[mode], "--exact", "--ignored", "--nocapture"],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
    text = (directory / "run.log").read_text()
    print(text, flush=True)
    if result.returncode or f"correctness=passed commands={COMMANDS * 6} final_sequence={COMMANDS * 6}" not in text:
        raise RuntimeError(f"Benchmark or exact recovery failed: {directory.name}")
    rows = []
    for line in text.splitlines():
        if not line.startswith("RUSTFS lane="):
            continue
        fields = dict(re.findall(r"(\w+)=(\S+)", line))
        lane = fields.pop("lane")
        row = {key: float(value) for key, value in fields.items()}
        if not all(math.isfinite(value) and value >= 0 for value in row.values()):
            raise RuntimeError(f"Invalid numeric measurement: {lane}")
        if row["elapsed_s"] <= 0:
            raise RuntimeError(f"Invalid elapsed time: {lane}")
        # The log rounds seconds to six places and throughput to three.
        minimum_rate = row["calls"] / (row["elapsed_s"] + 0.0000005) - 0.0005
        maximum_rate = row["calls"] / max(row["elapsed_s"] - 0.0000005, 1e-12) + 0.0005
        if not minimum_rate <= row["throughput"] <= maximum_rate:
            raise RuntimeError(f"Throughput mismatch: {lane}")
        expected_calls = (1 if lane.endswith("_cold") else PACED_BURSTS * 16 if lane.endswith("_expired_bursts")
                          else 64 if lane.endswith("_uncached_route") else COMMANDS if lane.endswith("_command")
                          else QUERIES)
        if row["calls"] != expected_calls:
            raise RuntimeError(f"Workload changed: {lane}")
        if lane.endswith("_command"):
            path = directory / f"{lane}-c{int(row['concurrency'])}.publication.tsv"
            with path.open(newline="") as source:
                publications = [{key: int(value) for key, value in item.items()}
                                for item in csv.DictReader(source, delimiter="\t")]
            sequences = [item["sequence"] for item in publications]
            if (len(publications) != COMMANDS or not sequences
                    or sequences != list(range(sequences[0], sequences[0] + COMMANDS))
                    or any(value < 0 for item in publications for value in item.values())):
                raise RuntimeError(f"Incomplete publication evidence: {lane}")
        samples = [int(value) for value in (directory / f"{lane}-c{int(row['concurrency'])}.ns").read_text().splitlines()]
        if len(samples) != row["calls"] or samples != sorted(samples):
            raise RuntimeError(f"Raw sample mismatch: {lane}")
        for quantile in (50, 95, 99):
            actual = samples[(len(samples) * quantile + 99) // 100 - 1] / 1_000_000
            if abs(actual - row[f"p{quantile}_ms"]) > 0.000001:
                raise RuntimeError(f"Quantile mismatch: {lane}")
        if version == "candidate" and lane in ("local_query", "forwarded_query"):
            expected_reads = 0 if mode == "leased" else row["calls"]
            expected_hops = row["calls"] if lane == "forwarded_query" else 0
            # The fixture renews its 15-second signed lease every five seconds.
            # A fresh sender enrollment has at least nine seconds of reuse;
            # expiry can require control + enrollment reads during long lanes.
            # The ordinary golden test checks exactly one receiver read.
            refresh_budget = 2 * math.ceil(row["elapsed_s"] / 9) + 1 if lane == "forwarded_query" else 0
            if not expected_reads <= row["reads"] <= expected_reads + refresh_budget or row["hops"] != expected_hops:
                raise RuntimeError(f"Routing work changed: {mode} {lane} {row}")
        if version == "candidate" and lane.endswith("_expired_bursts"):
            expected_reads = 0 if mode == "leased" else row["calls"]
            expected_hops = row["calls"] if lane.startswith("forwarded") else 0
            if row["reads"] != expected_reads or row["hops"] != expected_hops:
                raise RuntimeError(f"Paced routing work changed: {mode} {lane} {row}")
        if version == "candidate" and lane == "forwarded_query_uncached_route":
            expected_reads = 8 + (0 if mode == "leased" else row["calls"])
            if row["reads"] != expected_reads or row["hops"] != row["calls"]:
                raise RuntimeError(f"Cold routing work changed: {mode} {row}")
        rows.append(dict(row, lane=lane, mode=mode, version=version, run=index))
    if len(rows) != len(EXPECTED_ROWS) or {(row["lane"], row["concurrency"]) for row in rows} != EXPECTED_ROWS:
        raise RuntimeError(f"Incomplete benchmark: {len(rows)} lanes")
    write_json(directory / "rows.json", rows)
    return rows


def compare(rows):
    comparisons = []
    failures = []
    if {row["mode"] for row in rows} != set(SELECTORS):
        raise RuntimeError("Incomplete routing modes")
    safe_unleased_baseline = any(row["reads"] > 0 for row in rows
                                if row["mode"] == "object_only" and row["version"] == "baseline"
                                and row["lane"] == "local_query")
    for mode in SELECTORS:
        keys = sorted({(row["lane"], row["concurrency"]) for row in rows if row["mode"] == mode})
        for lane, concurrency in keys:
            selected = {version: [row for row in rows if row["mode"] == mode
                                  and row["lane"] == lane and row["concurrency"] == concurrency
                                  and row["version"] == version] for version in ORDER[:2]}
            if any(len(values) != 3 for values in selected.values()):
                raise RuntimeError(f"Incomplete repeats: {mode} {lane}")
            medians = {version: {metric: statistics.median(row[metric] for row in values)
                                for metric in ("throughput", "p50_ms", "p95_ms", "p99_ms", "reads")}
                       for version, values in selected.items()}
            ratios = {metric: medians["candidate"][metric] / medians["baseline"][metric]
                      for metric in ("throughput", "p50_ms", "p95_ms", "p99_ms")}
            # A historical unleased zero-read cache is not a safe latency target.
            # Its measurements remain in evidence; only leased historical routing
            # and unleased comparisons with fresh authority qualify latency.
            safe_baseline = mode == "leased" or safe_unleased_baseline
            gated = safe_baseline and lane in (
                "local_query_fresh_client", "local_query", "forwarded_query",
                "local_command", "forwarded_command", "local_query_expired_bursts",
                "forwarded_query_expired_bursts", "forwarded_query_uncached_route",
            )
            if gated and (ratios["p95_ms"] > 1.10 or ratios["p99_ms"] > 1.10
                          or ("expired_bursts" not in lane and ratios["throughput"] < 0.90)):
                failures.append(f"{mode}/{lane}/c{int(concurrency)}")
            comparisons.append(dict(mode=mode, lane=lane, concurrency=concurrency,
                                    medians=medians, ratios=ratios, latency_gate=gated))
    return {"comparisons": comparisons, "failures": failures,
            "threshold": "median of all three runs: p95/p99 <= 110%, throughput >= 90%"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    state = args.state.resolve()
    state.mkdir()
    evidence = state / "evidence"
    evidence.mkdir()
    harness = (PEER / "src/performance_tests.rs").read_bytes()
    revisions = {"baseline": output("git", "rev-parse", "--verify", f"{args.baseline}^{{commit}}"),
                 "candidate": output("git", "rev-parse", "HEAD")}
    manifest = {"order": ORDER, "queries": QUERIES, "commands_per_lane": COMMANDS,
                "paced_bursts": PACED_BURSTS,
                "selectors": SELECTORS, "host": platform.uname()._asdict(),
                "test_only_transplant": [str(PEER / path) for path in (
                    "src/performance_tests.rs", "src/lib.rs", "src/tests.rs", "Cargo.toml")] + ["Cargo.lock (peer test dependencies only)"],
                "fixture_endpoint": os.environ["CELLULE_TEST_ENDPOINT"]}
    for name, revision in revisions.items():
        manifest[name] = build(revision, name, state, evidence, harness)
        write_json(evidence / "manifest.json", manifest)
    rows = []
    for mode in SELECTORS:
        for index, version in enumerate(ORDER, 1):
            binary = Path(manifest[version]["binary"])
            if digest(binary) != manifest[version]["binary_sha256"]:
                raise RuntimeError("Frozen binary changed")
            rows.extend(measure(version, mode, index, str(binary), evidence))
    summary = compare(rows)
    write_json(evidence / "comparison.json", summary)
    if summary["failures"]:
        raise RuntimeError(f"Performance gate failed: {summary['failures']}")


if __name__ == "__main__":
    main()
