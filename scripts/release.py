#!/usr/bin/env python3
"""Validate and publish Cellule's lockstep crates.io releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRATES = (
    "cellule-types",
    "cellule-store",
    "cellule-ltx",
    "cellule-runtime",
    "cellule-app",
    "cellule-host",
    "cellule-peer-http",
    "cellule-axum",
)
REGISTRY_API = "https://crates.io/api/v1/crates"
USER_AGENT = "cellule-release/1.0 (https://github.com/crabbuild/cellule)"
CHANGELOG = ROOT / "CHANGELOG.md"


class ReleaseError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise ReleaseError(message)


def changelog_notes(version: str) -> str:
    try:
        text = CHANGELOG.read_text()
    except OSError as error:
        fail(f"could not read {CHANGELOG}: {error}")
    headings = list(re.finditer(r"^## \[([^\]]+)\](?: - \d{4}-\d{2}-\d{2})?\s*$", text, re.MULTILINE))
    for index, heading in enumerate(headings):
        if heading.group(1) != version:
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        notes = text[heading.end() : end].strip()
        if not notes:
            fail(f"CHANGELOG.md has an empty section for {version}")
        return notes
    fail(f"CHANGELOG.md needs a non-empty section for {version}")


def cargo_metadata() -> dict[str, object]:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        fail(f"cargo metadata failed:\n{result.stderr.strip()}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"cargo metadata returned invalid JSON: {error}")


def dependency_requirement(version: str) -> str:
    if "-" in version:
        return f"={version}"
    major_text, minor_text, _ = version.split(".", 2)
    major = int(major_text)
    minor = int(minor_text)
    if major == 0:
        return f"0.{minor}.0"
    return f"{major}.0.0"


def validate(tag: str | None = None) -> str:
    try:
        root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"could not read workspace Cargo.toml: {error}")

    workspace = root_manifest.get("workspace", {})
    version = workspace.get("package", {}).get("version")
    if not isinstance(version, str) or not version:
        fail("[workspace.package].version must contain the release version")
    if "+" in version:
        fail("crates.io release versions must omit SemVer build metadata")
    changelog_notes(version)

    metadata = cargo_metadata()
    workspace_member_ids = set(metadata.get("workspace_members", []))
    packages = {
        package["name"]: package
        for package in metadata.get("packages", [])
        if package.get("id") in workspace_member_ids
    }
    if set(packages) != set(CRATES):
        missing = sorted(set(CRATES) - set(packages))
        unexpected = sorted(set(packages) - set(CRATES))
        fail(f"workspace release set changed (missing={missing}, unexpected={unexpected})")

    for name in CRATES:
        manifest_path = Path(packages[name]["manifest_path"])
        try:
            package_manifest = tomllib.loads(manifest_path.read_text())
        except (OSError, tomllib.TOMLDecodeError) as error:
            fail(f"could not read {manifest_path}: {error}")
        if package_manifest.get("package", {}).get("version") != {"workspace": True}:
            fail(f"{name} must inherit version.workspace = true")
        if packages[name].get("version") != version:
            fail(f"{name} resolved to {packages[name].get('version')}, expected {version}")

    workspace_dependencies = workspace.get("dependencies", {})
    expected_requirement = dependency_requirement(version)
    for name in CRATES:
        dependency = workspace_dependencies.get(name)
        requirement = dependency.get("version") if isinstance(dependency, dict) else None
        if requirement != expected_requirement:
            fail(
                f"workspace dependency {name} must use the matched compatibility range; "
                f"expected version = \"{expected_requirement}\""
            )

    if tag is not None:
        if tag != f"v{version}":
            fail(f"tag {tag!r} does not match workspace version v{version}")

    return version


def target_package_dir() -> Path:
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    target = Path(target_dir) if target_dir else ROOT / "target"
    if not target.is_absolute():
        target = ROOT / target
    return target.resolve() / "package"


def registry_checksum(crate: str, version: str) -> str | None:
    url = f"{REGISTRY_API}/{crate}/{version}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise
    release = body.get("version")
    if not isinstance(release, dict) or not isinstance(release.get("checksum"), str):
        fail(f"crates.io returned no checksum for {crate} {version}")
    return release["checksum"]


def sparse_index_contains(crate: str, version: str) -> bool:
    if len(crate) == 1:
        path = f"1/{crate}"
    elif len(crate) == 2:
        path = f"2/{crate}"
    elif len(crate) == 3:
        path = f"3/{crate[0]}/{crate}"
    else:
        path = f"{crate[:2]}/{crate[2:4]}/{crate}"
    request = urllib.request.Request(
        f"https://index.crates.io/{path}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            records = response.read().decode("utf-8").splitlines()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    for record in records:
        try:
            if json.loads(record).get("vers") == version:
                return True
        except json.JSONDecodeError as error:
            fail(f"crates.io sparse index returned invalid JSON for {crate}: {error}")
    return False


def wait_for_registry(crate: str, version: str, expected_checksum: str) -> None:
    deadline = time.monotonic() + 120
    while True:
        try:
            checksum = registry_checksum(crate, version)
            indexed = sparse_index_contains(crate, version)
        except (urllib.error.HTTPError, urllib.error.URLError) as error:
            print(f"Waiting for crates.io to respond for {crate} {version}: {error}", flush=True)
            checksum = None
            indexed = False
        if checksum is not None and checksum != expected_checksum:
            fail(f"crates.io checksum mismatch after publishing {crate} {version}")
        if checksum is not None and indexed:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(3, remaining))
    fail(f"{crate} {version} did not become visible on crates.io within 120 seconds")


def publish(tag: str) -> None:
    version = validate(tag)
    package_dir = target_package_dir()

    for crate in CRATES:
        subprocess.run(
            ["cargo", "package", "--package", crate, "--locked", "--no-verify"],
            cwd=ROOT,
            check=True,
        )
        archive = package_dir / f"{crate}-{version}.crate"
        if not archive.is_file():
            fail(f"cargo package did not create {archive}")
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()

        existing_checksum = registry_checksum(crate, version)
        if existing_checksum is not None:
            if existing_checksum != checksum:
                fail(
                    f"{crate} {version} already exists on crates.io with a different checksum; "
                    "stop and investigate the partial release"
                )
            wait_for_registry(crate, version, checksum)
            print(f"{crate} {version} is already published with the expected checksum", flush=True)
            continue

        subprocess.run(
            ["cargo", "publish", "--package", crate, "--locked"],
            cwd=ROOT,
            check=True,
        )
        wait_for_registry(crate, version, checksum)
        print(f"Published {crate} {version}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="validate workspace release metadata")
    check.add_argument("--tag", help="require the tag to match the workspace version")
    notes = commands.add_parser("notes", help="print the current version's changelog section")
    notes.add_argument("--tag", required=True, help="the v-prefixed release tag")
    publishing = commands.add_parser("publish", help="publish the tagged workspace to crates.io")
    publishing.add_argument("--tag", required=True, help="the v-prefixed release tag")
    args = parser.parse_args()

    try:
        if args.command == "check":
            version = validate(args.tag)
            print(f"Validated Cellule workspace release {version}")
        elif args.command == "notes":
            version = validate(args.tag)
            print(changelog_notes(version))
        else:
            publish(args.tag)
    except (ReleaseError, OSError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        print(f"release error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
