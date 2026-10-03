#!/usr/bin/env python3
"""Check cookbook membership, dependency direction, source ownership, and doc links."""

from pathlib import Path
import re
import tomllib
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
FRAMEWORK = ROOT.parent / "crates"
DECLARATION = re.compile(r"^\s*(?:pub(?:\([^)]*\))? )?mod ([a-z_][a-z_0-9]*)\s*;", re.M)
LINK = re.compile(r"\]\(([^)]+)\)")


def main() -> int:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    members = [ROOT / member for member in manifest["workspace"]["members"]]
    failures = []
    names = {}
    for member in members:
        package = tomllib.loads((member / "Cargo.toml").read_text())
        name = package["package"]["name"]
        names[name] = member
        if not name.startswith("cellule-cookbook-"):
            failures.append(f"{member}: package must use the cookbook prefix")
        if not (member / "README.md").is_file() or not (member / "src/lib.rs").is_file():
            failures.append(f"{member}: missing library or runnable guide")
        if member.parent.name == "apps" and not (member / "src/main.rs").is_file():
            failures.append(f"{member}: application has no executable")
        if member.parent.name == "apps":
            if name != f"cellule-cookbook-{member.name}":
                failures.append(f"{member}: application package name must match its launcher slug")
            for required in [ROOT / "scripts" / f"{member.name}.sh", ROOT / "scenarios" / f"{member.name}.py"]:
                if not required.is_file():
                    failures.append(f"{member}: missing runnable launcher or persistent process scenario: {required}")
        for source in (member / "src").rglob("*.rs"):
            if source.name in {"lib.rs", "main.rs"} or "bin" in source.relative_to(member / "src").parts:
                continue
            owner_dir = source.parent.parent if source.name == "mod.rs" else source.parent
            if owner_dir == member / "src":
                candidates = [owner_dir / "lib.rs", owner_dir / "main.rs"]
            else:
                candidates = [owner_dir / "mod.rs"]
            child = source.parent.name if source.name == "mod.rs" else source.stem
            owners = [owner for owner in candidates if owner.is_file() and child in DECLARATION.findall(owner.read_text())]
            if len(owners) != 1:
                failures.append(f"{source}: production module must have exactly one declaring library, binary, or parent module")
            if source.name != "mod.rs" and source.with_suffix("").is_dir():
                failures.append(f"{source}: module entry belongs inside its directory")
    for member in members:
        package = tomllib.loads((member / "Cargo.toml").read_text())
        for name in package.get("dependencies", {}):
            if name.startswith("cellule-cookbook-") and name != "cellule-cookbook-support":
                failures.append(f"{member}: application dependency on {name}")
            if member.name == "support" and name in names:
                failures.append(f"{member}: support depends on a cookbook app")
    for crate in FRAMEWORK.glob("*/Cargo.toml"):
        package = tomllib.loads(crate.read_text())
        for section in ["dependencies", "dev-dependencies", "build-dependencies"]:
            if any(name in names for name in package.get(section, {})):
                failures.append(f"{crate}: framework depends on cookbook")
    for document in ROOT.rglob("*.md"):
        if any(part in {"target", ".state"} for part in document.relative_to(ROOT).parts):
            continue
        for raw in LINK.findall(document.read_text()):
            link = raw.split(" ", 1)[0].strip("<>")
            if link.startswith(("https:", "http:", "mailto:", "/")):
                continue
            destination, _, anchor = link.partition("#")
            resolved = document.parent / unquote(destination) if destination else document
            if not resolved.exists():
                failures.append(f"{document}: missing link {link}")
            elif anchor and resolved.suffix == ".md":
                headings = re.findall(r"^#+\s+(.+)$", resolved.read_text(), re.M)
                anchors = {re.sub(r"[^\w\-\s]", "", text.lower()).replace(" ", "-") for text in headings}
                if unquote(anchor) not in anchors:
                    failures.append(f"{document}: missing anchor {link}")
    for failure in failures:
        print(f"error: {failure}")
    if failures:
        return 1
    print(f"ok: {len(members)} cookbook packages; dependency direction, module ownership, and links")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
