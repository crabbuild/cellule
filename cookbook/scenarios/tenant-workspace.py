#!/usr/bin/env python3
"""Private persistent HTTP service: tenant isolation, retained retries, and restore.

Run in CI or an isolated source snapshot; one fixed application installation
owns this private storage prefix. No credential-dependent Cargo test is used.
"""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


class Server:
    def __init__(self, binary, state, credentials, errors):
        self.errors_path = errors
        self.errors = errors.open("w")
        self.process = subprocess.Popen([binary, "serve", str(state), str(credentials), "127.0.0.1:0"],
                                        stdout=subprocess.PIPE, stderr=self.errors, bufsize=0)
        self.address = None
        try:
            self.ready()
        except BaseException:
            self.close()
            raise

    def ready(self):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            buffer = b""
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                for key, _ in selector.select(timeout=1):
                    chunk = os.read(key.fileobj.fileno(), 4096)
                    if not chunk:
                        raise RuntimeError(f"server exited before readiness: {self.errors_path.read_text()}")
                    buffer += chunk
                    if len(buffer) > 8192:
                        raise AssertionError("readiness output exceeded 8 KiB")
                    if b"\n" in buffer:
                        line, _ = buffer.split(b"\n", 1)
                        event = json.loads(line)
                        assert event["event"] == "ready" and event["address"].startswith("127.0.0.1:")
                        self.address = event["address"]
                        return
            raise AssertionError("server did not become ready within 45 seconds")

    def finish(self, crash=False):
        if crash:
            self.process.kill()
        else:
            self.process.send_signal(signal.SIGTERM)
        self.process.communicate(timeout=25)
        self.errors.close()
        if not crash and self.process.returncode:
            raise RuntimeError(f"server drain failed: {self.errors_path.read_text()}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)
        self.errors.close()


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-tenant-workspace-") as temporary:
        root = Path(temporary)
        state = root / "state"
        credentials = root / "credentials.json"
        active = None

        def run(*arguments, success=True):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True, text=True, timeout=75)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(subject, resource, change):
            name = uuid.uuid4().hex
            source, retained = root / f"{name}.json", root / f"{name}.mutation.json"
            source.write_text(json.dumps(change))
            run("prepare", credentials, subject, resource, source, retained)
            frozen = retained.read_bytes()
            run("prepare", credentials, subject, resource, source, retained, success=False)
            assert retained.read_bytes() == frozen
            return json.loads(frozen)

        def send(method, path, subject=None, body=None, expected=200, headers=None, lose_body=False):
            data = None if body is None else json.dumps(body).encode()
            supplied = {"Content-Type": "application/json", **(headers or {})}
            if subject:
                supplied["Authorization"] = "Bearer " + tokens[subject]
            request = urllib.request.Request("http://" + active.address + path, data=data, headers=supplied, method=method)
            try:
                response = urllib.request.urlopen(request, timeout=20)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                assert response.status == expected, (method, path, response.status, response.read(8193))
                if lose_body:
                    return None  # the caller deliberately never decodes its command response
                content = response.read(8193)
                assert len(content) <= 8192
                return json.loads(content) if content else None

        def deny_cross_tenant():
            routes = [("GET", "projects/launch"), ("PUT", "projects/launch"),
                      ("POST", "projects/launch/resolve"), ("GET", "preferences"),
                      ("PUT", "preferences"), ("POST", "preferences/resolve"),
                      ("GET", "admin/members")]
            for tenant, other in [("acme", "globex"), ("globex", "acme")]:
                for role in ["admin", "editor", "viewer"]:
                    for method, suffix in routes:
                        denied = send(method, f"/v1/tenants/{other}/{suffix}", f"{tenant}-{role}",
                                      {"target": {"tenant": other}}, expected=403)
                        assert denied == {"error": "forbidden"}

        try:
            run("init", credentials)
            run("init", credentials, success=False)
            document = json.loads(credentials.read_text())
            tokens = {member["subject"]: member["token"] for member in document["members"]}
            assert len(tokens) == 6
            run("serve", state, credentials, "0.0.0.0:0", success=False)
            active = Server(binary, state, credentials, root / "initial.stderr")
            assert send("GET", "/healthz") == {"ready": True}
            deny_cross_tenant()
            assert not list(state.iterdir()), "cross-tenant denial must precede Cell provisioning"
            for subject in tokens:
                me = send("GET", "/v1/me", subject)
                assert me["subject"] == subject and me["tenant"] == subject.split("-")[0]
            assert send("GET", "/v1/tenants/acme/projects/launch", expected=401) == {"error": "unauthorized"}
            expired_identity = {"request_id": str(uuid.uuid4()), "issued_at_ms": int(time.time() * 1000) - 2000,
                                "expires_at_ms": int(time.time() * 1000) - 1000}
            for resource, route, change in [
                ({"kind": "project", "key": "launch"}, "projects/launch", {"expected_revision": None, "title": "Expired", "description": "Never dispatched"}),
                ({"kind": "preferences"}, "preferences", {"key": "theme", "expected": None, "value": "dark"}),
            ]:
                expired = {"version": 1, "tenant": "acme", "subject": "acme-admin",
                           "resource": resource, "identity": expired_identity, "change": change}
                assert send("POST", f"/v1/tenants/acme/{route}/resolve", "acme-admin", expired) == {"resolution": "expired", "absence_proven": False}
                send("PUT", f"/v1/tenants/acme/{route}", "acme-admin", expired, expected=400)
            assert not list(state.iterdir()), "expiry metadata must not provision a Cell or prove absence"
            projects, preferences, original_results = {}, {}, {}
            for tenant, theme in [("acme", "dark"), ("globex", "light")]:
                actor = tenant + "-editor"
                route = f"/v1/tenants/{tenant}/projects/launch"
                current = send("GET", route, actor)
                projects[tenant] = mutation(actor, "project:launch", {"expected_revision": None if current["project"] is None else current["project"]["revision"],
                                                                         "title": f"Private {tenant} launch", "description": "Tenant-owned document"})
                assert send("POST", route + "/resolve", actor, projects[tenant])["resolution"] == "absent"
                send("PUT", route, actor, projects[tenant], lose_body=True)
                resolved = send("POST", route + "/resolve", actor, projects[tenant])
                assert resolved["resolution"] == "committed" and resolved["outcome"]["project"]["title"] == f"Private {tenant} launch"
                original_results[tenant] = send("PUT", route, actor, projects[tenant])
                assert original_results[tenant]["receipt"]["commit_sequence"] == resolved["commit_sequence"]
                assert send("PUT", route, actor, projects[tenant]) == original_results[tenant]
                preference_route = f"/v1/tenants/{tenant}/preferences"
                current = send("GET", preference_route, tenant + "-admin")["preferences"]
                previous = next((value["version"] for value in current if value["key"] == "theme"), None)
                preferences[tenant] = mutation(tenant + "-admin", "preferences", {"key": "theme", "expected": previous, "value": theme})
                sent = send("PUT", preference_route, tenant + "-admin", preferences[tenant])
                assert send("PUT", preference_route, tenant + "-admin", preferences[tenant]) == sent
            assert original_results["acme"]["receipt"]["cell"] != original_results["globex"]["receipt"]["cell"]
            foreign_receipt = json.dumps(original_results["globex"]["receipt"], separators=(",", ":"))
            send("GET", "/v1/tenants/acme/projects/launch", "acme-admin", expected=400,
                 headers={"X-Cellule-Receipt": foreign_receipt})
            for hint in ["X-Tenant-Id", "X-Application-Id", "X-Namespace-Id", "X-Cellule-Target", "X-Cellule-Partition"]:
                send("GET", "/v1/tenants/acme/projects/launch", "acme-admin", expected=400, headers={hint: "globex"})
            forged = {**projects["acme"], "target": {"tenant": "globex"}}
            send("PUT", "/v1/tenants/acme/projects/launch", "acme-editor", forged, expected=400)
            send("PUT", "/v1/tenants/acme/projects/support", "acme-editor", projects["acme"], expected=400)
            send("PUT", "/v1/tenants/acme/projects/launch", "acme-viewer", projects["acme"], expected=403)
            send("PUT", "/v1/tenants/acme/preferences", "acme-editor", preferences["acme"], expected=403)
            send("POST", "/v1/tenants/acme/projects/launch/resolve", "acme-viewer", projects["acme"], expected=403)
            send("POST", "/v1/tenants/acme/preferences/resolve", "acme-editor", preferences["acme"], expected=403)
            send("GET", "/v1/tenants/acme/admin/members", "acme-editor", expected=403)
            subjects, after = [], None
            while True:
                page = send("GET", "/v1/tenants/acme/admin/members?limit=1" + ("&after=" + after if after else ""), "acme-admin")
                assert len(page["members"]) <= 1 and "token" not in json.dumps(page)
                subjects.extend(member["subject"] for member in page["members"])
                after = page["next"]
                if after is None:
                    break
            assert subjects == ["acme-admin", "acme-editor", "acme-viewer"]
            send("GET", "/v1/tenants/acme/admin/members?limit=11", "acme-admin", expected=400)
            send("GET", "/v1/tenants/acme/projects/launch?tenant=globex", "acme-admin", expected=400)
            stale = mutation("acme-editor", "project:launch", {"expected_revision": 999999, "title": "Stale", "description": "Rejected"})
            rejected = send("PUT", "/v1/tenants/acme/projects/launch", "acme-editor", stale, expected=409)
            assert send("PUT", "/v1/tenants/acme/projects/launch", "acme-editor", stale, expected=409) == rejected
            assert send("POST", "/v1/tenants/acme/projects/launch/resolve", "acme-editor", stale)["outcome"]["status"] == "conflict"
            active.finish()
            active = None
            assert not list(state.iterdir()), "graceful HTTP and node drain must release SQLite files"

            # A fresh process reconstructs both tenants from persistent objects.
            active = Server(binary, state, credentials, root / "restore.stderr")
            for tenant in ["acme", "globex"]:
                route = f"/v1/tenants/{tenant}/projects/launch"
                result = send("GET", route, tenant + "-viewer", headers={"X-Cellule-Receipt": json.dumps(original_results[tenant]["receipt"], separators=(",", ":"))})
                assert result["project"]["title"] == f"Private {tenant} launch"
                assert send("PUT", route, tenant + "-editor", projects[tenant]) == original_results[tenant]
                assert send("POST", route + "/resolve", tenant + "-editor", projects[tenant])["commit_sequence"] == original_results[tenant]["receipt"]["commit_sequence"]
                settings = send("GET", f"/v1/tenants/{tenant}/preferences", tenant + "-viewer")["preferences"]
                assert next(item["value"] for item in settings if item["key"] == "theme") == ("dark" if tenant == "acme" else "light")
            deny_cross_tenant()
            active.finish(crash=True)
            active = None
            orphan_sessions = set(state.iterdir())
            assert orphan_sessions
            # Listener readiness alone does not acquire any Cell. A second service
            # can enroll, but its authorized data operation cannot steal a live lease.
            active = Server(binary, state, credentials, root / "fenced.stderr")
            send("GET", "/v1/tenants/acme/projects/launch", "acme-admin", expected=503)
            active.finish()
            active = None
            time.sleep(32)
            active = Server(binary, state, credentials, root / "takeover.stderr")
            assert send("GET", "/v1/tenants/acme/projects/launch", "acme-viewer")["project"]["title"] == "Private acme launch"
            active.finish()
            active = None
            assert set(state.iterdir()) == orphan_sessions, "successful drain preserves interrupted-session evidence"
            print(json.dumps({"scenario": "passed", "checks": ["real-loopback-http", "private-random-credentials", "loopback-binding", "both-direction-tenant-isolation", "every-public-route", "denial-before-provisioning", "forged-target-rejection", "foreign-receipt-rejection", "resource-bound-retries", "explicit-expiry-without-absence-proof", "viewer-editor-admin-policy", "bounded-admin-pages", "lost-response-resolution", "durable-project-and-kv-replay", "durable-conflict", "independent-process-cold-restore", "sigkill", "live-owner-refusal", "expired-owner-takeover", "http-and-node-drain"]}))
        finally:
            if active:
                active.close()


if __name__ == "__main__":
    main()
