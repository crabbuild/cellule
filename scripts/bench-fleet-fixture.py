#!/usr/bin/env python3
"""Own the two private follower processes around the canonical capacity point."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time


def verify_durability(metrics, bench):
    """Refuse a follower result backed only by object publication or fallback."""
    bench.require(metrics is not None, "follower proof-source metrics missing")
    replies = metrics["response_sources"]
    submissions = metrics["submission_sources"]
    appends = metrics["node_log_append"]
    bench.require(replies["fleet"] > 0 and submissions["fleet"] > 0 and appends["successes"] > 0,
                  "point did not exercise follower-proven responses")
    bench.require(appends["failures"] == 0 and all(submissions[name] == 0 for name in
                  ("unsupported", "unavailable", "rejected")), "point used failed follower submission or append")


class Peer:
    def __init__(self, binary, directory, prefix, fixture, index, timeout):
        self.log_path = directory / f"follower-{index}.log"
        self.log = self.log_path.open("x")
        env = dict(os.environ, CELLULE_AXUM_FLEET_DIR=str(fixture),
                   CELLULE_AXUM_FOLLOWER=str(index), CELLULE_AXUM_BIND="127.0.0.1:0",
                   CELLULE_TEST_PREFIX=prefix)
        self.process = subprocess.Popen([str(binary)], env=env, stdout=self.log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"follower {index} exited before readiness: {self.log_path}")
                if "Follower service:" in self.log_path.read_text():
                    return
                time.sleep(0.05)
            raise TimeoutError(f"follower {index} startup timed out: {self.log_path}")
        except BaseException:
            self.stop()
            raise

    def stop(self):
        try:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    self.process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
                    raise RuntimeError(f"follower drain timed out; original data retained: {self.log_path}")
        finally:
            self.log.close()
        if self.process.returncode != 0:
            raise RuntimeError(f"follower failed with {self.process.returncode}: {self.log_path}")


@contextmanager
def followers(args, cells, rate, bench):
    if args.fleet_directory is None:
        yield
        return
    fixture = args.fleet_directory.resolve(strict=True)
    for name in ["ca.crt", *[f"node-{index}.{suffix}" for index in range(3) for suffix in ("crt", "key")]]:
        if not (fixture / name).is_file():
            raise RuntimeError(f"missing benchmark TLS fixture file: {fixture / name}")
    directory = args.output / f"followers-cells-{cells}-rate-{rate}"
    directory.mkdir()
    prefix = f'{os.environ["CELLULE_TEST_PREFIX"]}/cells-{cells}-rate-{rate}'
    previous = os.environ.get("CELLULE_AXUM_FLEET_DIR")
    peers = []
    stopped = threading.Event()
    sampling_errors = []
    sampler = None
    try:
        for index in (1, 2):
            peers.append(Peer(args.binary, directory, prefix, fixture, index, args.startup_timeout))
        os.environ["CELLULE_AXUM_FLEET_DIR"] = str(fixture)
        started = time.monotonic()

        def sample():
            try:
                with (directory / "resources.jsonl").open("x") as output:
                    while not stopped.is_set():
                        for index, peer in enumerate(peers, 1):
                            if peer.process.poll() is not None:
                                raise RuntimeError(f"follower {index} stopped while owner point was active")
                            resource = bench.process_usage(peer.process.pid)
                            resource.update(follower=index, seconds=time.monotonic() - started)
                            output.write(json.dumps(resource) + "\n")
                        output.flush()
                        stopped.wait(1)
            except BaseException as error:
                sampling_errors.append(str(error))

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        yield
    finally:
        stopped.set()
        if sampler is not None:
            sampler.join(timeout=5)
        if previous is None:
            os.environ.pop("CELLULE_AXUM_FLEET_DIR", None)
        else:
            os.environ["CELLULE_AXUM_FLEET_DIR"] = previous
        cleanup_errors = []
        for peer in reversed(peers):
            try:
                peer.stop()
            except BaseException as error:
                cleanup_errors.append(str(error))
        if sampling_errors or cleanup_errors or (sampler is not None and sampler.is_alive()):
            raise RuntimeError(f"follower fixture did not complete: {sampling_errors + cleanup_errors}")
