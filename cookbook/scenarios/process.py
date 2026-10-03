"""Bounded process observation for persistent cookbook qualification scenarios."""
import json
import os
import selectors
import signal
import subprocess
import time


def until(binary, arguments, complete, error_log, timeout=180):
    """Drain after exact progress evidence; retain the same bounded failure checks."""
    events = []
    process = None
    with error_log.open("w") as errors:
        try:
            process = subprocess.Popen([binary, *map(str, arguments)], stdout=subprocess.PIPE,
                                       stderr=errors, bufsize=0)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                buffered = b""
                deadline = time.monotonic() + timeout
                completed = False
                while time.monotonic() < deadline and not completed:
                    for key, _ in selector.select(timeout=1):
                        chunk = os.read(key.fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError(f"process exited before completion: {error_log.read_text()}")
                        buffered += chunk
                        while b"\n" in buffered:
                            line, buffered = buffered.split(b"\n", 1)
                            event = json.loads(line)
                            events.append(event)
                            completed = complete(events)
                            if completed:
                                break
                if not completed:
                    raise AssertionError(f"completion evidence absent after {timeout}s: {events}; {error_log.read_text()}")
            process.send_signal(signal.SIGTERM)
            tail, _ = process.communicate(timeout=25)
            events.extend(json.loads(line) for line in (buffered + tail).splitlines() if line)
            if process.returncode:
                raise RuntimeError(f"process failed during drain: {error_log.read_text()}")
            return events
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                if process.stdout is not None:
                    process.stdout.close()
