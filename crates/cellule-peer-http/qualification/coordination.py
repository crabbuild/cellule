"""Serialize matched measurement windows without polling benchmark workers."""

import asyncio
import json
import re
import time
from pathlib import Path


async def coordinate(workers, schedule, evidence):
    processes, readers, streams, events = {}, {}, {}, {}
    trace = []

    def host_pressure():
        paths = [Path('/proc/pressure') / kind for kind in ('cpu', 'io')]
        return {path.name: path.read_text() for path in paths if path.exists()}

    async def read_events(version):
        process = processes[version]
        while line := await process.stdout.readline():
            streams[version].write(line.decode())
            streams[version].flush()
            match = re.fullmatch(rb"RUSTFS gate (ready|done)=([^\s]+)\n", line)
            if match:
                await events[version].put((match[1].decode(), match[2].decode(), time.monotonic_ns(), time.time_ns()))
        await events[version].put(("exit", await process.wait(), time.monotonic_ns(), time.time_ns()))

    async def expect(version, kind, stage):
        event = await asyncio.wait_for(events[version].get(), timeout=600)
        if event[:2] != (kind, stage):
            raise RuntimeError(f"Coordinator expected {version} {kind} {stage}, received {event[:2]}")
        return event[2:]

    async def send(version, message):
        process = processes[version]
        process.stdin.write((message + "\n").encode())
        await process.stdin.drain()

    try:
        for version, (command, env, log) in workers.items():
            streams[version] = log.open("w")
            events[version] = asyncio.Queue()
            processes[version] = await asyncio.create_subprocess_exec(
                *command, env=env, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                limit=1 << 20,
            )
            readers[version] = asyncio.create_task(read_events(version))
        for stage, order in schedule:
            if set(order) != set(workers) or len(order) != len(workers):
                raise RuntimeError(f"Invalid measurement order: {order}")
            record = {"stage": stage, "order": order, "windows": {}, "host_pressure": {}}
            for version in order:
                ready, wall = await expect(version, "ready", stage)
                record["windows"][version] = {"ready_ns": ready, "ready_wall_ns": wall}
            # Both fixtures are idle and ready. No measured work overlaps.
            for version in order:
                record["host_pressure"][version] = {"before": host_pressure()}
                record["windows"][version]["start_ns"] = time.monotonic_ns()
                record["windows"][version]["start_wall_ns"] = time.time_ns()
                await send(version, f"start {stage}")
                done, wall = await expect(version, "done", stage)
                record["windows"][version].update(done_ns=done, done_wall_ns=wall)
                record["host_pressure"][version]["after"] = host_pressure()
            trace.append(record)
            evidence.write_text(json.dumps(trace, indent=2) + "\n")
            for version in order:
                await send(version, f"next {stage}")
        for version in workers:
            event = await asyncio.wait_for(events[version].get(), timeout=600)
            if event[:2] != ("exit", 0):
                raise RuntimeError(f"Coordinator expected successful {version} exit, received {event[:2]}")
    finally:
        for process in processes.values():
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10)
                except TimeoutError:
                    process.kill()
                    await process.wait()
        await asyncio.gather(*readers.values(), return_exceptions=True)
        for stream in streams.values():
            stream.close()
    return trace
