"""Serialize matched measurement windows without polling benchmark workers."""

import asyncio
import json
import re
import time


async def coordinate(workers, schedule, evidence):
    processes, readers, streams, events = {}, {}, {}, {}
    trace = []

    async def read_events(version):
        process = processes[version]
        while line := await process.stdout.readline():
            streams[version].write(line.decode())
            streams[version].flush()
            match = re.fullmatch(rb"RUSTFS gate (ready|done)=([^\s]+)\n", line)
            if match:
                await events[version].put((match[1].decode(), match[2].decode(), time.monotonic_ns()))
        await events[version].put(("exit", await process.wait(), time.monotonic_ns()))

    async def expect(version, kind, stage):
        event = await asyncio.wait_for(events[version].get(), timeout=600)
        if event[:2] != (kind, stage):
            raise RuntimeError(f"Coordinator expected {version} {kind} {stage}, received {event[:2]}")
        return event[2]

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
            record = {"stage": stage, "order": order, "windows": {}}
            for version in order:
                record["windows"][version] = {"ready_ns": await expect(version, "ready", stage)}
            # Both fixtures are idle and ready. No measured work overlaps.
            for version in order:
                record["windows"][version]["start_ns"] = time.monotonic_ns()
                await send(version, f"start {stage}")
                record["windows"][version]["done_ns"] = await expect(version, "done", stage)
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
