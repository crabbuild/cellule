"""Audit sparse point reads across 2,000 resident Cells in one Docker owner.

The generator shares the owner's 8-CPU/16-GiB budget. One seeded key per Cell
and local author handles make this a diagnostic, not mixed-load qualification.
"""

import argparse
from collections import Counter
import hashlib
import heapq
from pathlib import Path
import re
import struct

from entities import distribution, iter_rows, rows
from population import execute
from sql_slots import bind_query, read_slots, slot_timelines, summarize_tail

RATES = (5000, 10_000, 25_000, 50_000)
SECONDS = 60
ROW = struct.Struct(">9Q32s")


def verify_seed(seed, cell, entity):
    assert int(seed["entity"]) == int(cell["entity"]) == entity
    assert int(seed["minimum_sequence"]) == int(cell["sequence"]) > 0
    assert 0 <= int(seed["key"]) < 100_000 and int(seed["payload_bytes"]) == 1024
    assert re.fullmatch(r"[a-f0-9]{64}", seed["digest"])


def verify_attempt(fields, digest, seed, arrival, rate, elapsed):
    index, entity, scheduled, generated, dispatched, terminal, minimum, observed, outcome = fields
    assert index == arrival and entity == arrival % 2000, "substituted read arrival or Cell"
    assert scheduled == arrival * 1_000_000 // rate, "changed read schedule"
    assert scheduled <= generated <= dispatched <= terminal <= elapsed, "invalid read timestamps"
    assert minimum == int(seed["minimum_sequence"]) > 0, "changed minimum receipt"
    assert outcome in (1, 2, 3), "missing terminal read outcome"
    if outcome == 1:
        assert observed == minimum and digest.hex() == seed["digest"], "wrong read receipt or value"
    else:
        assert observed == 0 and digest == bytes(32), "failed read presented a receipt or value"
        if outcome == 3:
            assert terminal == dispatched, "client-full read was dispatched"
    return terminal - scheduled, generated - scheduled, terminal - dispatched


def fully_served(outcomes, latency_p99_us, generator_p99_us, elapsed_us, export_failed=0):
    return (outcomes[2] == outcomes[3] == 0 and latency_p99_us <= 50_000
            and generator_p99_us <= 5000 and elapsed_us <= 62_000_000 and export_failed == 0)


def verify_exports(window, observations):
    requests = window["export_requests"]
    accepted, failed, completed = (window[key] for key in ("export_accepted", "export_failed", "export_completed"))
    assert requests == accepted + failed and completed == accepted, "export request accounting mismatch"
    assert 0 < requests <= (window["elapsed_us"] + 999999) // 1_000_000
    periodic = [row for row in observations if row["terminal"] == "false"]
    terminal = [row for row in observations if row["terminal"] == "true"]
    assert len(periodic) == accepted and len(terminal) == 1, "lost export flush evidence"
    ordinals = [int(row["ordinal"]) for row in periodic]
    assert ordinals == sorted(set(ordinals)) and all(1 <= value <= requests for value in ordinals)
    assert observations[-1] == terminal[0] and int(terminal[0]["ordinal"]) == 0
    assert all(row["terminal"] in ("true", "false") and int(row["window"]) == window["window"]
               and int(row["queue_us"]) >= 0 and int(row["flush_us"]) >= 0 for row in observations)
    return dict(queue_wait=distribution([int(row["queue_us"]) for row in observations]),
                flush=distribution([int(row["flush_us"]) for row in observations]), failed=failed)


QUERY_PHASES = ("actor_queue_us", "worker_admission_us", "worker_queue_us", "execution_us", "reply_queue_us")
ACTOR_PHASES = ("actor_ingress_us", "cell_queue_us", "task_start_us")
ACTOR_STATES = {"Ready", "Renewal", "Busy", "Inventory", "Queued"}


def verify_query(row):
    total = int(row["total_us"])
    values = [int(row[key]) if row[key] != "" else None for key in QUERY_PHASES]
    reached = [value for value in values if value is not None]
    assert values == reached + [None] * (5 - len(reached)), "query phases skipped an earlier boundary"
    assert all(value >= 0 for value in reached) and total >= sum(reached), "invalid query phase timing"
    if len(reached) == 5:
        assert total - sum(reached) <= 4, "query phases do not partition total"
    assert row["succeeded"] in ("true", "false") and row["delivered"] in ("true", "false")
    if row["succeeded"] == "true":
        assert len(reached) == 5, "successful query has unreached phases"
    return values + [total]


def verify_actor(row):
    values = [int(row[key]) if row[key] else None for key in ACTOR_PHASES]
    reached = [value for value in values if value is not None]
    assert values == reached + [None] * (3 - len(reached)), "actor phases skipped a boundary"
    assert all(value >= 0 for value in reached), "negative actor phase"
    state = row["actor_state"]
    assert state == "" or state in ACTOR_STATES, "unbounded actor state"
    if row["actor_queue_us"] != "":
        assert len(reached) == 3 and state in ACTOR_STATES, "started query lacks actor boundaries"
        assert 0 <= int(row["actor_queue_us"]) - sum(reached) <= 2, "actor phases do not partition wait"
    else:
        assert values[-1] is None, "task started without actor start"
        assert sum(reached) <= int(row["total_us"]), "actor phases exceed total"
    if state:
        assert values[0] is not None, "enqueued state precedes actor receipt"
    if values[1] is not None:
        assert state in ACTOR_STATES, "dispatch lacks an enqueued state"
    return values, state


def counter_distribution(counts):
    count = sum(counts.values())
    if count == 0:
        return dict(count=0)
    targets = {p: (count * p + 99) // 100 for p in (50, 95, 99, 100)}
    quantiles, cumulative = {}, 0
    for value, occurrences in sorted(counts.items()):
        cumulative += occurrences
        for p, target in targets.items():
            if cumulative >= target and f"p{p}_ms" not in quantiles:
                quantiles[f"p{p}_ms"] = value / 1000
    return dict(count=count, **quantiles)


def verify_queries(control, windows):
    slots = read_slots(control, workers=8)
    seen_slots = set()
    timelines = slot_timelines(slots, workers=8)
    tails = [[] for _ in windows]
    phases = [[Counter() for _ in range(6)] for _ in windows]
    actor_phases = [[Counter() for _ in range(3)] for _ in windows]
    states = [Counter() for _ in windows]
    cell_waits = [{state: Counter() for state in ACTOR_STATES} for _ in windows]
    observed = [Counter() for _ in windows]
    population = {row["cell"] for row in rows(control / "population-cells.tsv")}
    for row in iter_rows(control / "node-0-queries.tsv"):
        values = verify_query(row)
        actor_values, state = verify_actor(row)
        bind_query(row, slots, workers=8, seen=seen_slots)
        assert row["cell"] in population, "query belongs to an unseeded Cell"
        at = int(row["at_ms"])
        index = next((index for index, window in enumerate(windows)
                      if window["start_at_ms"] <= at <= window["end_at_ms"]), None)
        if index is None:
            continue # Seed verification and final owner management are outside timed reads.
        observed[index]["count"] += 1
        observed[index]["succeeded"] += int(row["succeeded"] == "true")
        observed[index]["delivered"] += int(row["delivered"] == "true")
        heapq.heappush(tails[index], (values[-1], observed[index]["count"], int(row["job_id"]),
                                     values[:-1], actor_values, state))
        if len(tails[index]) > 3000:
            heapq.heappop(tails[index])
        for counter, value in zip(phases[index], values):
            if value is not None:
                counter[value] += 1
        states[index][state or "Unenqueued"] += 1
        for counter, value in zip(actor_phases[index], actor_values):
            if value is not None:
                counter[value] += 1
        if actor_values[1] is not None:
            cell_waits[index][state][actor_values[1]] += 1
    for window, counts, values, tail, actors, state_counts, waits in zip(
            windows, observed, phases, tails, actor_phases, states, cell_waits):
        success, error = window["outcomes"][1], window["outcomes"][2]
        assert success <= counts["count"] <= success + error, "missing or extra admitted query evidence"
        assert counts["succeeded"] >= success and counts["delivered"] == counts["count"], "query evidence contradicts client results"
        window["query_phases"] = dict(observations=dict(counts),
            **{key.removesuffix("_us"): counter_distribution(counter)
               for key, counter in zip(QUERY_PHASES + ("total_us",), values)})
        window["actor_phases"] = dict(enqueue_states=dict(state_counts),
            cell_queue_by_enqueue_state={state: counter_distribution(counter) for state, counter in sorted(waits.items())},
            **{key.removesuffix("_us"): counter_distribution(counter) for key, counter in zip(ACTOR_PHASES, actors)})
        window["sql_slot_tail"] = summarize_tail(tail, slots, timelines, QUERY_PHASES, ACTOR_PHASES)


def verify_window(row, raw, errors, seeds):
    sample = {key: int(value) for key, value in row.items() if key not in ("phase", "fully_served")}
    rate, elapsed = sample["rate"], sample["elapsed_us"]
    assert rate in RATES and sample["seconds"] == SECONDS and sample["concurrency"] == 1024
    assert sample["count"] == rate * SECONDS and len(raw) == sample["count"] * ROW.size, "lost read arrivals"
    assert elapsed >= SECONDS * 1_000_000
    assert 0 <= sample["cpu_us"] <= (elapsed + 20_000) * 8 + 100_000, "impossible owner CPU usage"
    assert 0 < sample["process_rss_bytes"]
    assert 0 < sample["memory_current_bytes"] <= sample["memory_peak_bytes"] <= 16 * 2**30
    assert sample["active_cells"] == 2000
    assert 0 < sample["start_at_ms"] <= sample["end_at_ms"]
    assert 0 < sample["sqlite_descriptors"] <= sample["descriptors"] <= 65_536
    error_map = {int(row["arrival"]): row["error"] for row in errors}
    assert len(error_map) == len(errors) and all(error_map.values()), "missing or duplicate read error"
    latency, generator, execution = [], [], []
    outcomes, timely = {1: 0, 2: 0, 3: 0}, 0
    previous_generated = previous_dispatched = 0
    for arrival, record in enumerate(ROW.iter_unpack(raw)):
        fields, digest = record[:9], record[9]
        delays = verify_attempt(fields, digest, seeds[arrival % 2000], arrival, rate, elapsed)
        assert fields[3] >= previous_generated and fields[4] >= previous_dispatched, "read clock moved backward"
        previous_generated, previous_dispatched = fields[3:5]
        outcome = fields[8]
        assert (arrival in error_map) == (outcome == 2), "read error evidence disagrees with outcome"
        if outcome == 2:
            del error_map[arrival]
        outcomes[outcome] += 1
        timely += int(outcome == 1 and fields[5] <= SECONDS * 1_000_000)
        latency.append(delays[0])
        generator.append(delays[1])
        execution.append(delays[2])
    assert not error_map, "error for an unplanned read"
    scheduled_distribution, generator_distribution = distribution(latency), distribution(generator)
    passed = fully_served(outcomes, scheduled_distribution["p99_ms"] * 1000,
                         generator_distribution["p99_ms"] * 1000, elapsed, sample["export_failed"])
    assert row["fully_served"] == str(passed).lower(), "read gate disagrees with raw arrivals"
    return dict(**sample, phase=row["phase"], outcomes=outcomes, fully_served=passed,
                original_success_tps=timely / SECONDS, scheduled_latency=scheduled_distribution,
                generator_delay=generator_distribution, dispatched_latency=distribution(execution),
                cpu_percent_of_eight_cores=sample["cpu_us"] / elapsed / 8 * 100,
                raw_sha256=hashlib.sha256(raw).hexdigest())


def verify_owner_reads(control: Path):
    seeds = rows(control / "read-seeds.tsv")
    population = rows(control / "population-cells.tsv")
    assert len(seeds) == len(population) == 2000, "missing read seed"
    for entity, (seed, cell) in enumerate(zip(seeds, population)):
        verify_seed(seed, cell, entity)
    windows = rows(control / "read-windows.tsv")
    assert 2 <= len(windows) <= 5, "missing measured read window"
    expected = [(0, "warmup", 5000)] + [(index + 1, "measure", rate) for index, rate in enumerate(RATES)]
    assert [(int(row["window"]), row["phase"], int(row["rate"])) for row in windows] == expected[:len(windows)], "changed read ramp"
    reports = []
    exports = rows(control / "read-export.tsv")
    assert all(int(row["window"]) in range(len(windows)) for row in exports), "unplanned export window"
    for row in windows:
        id = row["window"]
        reports.append(verify_window(row, (control / f"read-{id}.bin").read_bytes(),
                                     rows(control / f"read-{id}-errors.tsv"), seeds))
        reports[-1]["evidence_export"] = verify_exports(reports[-1], [row for row in exports if row["window"] == id])
    assert all(left["end_at_ms"] < right["start_at_ms"] for left, right in zip(reports, reports[1:])), "overlapping query observation windows"
    verify_queries(control, reports)
    assert all(row["fully_served"] for row in reports[1:-1]), "ramp continued after a failed read point"
    assert len(reports) == 5 or not reports[-1]["fully_served"], "ramp stopped before capacity limit"
    supported = [row["rate"] for row in reports[1:] if row["fully_served"]]
    return dict(scope=__doc__.strip(), qualification_pass=False, windows=reports,
                arrivals_verified=sum(row["count"] for row in reports),
                highest_supported_diagnostic_rate=max(supported, default=0),
                seed_sha256=hashlib.sha256((control / "read-seeds.tsv").read_bytes()).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--docker-context", required=True)
    parser.add_argument("--compose-file", type=Path, action="append", default=[])
    execute(parser.parse_args(), read_mode=True, read_verifier=verify_owner_reads)


if __name__ == "__main__":
    main()
