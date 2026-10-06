"""Audit scheduled entity traffic against receipts, ownership and resource samples."""

from __future__ import annotations

import bisect
from collections import Counter
import csv
import hashlib
import re
from pathlib import Path

STAGES = (3, 5, 10, 20)
SHAPES = ("uniform", "hot", "skewed")
POINTS = ((1, 4), (4, 16), (16, 64))
CAPACITY_POINTS = ((2, 8), (4, 16), (16, 64), (24, 96), (32, 128),
                   (48, 192), (64, 256), (96, 256), (128, 256),
                   (192, 256), (256, 256), (1024, 256))
CELLS_PER_NODE = 4
SECONDS = 10
CAPACITY_DRAIN_GRACE_US = 2_000_000


def iter_rows(path: Path):
    with path.open(newline="") as source:
        yield from csv.DictReader(source, delimiter="\t")


def rows(path: Path) -> list[dict]:
    return list(iter_rows(path))


def destination(shape: str, arrival: int, cells: int) -> tuple[int, str]:
    if shape == "uniform":
        return arrival % cells, "write"
    if shape == "hot":
        return (1 + (arrival // 5) % (cells - 1) if arrival % 5 == 4 else 0), "write"
    assert shape == "skewed"
    return ((arrival // 5) % cells, "write") if arrival % 5 == 0 else (arrival % 4, "read")


def distribution(values: list[int]) -> dict:
    ordered = sorted(values)
    if not ordered:
        return dict(count=0)
    return dict(count=len(values), **{
        f"p{p}_ms": ordered[(len(ordered) * p + 99) // 100 - 1] / 1000
        for p in (50, 95, 99, 100)
    })


def verify_object_operations(control: Path, node: int, observations: list[dict]) -> list[dict]:
    samples = rows(control / f"node-{node}-object-operations.tsv")
    assert Counter((row["operation"], row["outcome"]) for row in samples) == {
        (row["operation"], row["outcome"]): int(row["count"]) for row in observations
        if int(row["count"]) > 0}, "object operation samples disagree with counters"
    for row in samples:
        assert int(row["at_ms"]) > 0
        assert all(int(row[key]) >= 0 for key in ("duration_us", "bytes_read", "bytes_written"))
    return samples


def write_phase_summary(store: list[dict], batches: list[dict]) -> dict:
    return dict(
        follower_store=dict(
            attempts=len(store), failures=sum(row["succeeded"] == "false" for row in store),
            frames=sum(int(row["frames"]) for row in store),
            encoded_bytes=sum(int(row["encoded_bytes"]) for row in store),
            data_sync_calls=sum(int(row["data_sync_calls"]) for row in store),
            directory_sync_calls=sum(int(row["directory_sync_calls"]) for row in store),
            recounts=sum(int(row["recounts"]) for row in store),
            **{phase: distribution([int(row[f"{phase}_us"]) for row in store])
               for phase in ("blocking_queue", "accounting_wait", "accounting_hold", "lane_wait",
                             "append", "prune", "data_sync", "directory_sync", "recount", "total")}),
        node_log_batch=dict(
            batches=len(batches), failures=sum(row["succeeded"] == "false" for row in batches),
            frames=sum(int(row["frames"]) for row in batches),
            completed_captures=sum(int(row["completed_captures"]) for row in batches),
            encoded_bytes=sum(int(row["encoded_bytes"]) for row in batches),
            replica_bytes=sum(int(row["encoded_bytes"]) * int(row["members"]) for row in batches),
            **{phase: distribution([int(row[f"{phase}_us"]) for row in batches])
               for phase in ("queue_wait", "collection", "append")}))


def verify_write_phases(store: list[dict], batches: list[dict], appends: list[dict]) -> dict:
    for row in store:
        assert int(row["at_ms"]) > 0 and row["succeeded"] in {"true", "false"}
        assert re.fullmatch(r"SessionId\([0-9a-f]{32}\)", row["leader"]) and int(row["epoch"]) > 0
        assert 1 <= int(row["frames"]) <= 64 and int(row["encoded_bytes"]) > 0
        assert int(row["data_sync_calls"]) >= 0 and int(row["recounts"]) >= 0
        assert int(row["directory_sync_calls"]) >= 0
        total = int(row["total_us"])
        assert total >= 0
        for phase in ("blocking_queue", "accounting_wait", "accounting_hold", "lane_wait",
                      "append", "prune", "data_sync", "directory_sync", "recount"):
            assert 0 <= int(row[f"{phase}_us"]) <= total, "follower phase exceeds total"
        assert int(row["prune_us"]) <= int(row["append_us"]), "prune exceeds append"
        assert int(row["data_sync_us"]) <= int(row["append_us"]), "data sync exceeds append"
        assert int(row["directory_sync_us"]) <= int(row["append_us"]), "directory sync exceeds append"
    assert len(batches) == len(appends), "missing node-log batch evidence"
    for row, append in zip(batches, appends):
        assert int(row["at_ms"]) > 0 and row["succeeded"] in {"true", "false"}
        assert re.fullmatch(r"SessionId\([0-9a-f]{32}\)", row["leader"]) and int(row["epoch"]) > 0
        assert 1 <= int(row["frames"]) <= 64
        assert 0 < int(row["first_sequence"]) <= int(row["last_sequence"])
        assert int(row["last_sequence"]) - int(row["first_sequence"]) + 1 == int(row["frames"]), \
            "batch sequence range differs from frames"
        assert 0 <= int(row["completed_captures"]) <= int(row["frames"])
        assert int(row["encoded_bytes"]) > 0 and 1 <= int(row["members"]) <= 2
        for phase in ("queue_wait", "collection", "append"):
            assert int(row[f"{phase}_us"]) >= 0
        assert row["succeeded"] == append["acknowledged"], "batch outcome differs from append"
        assert int(row["encoded_bytes"]) * int(row["members"]) == int(append["bytes"]), \
            "batch replica bytes differ from append"
    return write_phase_summary(store, batches)


def transport_phase_summary(transport: list[dict]) -> dict:
    return dict(connection_attempts=sum(int(row["connection_attempts"]) for row in transport),
                **{key.removesuffix("_us"): distribution([int(row[key]) for row in transport])
                   for key in ("pool_wait_us", "member_resolution_us", "encoding_us", "address_resolution_us", "connection_us", "wire_us", "verification_us", "total_us")})


SUBMISSION_PHASES = ("byte_admission", "queue_admission", "capture_load", "ticket_order", "encoding")
CONTROL_TRANSITIONS = {"Renew", "Activate", "Publish", "Migrate", "Release", "AttachRecovery",
                       "PublishRecovery", "Takeover", "Tombstone"}


def submission_phase_summary(submissions: list[dict]) -> dict:
    return dict(attempts=len(submissions),
                outcomes=dict(Counter(row["enqueued"] or "cancelled" for row in submissions)),
                **{phase: distribution([int(row[f"{phase}_us"]) for row in submissions
                                        if row[f"{phase}_us"] != ""])
                   for phase in (*SUBMISSION_PHASES, "total")})


def verify_submission_timings(submissions: list[dict]) -> dict:
    seen = set()
    for row in submissions:
        assert int(row["at_ms"]) > 0 and re.fullmatch(r"CellId\([0-9a-f]{64}\)", row["cell"])
        key = (row["cell"], int(row["commit_sequence"]))
        assert key not in seen and key[1] > 0, "duplicate or invalid node-log submission"
        seen.add(key)
        assert int(row["encoded_bytes"]) > 0 and int(row["total_us"]) >= 0
        assert row["enqueued"] in {"true", "false", ""}
        values = [row[f"{phase}_us"] for phase in SUBMISSION_PHASES]
        missing = False
        for value in values:
            if value == "":
                missing = True
            else:
                assert not missing, "submission completed a phase after a missing boundary"
                assert 0 <= int(value) <= int(row["total_us"])
        assert sum(int(value) for value in values if value != "") <= int(row["total_us"]), \
            "disjoint submission phases exceed total"
        if row["enqueued"] == "true":
            assert all(value != "" for value in values), "enqueued submission has incomplete phases"
            assert int(row["first_sequence"]) > 0
        else:
            assert row["first_sequence"] == "", "uncommitted submission has a node sequence"
    return submission_phase_summary(submissions)


def control_transition_summary(transitions: list[dict]) -> dict:
    return {transition: dict(attempts=len(selected),
                             outcomes=dict(Counter(row["succeeded"] or "cancelled" for row in selected)),
                             duration=distribution([int(row["elapsed_us"]) for row in selected]))
            for transition in sorted({row["transition"] for row in transitions})
            for selected in [[row for row in transitions if row["transition"] == transition]]}


def verify_control_transitions(transitions: list[dict]) -> dict:
    for row in transitions:
        assert int(row["at_ms"]) > 0 and re.fullmatch(r"CellId\([0-9a-f]{64}\)", row["cell"])
        assert row["transition"] in CONTROL_TRANSITIONS
        assert int(row["elapsed_us"]) >= 0 and row["succeeded"] in {"true", "false", ""}
    return control_transition_summary(transitions)


def verify_trace_counts(control: Path, node: int) -> dict:
    files = dict(object_operations="object-operations", proofs="durability",
                 responses="responses", executions="executions", queries="queries", sql_slots="sql-slots", publications="publications",
                 phases="phases", captures="captures", publication_costs="publication-costs",
                 follower_appends="follower-appends", follower_network="follower-network",
                 follower_transport="follower-transport",
                 follower_store="follower-store", node_log_batches="node-log-batches",
                 node_log_submissions="node-log-submissions", control_transitions="control-transitions",
                 node_log_events="node-log-events")
    counts = rows(control / f"node-{node}-trace-counts.tsv")
    assert len(counts) == len(files) and {row["buffer"] for row in counts} == set(files), "missing or duplicate trace buffer counts"
    report = {}
    for row in counts:
        name = row["buffer"]
        assert int(row["capacity"]) == 65_536, "trace buffer bound changed"
        assert int(row["dropped"]) == 0, f"{name}: lost trace events"
        assert int(row["buffered"]) == 0, f"{name}: trace events were not drained"
        recorded = int(row["recorded"])
        exported = sum(1 for _ in iter_rows(control / f"node-{node}-{files[name]}.tsv"))
        # The legacy durability file contains only object proofs; Fleet proofs
        # remain represented by the command-response and node-log evidence.
        assert (recorded >= exported if name == "proofs" else recorded == exported), f"{name}: trace count differs from exported rows"
        report[name] = dict(recorded=recorded, exported=exported, dropped=0)
    return report


def verify_timing_evidence(control: Path, node: int, windows: list[dict]) -> dict:
    responses = rows(control / f"node-{node}-responses.tsv")
    executions = rows(control / f"node-{node}-executions.tsv")
    publications = rows(control / f"node-{node}-publications.tsv")
    phases = rows(control / f"node-{node}-phases.tsv")
    captures = rows(control / f"node-{node}-captures.tsv")
    costs = rows(control / f"node-{node}-publication-costs.tsv")
    appends = rows(control / f"node-{node}-follower-appends.tsv")
    network = rows(control / f"node-{node}-follower-network.tsv")
    transport = rows(control / f"node-{node}-follower-transport.tsv")
    store = rows(control / f"node-{node}-follower-store.tsv")
    batches = rows(control / f"node-{node}-node-log-batches.tsv")
    submissions = rows(control / f"node-{node}-node-log-submissions.tsv")
    transitions = rows(control / f"node-{node}-control-transitions.tsv")
    verify_submission_timings(submissions)
    verify_control_transitions(transitions)
    log_events = rows(control / f"node-{node}-node-log-events.tsv")
    assert responses, f"node {node}: missing command response evidence"
    assert executions, f"node {node}: missing command execution evidence"
    assert publications, f"node {node}: missing publication evidence"
    assert phases and captures and costs, f"node {node}: missing LTX or publication phase evidence"
    sources = {"Recorded", "Fleet", "Object"}
    for row in responses:
        assert row["source"] in sources
        assert int(row["at_ms"]) > 0
        assert int(row["response_us"]) >= int(row["confirmation_us"]) >= 0
    for row in executions:
        assert int(row["at_ms"]) > 0
        assert int(row["queue_wait_us"]) >= 0 and int(row["worker_round_trip_us"]) >= 0
        assert row["succeeded"] in {"true", "false"}
    seen = set()
    for row in publications:
        key = (row["cell"], int(row["sequence"]))
        assert key not in seen, f"node {node}: duplicate publication"
        seen.add(key)
        assert int(row["at_ms"]) > 0 and int(row["sequence"]) > 0
        assert row["succeeded"] in {"true", "false"}
        total = int(row["total_us"])
        assert total >= 0
        for phase in ("queue_wait_us", "preparation_us", "authority_us"):
            assert 0 <= int(row[phase]) <= total
    for row in phases:
        assert int(row["at_ms"]) > 0 and int(row["elapsed_us"]) >= 0
        assert row["succeeded"] in {"true", "false"}
    for row in captures:
        assert int(row["at_ms"]) > 0 and row["succeeded"] in {"true", "false"}
        total = int(row["total_us"])
        assert total >= 0
        for key in ("preparation_us", "schema_check_us", "wal_read_us", "page_collection_us",
                    "verification_us", "encode_us", "local_write_us", "fsync_us", "checkpoint_us"):
            assert 0 <= int(row[key]) <= total
        assert int(row["wal_bytes"]) >= 0 and int(row["ltx_bytes"]) >= 0
    for row in costs:
        assert int(row["at_ms"]) > 0 and int(row["objects"]) >= 0 and int(row["bytes"]) >= 0
    for row in appends:
        assert int(row["at_ms"]) > 0 and int(row["bytes"]) >= 0
        assert row["acknowledged"] in {"true", "false"}
    for row in network:
        assert int(row["at_ms"]) > 0
        assert int(row["bytes"]) >= 0 and int(row["duration_us"]) >= 0
        assert row["acknowledged"] in {"true", "false"}
    assert len(transport) == len(network), "missing transport phase evidence"
    for row, aggregate in zip(transport, network, strict=True):
        assert (row["at_ms"], row["acknowledged"], row["bytes"], row["total_us"]) == (
            aggregate["at_ms"], aggregate["acknowledged"], aggregate["bytes"], aggregate["duration_us"]), "transport phases differ from aggregate"
        total = int(row["total_us"])
        for key in ("pool_wait_us", "member_resolution_us", "encoding_us", "address_resolution_us", "connection_us", "wire_us", "verification_us"):
            assert 0 <= int(row[key]) <= total, f"{key}: transport phase exceeds total"
        assert 0 <= int(row["connection_attempts"]) <= 1
    write_phases = verify_write_phases(store, batches, appends)
    last_covered = 0
    for row in log_events:
        assert int(row["at_ms"]) > 0 and int(row["epoch"]) > 0
        assert row["phase"] in {"enrolled", "active", "coverage", "closed"}
        covered = int(row["covered_through"])
        if row["phase"] in {"enrolled", "active"}:
            assert covered == 0, "node-log lifecycle marker has coverage"
        else:
            assert covered >= last_covered, "node-log coverage regressed"
            last_covered = covered
    for window in windows:
        if node >= window["nodes"]:
            continue
        start, end = window["started_ms"], window["ended_ms"]
        selected_responses = [row for row in responses if start <= int(row["at_ms"]) <= end]
        selected_executions = [row for row in executions if start <= int(row["at_ms"]) <= end]
        selected_publications = [row for row in publications if start <= int(row["at_ms"]) <= end]
        selected_phases = [row for row in phases if start <= int(row["at_ms"]) <= end]
        selected_captures = [row for row in captures if start <= int(row["at_ms"]) <= end]
        selected_costs = [row for row in costs if start <= int(row["at_ms"]) <= end]
        selected_appends = [row for row in appends if start <= int(row["at_ms"]) <= end]
        selected_network = [row for row in network if start <= int(row["at_ms"]) <= end]
        selected_transport = [row for row in transport if start <= int(row["at_ms"]) <= end]
        selected_store = [row for row in store if start <= int(row["at_ms"]) <= end]
        selected_batches = [row for row in batches if start <= int(row["at_ms"]) <= end]
        selected_submissions = [row for row in submissions if start <= int(row["at_ms"]) <= end]
        selected_transitions = [row for row in transitions if start <= int(row["at_ms"]) <= end]
        covered_before_end = [int(row["covered_through"]) for row in log_events
                              if row["phase"] in {"coverage", "closed"} and int(row["at_ms"]) <= end]
        response_sources = {source: sum(row["source"] == source for row in selected_responses)
                            for source in sorted(sources)}
        window.setdefault("node_durability", {})[node] = dict(
            response_sources=response_sources,
            response_latency=distribution([int(row["response_us"]) for row in selected_responses]),
            confirmation_latency=distribution([int(row["confirmation_us"]) for row in selected_responses]),
            actor_queue=distribution([int(row["queue_wait_us"]) for row in selected_executions]),
            worker_round_trip=distribution([int(row["worker_round_trip_us"]) for row in selected_executions]),
            worker_failures=sum(row["succeeded"] == "false" for row in selected_executions),
            publication_total=distribution([int(row["total_us"]) for row in selected_publications]),
            publication_queue=distribution([int(row["queue_wait_us"]) for row in selected_publications]),
            publication_preparation=distribution([int(row["preparation_us"]) for row in selected_publications]),
            publication_authority=distribution([int(row["authority_us"]) for row in selected_publications]),
            published_roots_per_second=sum(row["succeeded"] == "true" for row in selected_publications)
            * 1_000_000 / window["elapsed_us"],
            latest_published_sequence_by_cell={cell: max(int(row["sequence"]) for row in publications
                                                         if row["cell"] == cell and row["succeeded"] == "true"
                                                         and int(row["at_ms"]) <= end)
                                               for cell in {row["cell"] for row in publications
                                                            if row["succeeded"] == "true" and int(row["at_ms"]) <= end}},
            failed_publications=sum(row["succeeded"] == "false" for row in selected_publications))
        window["node_durability"][node].update(
            ltx_phases={phase: distribution([int(row["elapsed_us"]) for row in selected_phases
                                             if row["phase"] == phase])
                        for phase in sorted({row["phase"] for row in selected_phases})},
            capture_total=distribution([int(row["total_us"]) for row in selected_captures]),
            uploaded_objects=sum(int(row["objects"]) for row in selected_costs),
            uploaded_bytes=sum(int(row["bytes"]) for row in selected_costs),
            follower_appends=len(selected_appends),
            follower_append_failures=sum(row["acknowledged"] == "false" for row in selected_appends),
            follower_append_bytes=sum(int(row["bytes"]) for row in selected_appends),
            follower_network_latency=distribution([int(row["duration_us"]) for row in selected_network]),
            follower_network_bytes=sum(int(row["bytes"]) for row in selected_network),
            follower_transport=transport_phase_summary(selected_transport),
            node_log_submission=submission_phase_summary(selected_submissions),
            control_transitions=control_transition_summary(selected_transitions),
            node_log_covered_through=max(covered_before_end, default=0),
            **write_phase_summary(selected_store, selected_batches))
    return dict(response_sources={source: sum(row["source"] == source for row in responses)
                                  for source in sorted(sources)},
                response_latency=distribution([int(row["response_us"]) for row in responses]),
                actor_queue=distribution([int(row["queue_wait_us"]) for row in executions]),
                worker_round_trip=distribution([int(row["worker_round_trip_us"]) for row in executions]),
                publication_total=distribution([int(row["total_us"]) for row in publications]),
                capture_total=distribution([int(row["total_us"]) for row in captures]),
                uploaded_objects=sum(int(row["objects"]) for row in costs),
                uploaded_bytes=sum(int(row["bytes"]) for row in costs),
                follower_appends=len(appends),
                acknowledged_follower_appends=sum(row["acknowledged"] == "true" for row in appends),
                follower_append_bytes=sum(int(row["bytes"]) for row in appends),
                acknowledged_network_appends=sum(row["acknowledged"] == "true" for row in network),
                follower_network_latency=distribution([int(row["duration_us"]) for row in network]),
                follower_transport=transport_phase_summary(transport),
                node_log_submission=submission_phase_summary(submissions),
                control_transitions=control_transition_summary(transitions),
                node_log_phases=dict(Counter(row["phase"] for row in log_events)),
                node_log_epochs=sorted({int(row["epoch"]) for row in log_events}),
                node_log_covered_through=last_covered,
                completed_publications=sum(row["succeeded"] == "true" for row in publications),
                failed_publications=sum(row["succeeded"] == "false" for row in publications),
                **write_phases)


def verify_window(control: Path, nodes: int, shape: str, rate_per_node: int,
                  concurrency: int, window_id: int, positions: dict[int, list[int]],
                  prefix: str = "entities") -> dict:
    label = f"{prefix}-{nodes}-{shape}-{rate_per_node}"
    metadata, = rows(control / f"{label}-window.tsv")
    assert metadata["shape"] == shape
    for key, expected in dict(window_id=window_id, nodes=nodes, rate_per_node=rate_per_node,
                              concurrency=concurrency, seconds=SECONDS).items():
        assert int(metadata[key]) == expected
    elapsed_us = int(metadata["elapsed_us"])
    assert elapsed_us >= SECONDS * 1_000_000
    assert int(metadata["ended_boot_ms"]) >= int(metadata["started_boot_ms"]) + SECONDS * 1000
    rate = nodes * rate_per_node
    planned = rate * SECONDS
    samples = sorted(rows(control / f"{label}.tsv"), key=lambda row: int(row["arrival"]))
    assert [int(row["arrival"]) for row in samples] == list(range(planned)), "missing or duplicate arrival"
    successes, arrival_latencies, scheduled_latencies, writes, actions = [], [], [], [0] * nodes, [0] * nodes
    generator_latencies, handoff_latencies = [], []
    outcomes, intervals = {}, []
    new_positions = {entity: [] for entity in range(nodes * CELLS_PER_NODE)}
    for sample in samples:
        arrival = int(sample["arrival"])
        scheduled = int(sample["scheduled_us"])
        started = int(sample["started_us"])
        generated = int(sample["generator_started_us"])
        elapsed = int(sample["elapsed_us"])
        entity = int(sample["entity"])
        sequence, read_sequence, count = [int(sample[key]) for key in ("sequence", "read_sequence", "count")]
        outcome = sample["outcome"]
        assert scheduled == arrival * 1_000_000 // rate
        assert started >= scheduled and elapsed >= 0
        assert scheduled <= generated <= started, "generator timing is outside scheduled dispatch"
        generator_latencies.append(generated - scheduled)
        handoff_latencies.append(started - generated)
        assert (entity, sample["kind"]) == destination(shape, arrival, nodes * CELLS_PER_NODE)
        assert outcome in {"ok", "resolved", "write_only", "not_started", "absent", "client_full", "scheduler_late", "read_failed"}
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        scheduled_latencies.append(started + elapsed - scheduled)
        if outcome in ("client_full", "scheduler_late"):
            assert elapsed == sequence == read_sequence == count == 0
            if outcome == "scheduler_late":
                assert started >= (arrival + 1) * 1_000_000 // rate
            continue
        assert started < (arrival + 1) * 1_000_000 // rate
        assert started + elapsed <= elapsed_us
        intervals += [(started, 1), (started + elapsed, -1)]
        if sequence:
            assert sample["kind"] == "write" and outcome in ("ok", "resolved", "write_only")
            new_positions[entity].append(sequence)
            writes[entity // CELLS_PER_NODE] += 1
        if outcome in ("ok", "resolved"):
            assert read_sequence >= max(sequence, 1)
            assert sequence > 0 if sample["kind"] == "write" else sequence == 0
            successes.append(elapsed)
            actions[entity // CELLS_PER_NODE] += 1
            arrival_latencies.append(started + elapsed - scheduled)
        else:
            assert read_sequence == count == 0
            assert bool(sequence) == (outcome == "write_only")
    for entity, values in new_positions.items():
        previous = positions.setdefault(entity, [])
        assert len(values) == len(set(values)), "different requests reused one write receipt"
        assert not values or min(values) > max(previous, default=0), "write receipt regressed"
        previous.extend(sorted(values))
    for sample in samples:
        if sample["outcome"] in ("ok", "resolved"):
            expected = bisect.bisect_right(positions[int(sample["entity"])], int(sample["read_sequence"]))
            assert int(sample["count"]) == expected, "read value disagrees with its receipt"
    checks = rows(control / f"{label}-readback.tsv")
    assert [int(row["entity"]) for row in checks] == list(range(nodes * CELLS_PER_NODE))
    for row in checks:
        positions_for_cell = positions[int(row["entity"])]
        assert int(row["actual"]) == int(row["expected"]) == len(positions_for_cell), "lost or duplicated write"
        assert int(row["sequence"]) >= max(positions_for_cell, default=0)
    inflight, peak = 0, 0
    for _, delta in sorted(intervals):
        inflight += delta
        assert inflight >= 0
        peak = max(peak, inflight)
    assert inflight == 0 and peak <= concurrency
    return dict(nodes=nodes, shape=shape, rate_per_node=rate_per_node, concurrency=concurrency,
                generator_lateness=distribution(generator_latencies),
                generator_handoff=distribution(handoff_latencies),
                planned=planned, outcomes=outcomes, fully_served_arrivals=len(successes) == planned,
                acknowledged_writes_by_node=writes, completed_actions=len(successes),
                completed_actions_by_node=actions,
                latest_write_sequence_by_entity={entity: max(values, default=0)
                                                 for entity, values in positions.items()},
                completed_writes_per_second=sum(writes) * 1_000_000 / elapsed_us,
                completed_per_second=len(successes) * 1_000_000 / elapsed_us,
                service_latency=distribution(successes), arrival_latency=distribution(arrival_latencies),
                scheduled_arrival_latency=distribution(scheduled_latencies),
                started_ms=int(metadata["started_ms"]), ended_ms=int(metadata["ended_ms"]),
                started_boot_ms=int(metadata["started_boot_ms"]), ended_boot_ms=int(metadata["ended_boot_ms"]),
                wall_clock_adjustment_ms=int(metadata["ended_ms"]) - int(metadata["started_ms"]) - elapsed_us / 1000,
                elapsed_us=elapsed_us, peak_client_inflight=peak)


def verify_capacity_windows(control: Path, positions: dict[int, list[int]]) -> list[dict]:
    schedule = rows(control / "capacity-windows.tsv")
    assert schedule, "missing capacity schedule"
    assert [int(row["window_id"]) for row in schedule] == list(range(len(schedule))), "missing capacity window"
    windows = []
    for shape in SHAPES:
        selected = [row for row in schedule if row["shape"] == shape]
        assert 2 <= len(selected) <= len(CAPACITY_POINTS), f"{shape}: incomplete rate ramp"
        assert [(int(row["rate_per_node"]), int(row["concurrency"])) for row in selected] == list(CAPACITY_POINTS[:len(selected)]), f"{shape}: rate ramp changed"
        assert all(row["fully_served"] == "true" for row in selected[:-1]), f"{shape}: ramp continued after overload"
        assert selected[-1]["fully_served"] == "false", f"{shape}: missing overload point"
        for row in selected:
            result = verify_window(control, 3, shape, int(row["rate_per_node"]),
                                   int(row["concurrency"]), int(row["window_id"]), positions,
                                   prefix="capacity")
            result["fully_served_window"] = (result["fully_served_arrivals"] and
                                             result["elapsed_us"] <= SECONDS * 1_000_000 + CAPACITY_DRAIN_GRACE_US)
            assert result["fully_served_window"] == (row["fully_served"] == "true"), "mislabeled fully served rate"
            windows.append(result)
    assert [window["shape"] for window in windows] == [row["shape"] for row in schedule], "capacity shape order changed"
    return windows


def verify_follower_proof(resources: dict) -> dict:
    fleet_proofs = sum(resource["durability"]["response_sources"]["Fleet"]
                       for resource in resources.values())
    acknowledged_appends = sum(resource["durability"]["acknowledged_follower_appends"]
                               for resource in resources.values())
    network_appends = sum(resource["durability"]["acknowledged_network_appends"]
                          for resource in resources.values())
    assert fleet_proofs > 0, "follower lane returned no follower-proof responses"
    assert acknowledged_appends > 0, "missing acknowledged follower append evidence"
    assert network_appends > 0, "missing network follower append evidence"
    for resource in resources.values():
        phases = resource["durability"]["node_log_phases"]
        assert phases.get("enrolled", 0) == phases.get("active", 0) == phases.get("closed", 0) == 1, \
            "follower generation did not enroll, activate, and close exactly once"
        assert resource["durability"]["node_log_epochs"] == [1], "follower epoch changed"
    return dict(follower_proof_responses=fleet_proofs, follower_appends=acknowledged_appends,
                network_follower_appends=network_appends)


def verify_root_coverage(roots: list[dict], positions: dict[int, list[int]],
                         identity: dict[int, tuple], cells: int) -> None:
    assert [int(row["entity"]) for row in roots] == list(range(cells))
    for row in roots:
        entity = int(row["entity"])
        assert (row["cell"], row["owner"], row["epoch"], row["incarnation"]) == identity[entity]
        assert positions[entity], f"Cell {entity} received no acknowledged writes"
        assert int(row["root_sequence"]) >= max(positions[entity]), "published root does not cover writes"


def verify_entities(control: Path, capacity: bool = False, follower: bool = False) -> dict:
    assert not follower or capacity
    stages = (3,) if capacity else STAGES
    evidence_prefix = "capacity" if capacity else "entity"
    owners = rows(control / f"{evidence_prefix}-owners.tsv")
    identity = {}
    for stage in stages:
        selected = [row for row in owners if int(row["stage"]) == stage]
        assert [int(row["entity"]) for row in selected] == list(range(stage * CELLS_PER_NODE))
        for row in selected:
            entity = int(row["entity"])
            assert int(row["owner"]) == entity // CELLS_PER_NODE
            value = (row["cell"], row["owner"], row["epoch"], row["incarnation"])
            assert identity.setdefault(entity, value) == value, "existing Cell ownership changed"
        ingress = list(map(int, (control / f"{evidence_prefix}-ingress-{stage}.txt").read_text().split()))
        assert len(ingress) == stage and min(ingress) > 0 and max(ingress) - min(ingress) <= 1
    assert len({value[0] for value in identity.values()}) == stages[-1] * CELLS_PER_NODE, "entity targets collapsed"
    windows, positions = [], {}
    for nodes in stages:
        if capacity:
            windows.extend(verify_capacity_windows(control, positions))
        else:
            for shape in SHAPES:
                for rate, concurrency in POINTS:
                    windows.append(verify_window(control, nodes, shape, rate, concurrency, len(windows), positions))
        roots = rows(control / f"{evidence_prefix}-roots-{nodes}.tsv")
        verify_root_coverage(roots, positions, identity, nodes * CELLS_PER_NODE)
        for node in range(nodes):
            assert sum(window["acknowledged_writes_by_node"][node] for window in windows if window["nodes"] == nodes) > 0
    resources = {}
    for node in range(stages[-1]):
        samples = rows(control / f"node-{node}-resources.tsv")
        assert len(samples) >= 2
        assert all(int(row["active_cells"]) == CELLS_PER_NODE for row in samples)
        assert all(int(row["unpublished_node_log_bytes"]) >= 0 for row in samples)
        for column in ("boot_ms", "cpu_usage_us", "throttled_us", "object_started", "object_finished", "bytes_read", "bytes_written"):
            values = [int(row[column]) for row in samples]
            assert values == sorted(values), f"node {node}: {column} regressed"
        assert {int(row["stage"]) for row in samples} >= {stage for stage in stages if node < stage}
        local, forwarded = map(int, (control / f"node-{node}.counts").read_text().split())
        assert local > 0 and forwarded > 0
        observations = rows(control / f"node-{node}-objects.tsv")
        assert len(observations) == 99 and len({(row["operation"], row["outcome"]) for row in observations}) == 99
        assert all(int(row["count"]) >= 0 for row in observations)
        assert any(row["operation"] == "put" and row["outcome"] == "success" and int(row["count"]) > 0 for row in observations)
        object_operations = verify_object_operations(control, node, observations)
        waits = [int(row["object_wait_us"]) for row in rows(control / f"node-{node}-durability.tsv")]
        assert waits and min(waits) >= 0
        resources[node] = dict(samples=len(samples), max_memory_current_bytes=max(int(row["memory_current_bytes"]) for row in samples),
                               max_disk_file_bytes=max(int(row["disk_bytes"]) for row in samples),
                               gateway_local=local, gateway_forwarded=forwarded, object_wait=distribution(waits),
                               logical_object_operations=sum(int(row["count"]) for row in observations))
        resources[node]["durability"] = verify_timing_evidence(control, node, windows)
        resources[node]["trace_counts"] = verify_trace_counts(control, node)
        for window in windows:
            if node >= window["nodes"]:
                continue
            observed = [row for row in samples if window["started_boot_ms"] <= int(row["boot_ms"]) <= window["ended_boot_ms"]]
            assert len(observed) >= 2, "missing in-window node samples"
            first, last = observed[0], observed[-1]
            window.setdefault("node_samples", {})[node] = dict(
                first_boot_ms=int(first["boot_ms"]), last_boot_ms=int(last["boot_ms"]),
                cpu_usage_us=int(last["cpu_usage_us"]) - int(first["cpu_usage_us"]),
                throttled_us=int(last["throttled_us"]) - int(first["throttled_us"]),
                logical_object_started=int(last["object_started"]) - int(first["object_started"]),
                logical_object_finished=int(last["object_finished"]) - int(first["object_finished"]),
                max_memory_current_bytes=max(int(row["memory_current_bytes"]) for row in observed),
                max_unpublished_node_log_bytes=max(int(row["unpublished_node_log_bytes"]) for row in observed),
                max_disk_file_bytes=max(int(row["disk_bytes"]) for row in observed),
                max_worker_jobs=max(int(row["worker_jobs"]) for row in observed))
            selected_objects = [row for row in object_operations
                                if window["started_ms"] <= int(row["at_ms"]) <= window["ended_ms"]]
            window.setdefault("node_store", {})[node] = dict(
                operations={operation: distribution([int(row["duration_us"]) for row in selected_objects
                                                     if row["operation"] == operation])
                            for operation in sorted({row["operation"] for row in selected_objects})},
                outcomes=dict(Counter(row["outcome"] for row in selected_objects)),
                requests_per_acknowledged_write=(len(selected_objects)
                                                 / max(1, window["acknowledged_writes_by_node"][node])),
                requests_per_completed_action=(len(selected_objects)
                                               / max(1, window["completed_actions_by_node"][node])),
                bytes_read=sum(int(row["bytes_read"]) for row in selected_objects),
                bytes_written=sum(int(row["bytes_written"]) for row in selected_objects))
    extra = {}
    if capacity:
        if follower:
            extra.update(verify_follower_proof(resources))
        for window in windows:
            root_lags = {}
            for entity, acknowledged in window["latest_write_sequence_by_entity"].items():
                cell = identity[entity][0]
                owner = entity // CELLS_PER_NODE
                published = window["node_durability"][owner]["latest_published_sequence_by_cell"].get(cell, 0)
                root_lags[entity] = max(0, acknowledged - published)
            window["root_lag_commits_by_entity"] = root_lags
            window["max_root_lag_commits"] = max(root_lags.values(), default=0)
        capacity_curves = {}
        for shape in SHAPES:
            selected = [window for window in windows if window["shape"] == shape]
            fully_served = selected[-2]
            overloaded = selected[-1]
            capacity_curves[shape] = dict(
                max_fully_served_rate_per_node=fully_served["rate_per_node"],
                max_fully_served_logical_writes_per_second=fully_served["completed_writes_per_second"],
                first_overloaded_rate_per_node=overloaded["rate_per_node"],
                first_overloaded_outcomes=overloaded["outcomes"],
                max_root_lag_commits_at_fully_served_rate=fully_served["max_root_lag_commits"],
                max_root_lag_commits_at_overload=overloaded["max_root_lag_commits"],
                published_roots_per_second=sum(
                    node["published_roots_per_second"] for node in fully_served["node_durability"].values()),
            )
        extra["capacity_curves"] = capacity_curves
    return dict(integrity_verified=True, windows=windows, resources=resources,
                verified_cells=len(positions), acknowledged_writes=sum(map(len, positions.values())),
                raw_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                            for path in sorted(control.glob("*.tsv"))}, **extra)
