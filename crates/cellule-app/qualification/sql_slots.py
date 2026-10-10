"""Audit process-local SQL slot holders and their monotonic boundaries."""

from bisect import bisect_right
from collections import Counter

from entities import iter_rows

KINDS = {"Query", "Command", "Migration", "Effect", "HydrationPrepare", "HydrationInstall",
         "Inventory", "Resolve", "Snapshot", "Control"}


def verify_slot(row, workers):
    id, previous, shard = (int(row[key]) for key in ("id", "previous_job", "shard"))
    requested, acquired, released = (int(row[key]) for key in ("requested_ns", "acquired_ns", "released_ns"))
    started = int(row["started_ns"]) if row["started_ns"] else None
    assert id > 0 and 0 <= previous < id and 0 <= shard < workers, "invalid SQL slot identity"
    assert row["kind"] in KINDS, "unbounded SQL job kind"
    assert 0 <= requested <= acquired <= released, "invalid SQL slot clock"
    assert int(row["admission_us"]) == (acquired - requested) // 1000
    assert int(row["held_us"]) == (released - acquired) // 1000
    if previous == 0:
        assert row["after_last_release_us"] == "", "invented previous slot release"
    else:
        assert 0 <= int(row["after_last_release_us"]) <= int(row["admission_us"])
    if started is None:
        assert row["handoff_us"] == row["native_us"] == "", "invented native execution"
    else:
        assert acquired <= started <= released, "invalid native start"
        assert int(row["handoff_us"]) == (started - acquired) // 1000
        assert int(row["native_us"]) == (released - started) // 1000
    if row["kind"] == "Snapshot":
        assert started is None, "snapshot native work was not observed"
    return dict(id=id, previous=previous, shard=shard, kind=row["kind"], requested=requested,
                acquired=acquired, started=started, released=released,
                admission_us=int(row["admission_us"]),
                after_last_release_us=int(row["after_last_release_us"]) if previous else None)


def read_slots(control, workers):
    slots = {}
    for row in iter_rows(control / "node-0-sql-slots.tsv"):
        slot = verify_slot(row, workers)
        assert slot["id"] not in slots, "duplicate SQL slot release"
        slots[slot["id"]] = slot
    # Release callbacks can interleave after permitting the next job. CSV row
    # order therefore grants no chronological or predecessor proof.
    assert set(slots) == set(range(1, len(slots) + 1)), "missing SQL slot release"
    latest = {}
    for id in sorted(slots):
        slot = slots[id]
        assert slot["previous"] == latest.get(slot["shard"], 0), "wrong SQL slot predecessor"
        latest[slot["shard"]] = id
        if slot["previous"]:
            previous = slots[slot["previous"]]
            assert previous["released"] <= slot["acquired"], "overlapping SQL slot ownership"
            expected = (slot["acquired"] - max(previous["released"], slot["requested"])) // 1000
            assert slot["after_last_release_us"] == expected, "changed post-release wait"
    return slots


def bind_query(row, slots, workers, seen):
    id = int(row["job_id"])
    if id == 0:
        assert row["succeeded"] == "false", "successful query lacks a SQL slot"
        return None
    assert id in slots and id not in seen, "missing or reused query SQL slot"
    seen.add(id)
    slot = slots[id]
    assert slot["kind"] == "Query", "query bound to wrong slot work"
    if row["succeeded"] == "true":
        assert slot["started"] is not None, "successful query lacks native execution"
    digest = row["cell"].removeprefix("CellId(").removesuffix(")")
    assert int(digest[:16], 16) % workers == slot["shard"], "query assigned to wrong SQL shard"
    assert slot["admission_us"] <= int(row["worker_admission_us"]), "slot admission exceeds query phase"
    return slot


def slot_timelines(slots, workers):
    lanes = [[] for _ in range(workers)]
    for id in sorted(slots):
        slot = slots[id]
        lanes[slot["shard"]].append(slot)
    return [(lane, [slot["released"] for slot in lane]) for lane in lanes]


def admission_breakdown(slot, timelines):
    """Partition a wait by recorded reservations, without claiming worker idleness.

    Native spans include thread descheduling. Gaps include permit-release and
    async admission work; exempt lifecycle messages are outside this trace.
    """
    begin, end = slot["requested"], slot["acquired"]
    lane, releases = timelines[slot["shard"]]
    held, native = Counter(), 0
    for index in range(bisect_right(releases, begin), len(lane)):
        holder = lane[index]
        if holder["acquired"] >= end:
            break
        left, right = max(begin, holder["acquired"]), min(end, holder["released"])
        held[holder["kind"]] += right - left
        if holder["started"] is not None:
            native += max(0, right - max(left, holder["started"]))
    reserved = sum(held.values())
    assert 0 <= native <= reserved <= end - begin, "slot wait partition overlaps"
    return dict(admission_ns=end - begin, reserved_ns=reserved,
                recorded_native_span_ns=native, reserved_other_span_ns=reserved - native,
                no_recorded_holder_ns=end - begin - reserved, holders_ns=dict(held))


def summarize_tail(tail, slots, timelines, phase_names, actor_names):
    totals, holders, phases, actors, states, cell_waits = (Counter() for _ in range(6))
    bound = 0
    for total, _, id, values, actor_values, state in tail:
        phases.update({key: value for key, value in zip(phase_names, values) if value is not None})
        phases["total_us"] += total
        actors.update({key: value for key, value in zip(actor_names, actor_values) if value is not None})
        states[state or "Unenqueued"] += 1
        if actor_values[1] is not None:
            cell_waits[state] += actor_values[1]
        if id == 0:
            continue
        bound += 1
        part = admission_breakdown(slots[id], timelines)
        holders.update(part.pop("holders_ns"))
        totals.update(part)
    return dict(selection="up to 3,000 queries with the largest runtime total in this window",
                queries=len(tail), bound_queries=bound, phases_us=dict(phases),
                actor_phases_us=dict(actors), enqueue_states=dict(states), cell_queue_by_enqueue_state_us=dict(cell_waits),
                admission_partition_ns=dict(totals), holder_kinds_ns=dict(holders),
                interpretation="No recorded holder does not prove native-worker idleness; exempt messages and admission bookkeeping are unobserved. Native spans include thread descheduling.")
