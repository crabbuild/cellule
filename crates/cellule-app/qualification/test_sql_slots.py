from pathlib import Path
import tempfile
import unittest

from sql_slots import admission_breakdown, bind_query, read_slots, slot_timelines, summarize_tail, verify_slot


class SqlSlotEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.row = dict(id="1", previous_job="0", shard="0", kind="Query",
                        admission_us="2", after_last_release_us="", handoff_us="1", native_us="3", held_us="4",
                        requested_ns="1000", acquired_ns="3000", started_ns="4000", released_ns="7000")

    def test_native_boundaries_and_missing_snapshot_phases(self):
        self.assertEqual(verify_slot(self.row, 8)["started"], 4000)
        snapshot = dict(self.row, kind="Snapshot", started_ns="", handoff_us="", native_us="")
        self.assertIsNone(verify_slot(snapshot, 8)["started"])
        with self.assertRaisesRegex(AssertionError, "not observed"):
            verify_slot(dict(self.row, kind="Snapshot"), 8)

    def test_forged_identity_clock_and_durations_are_rejected(self):
        for changed in (dict(self.row, id="0"), dict(self.row, previous_job="1"), dict(self.row, shard="8"),
                        dict(self.row, kind="arbitrary"), dict(self.row, acquired_ns="999"),
                        dict(self.row, admission_us="3"), dict(self.row, started_ns="2999"),
                        dict(self.row, handoff_us="2"), dict(self.row, held_us="5"),
                        dict(self.row, after_last_release_us="0")):
            with self.assertRaises(AssertionError):
                verify_slot(changed, 8)

    def test_out_of_order_callbacks_keep_exact_predecessor_proof(self):
        fields = list(self.row)
        next_row = dict(self.row, id="2", previous_job="1", requested_ns="4000", acquired_ns="9000",
                        started_ns="10000", released_ns="13000", admission_us="5", after_last_release_us="2")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "node-0-sql-slots.tsv"
            def write(values):
                path.write_text("\t".join(fields) + "\n" + "".join("\t".join(row[key] for key in fields) + "\n" for row in values))
            write([next_row, self.row])
            self.assertEqual(len(read_slots(Path(directory), 8)), 2)
            for invalid in (dict(next_row, previous_job="0", after_last_release_us=""),
                            dict(next_row, after_last_release_us="3")):
                write([self.row, invalid])
                with self.assertRaises(AssertionError):
                    read_slots(Path(directory), 8)
            write([next_row])
            with self.assertRaisesRegex(AssertionError, "missing"):
                read_slots(Path(directory), 8)

    def test_query_must_bind_one_matching_native_slot(self):
        slots = {1: verify_slot(self.row, 8)}
        query = dict(job_id="1", succeeded="true", cell="CellId(" + "0" * 64 + ")", worker_admission_us="3")
        seen = set()
        self.assertEqual(bind_query(query, slots, 8, seen)["id"], 1)
        with self.assertRaises(AssertionError):
            bind_query(query, slots, 8, seen)
        for changed in (dict(query, job_id="0"), dict(query, job_id="2"),
                        dict(query, cell="CellId(0000000000000001" + "0" * 48 + ")"),
                        dict(query, worker_admission_us="1")):
            with self.assertRaises(AssertionError):
                bind_query(changed, slots, 8, set())
        unsent = {1: dict(slots[1], started=None)}
        with self.assertRaisesRegex(AssertionError, "native execution"):
            bind_query(query, unsent, 8, set())
        self.assertEqual(bind_query(dict(query, succeeded="false"), unsent, 8, set())["id"], 1)
        self.assertIsNone(bind_query(dict(query, job_id="0", succeeded="false"), slots, 8, set()))

    def test_wait_crosses_multiple_holders_and_gaps_without_inventing_native_work(self):
        slots = {
            1: dict(id=1, shard=0, kind="Query", requested=0, acquired=1000, started=2000, released=4000),
            2: dict(id=2, shard=0, kind="Inventory", requested=4000, acquired=7000, started=8000, released=10000),
            3: dict(id=3, shard=0, kind="Query", requested=3000, acquired=14000, started=15000, released=17000),
            4: dict(id=4, shard=1, kind="Snapshot", requested=1000, acquired=2000, started=None, released=19000),
        }
        timelines = slot_timelines(slots, 2)
        part = admission_breakdown(slots[3], timelines)
        self.assertEqual(part, dict(admission_ns=11000, reserved_ns=4000, recorded_native_span_ns=3000,
                                    reserved_other_span_ns=1000, no_recorded_holder_ns=7000,
                                    holders_ns=dict(Query=1000, Inventory=3000)))
        snapshot_wait = dict(shard=1, requested=3000, acquired=20000)
        snapshot = admission_breakdown(snapshot_wait, timelines)
        self.assertEqual(snapshot["reserved_ns"], 16000)
        self.assertEqual(snapshot["recorded_native_span_ns"], 0)
        self.assertEqual(snapshot["no_recorded_holder_ns"], 1000)
        empty = admission_breakdown(dict(shard=0, requested=17000, acquired=17000), timelines)
        self.assertEqual(empty["admission_ns"], 0)
        report = summarize_tail([(20, 1, 3, [2, 11, 1, 2, 4], [0, 1, 1], "Busy"),
                                 (7, 2, 0, [7, None, None, None, None], [1, 2, 4], "Renewal")],
                                slots, timelines, ("actor", "admission", "queue", "native", "reply"),
                                ("ingress", "cell", "task"))
        self.assertEqual(report["bound_queries"], 1)
        self.assertEqual(report["phases_us"], dict(actor=9, admission=11, queue=1, native=2, reply=4, total_us=27))
        self.assertEqual(report["admission_partition_ns"]["no_recorded_holder_ns"], 7000)
        self.assertEqual(report["actor_phases_us"], dict(ingress=1, cell=3, task=5))
        self.assertEqual(report["cell_queue_by_enqueue_state_us"], dict(Busy=1, Renewal=2))


if __name__ == "__main__":
    unittest.main()
