import unittest

from owner_reads import (ROW, counter_distribution, fully_served, verify_attempt,
                         verify_actor, verify_exports, verify_query, verify_seed, verify_window)


class OwnerReadEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.digest = bytes(range(32))
        self.seed = dict(minimum_sequence="2", digest=self.digest.hex())
        self.fields = [1, 1, 200, 212, 220, 250, 2, 2, 1]

    def test_receipt_bound_value_and_all_delays(self):
        self.assertEqual(verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000),
                         (50, 12, 30))

    def test_seed_accepts_hashed_key_and_binds_population_receipt(self):
        seed = dict(self.seed, entity="1", key="97331", payload_bytes="1024")
        cell = dict(entity="1", sequence="2")
        verify_seed(seed, cell, 1)
        for field, value in (("key", "100000"), ("payload_bytes", "4096"),
                             ("minimum_sequence", "3"), ("entity", "2")):
            with self.assertRaises(AssertionError):
                verify_seed(dict(seed, **{field: value}), cell, 1)

    def test_wrong_arrival_and_cell_are_rejected(self):
        for field in (0, 1):
            changed = self.fields.copy()
            changed[field] = 2
            with self.assertRaisesRegex(AssertionError, "substituted"):
                verify_attempt(changed, self.digest, self.seed, 1, 5000, 1000)

    def test_changed_schedule_is_rejected(self):
        self.fields[2] = 201
        with self.assertRaisesRegex(AssertionError, "schedule"):
            verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000)

    def test_early_and_reversed_and_out_of_window_timestamps_are_rejected(self):
        for field, value in ((3, 199), (4, 211), (5, 219), (5, 1001)):
            changed = self.fields.copy()
            changed[field] = value
            with self.assertRaisesRegex(AssertionError, "timestamps"):
                verify_attempt(changed, self.digest, self.seed, 1, 5000, 1000)

    def test_wrong_minimum_receipt_is_rejected(self):
        self.fields[6] = 1
        with self.assertRaisesRegex(AssertionError, "minimum"):
            verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000)

    def test_wrong_observed_receipt_and_value_are_rejected(self):
        self.fields[7] = 3
        with self.assertRaisesRegex(AssertionError, "receipt or value"):
            verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000)
        self.fields[7] = 2
        with self.assertRaisesRegex(AssertionError, "receipt or value"):
            verify_attempt(self.fields, bytes(32), self.seed, 1, 5000, 1000)

    def test_failed_read_cannot_present_success_evidence(self):
        self.fields[8] = 2
        with self.assertRaisesRegex(AssertionError, "failed read"):
            verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000)
        self.fields[7] = 0
        self.assertEqual(verify_attempt(self.fields, bytes(32), self.seed, 1, 5000, 1000),
                         (50, 12, 30))

    def test_client_full_cannot_claim_dispatch(self):
        self.fields[7:] = [0, 3]
        with self.assertRaisesRegex(AssertionError, "client-full"):
            verify_attempt(self.fields, bytes(32), self.seed, 1, 5000, 1000)
        self.fields[5] = self.fields[4]
        verify_attempt(self.fields, bytes(32), self.seed, 1, 5000, 1000)

    def test_missing_outcome_is_rejected(self):
        self.fields[8] = 0
        with self.assertRaisesRegex(AssertionError, "outcome"):
            verify_attempt(self.fields, self.digest, self.seed, 1, 5000, 1000)

    def test_original_gates_reject_failure_latency_generator_and_drain(self):
        success = {1: 300000, 2: 0, 3: 0}
        self.assertTrue(fully_served(success, 50000, 5000, 62000000))
        for outcomes, latency, generator, elapsed in (
            ({1: 299999, 2: 1, 3: 0}, 1, 1, 60000000),
            ({1: 299999, 2: 0, 3: 1}, 1, 1, 60000000),
            (success, 50001, 1, 60000000), (success, 1, 5001, 60000000),
            (success, 1, 1, 62000001),
        ):
            self.assertFalse(fully_served(outcomes, latency, generator, elapsed))
        self.assertFalse(fully_served(success, 1, 1, 60000000, export_failed=1))

    def test_export_counts_and_terminal_drain_cannot_lose_accepted_work(self):
        window = dict(window=1, elapsed_us=60000000, export_requests=59,
                      export_accepted=58, export_failed=1, export_completed=58)
        observations = [dict(window="1", ordinal=str(index), terminal="false", queue_us="12", flush_us="50")
                        for index in range(1, 59)]
        terminal = dict(window="1", ordinal="0", terminal="true", queue_us="0", flush_us="6")
        self.assertEqual(verify_exports(window, observations + [terminal])["failed"], 1)
        with self.assertRaisesRegex(AssertionError, "lost export"):
            verify_exports(window, observations[1:] + [terminal])
        with self.assertRaises(AssertionError):
            verify_exports(window, observations)
        with self.assertRaisesRegex(AssertionError, "accounting"):
            verify_exports(dict(window, export_completed=57), observations + [terminal])

    def test_query_phases_retain_absent_deadline_boundaries_and_partition_success(self):
        row = dict(actor_queue_us="1", worker_admission_us="2", worker_queue_us="3",
                   execution_us="4", reply_queue_us="5", total_us="18", succeeded="true", delivered="true")
        self.assertEqual(verify_query(row), [1, 2, 3, 4, 5, 18])
        deadline = dict(row, execution_us="", reply_queue_us="", succeeded="false")
        self.assertEqual(verify_query(deadline), [1, 2, 3, None, None, 18])
        for changed in (dict(deadline, succeeded="true"), dict(deadline, worker_admission_us=""),
                        dict(row, total_us="14"), dict(row, total_us="20")):
            with self.assertRaises(AssertionError):
                verify_query(changed)

    def test_streamed_histogram_quantiles_match_unrounded_phase_samples(self):
        from collections import Counter
        from entities import distribution
        values = [1] * 95 + [3, 7, 7, 11, 13]
        self.assertEqual(counter_distribution(Counter(values)), distribution(values))
        self.assertEqual(counter_distribution(Counter()), dict(count=0))

    def test_actor_subphases_partition_wait_and_retain_unreached_boundaries(self):
        row = dict(actor_queue_us="8", actor_ingress_us="1", cell_queue_us="4", task_start_us="2",
                   actor_state="Busy", total_us="12")
        self.assertEqual(verify_actor(row), ([1, 4, 2], "Busy"))
        queued = dict(row, actor_queue_us="", cell_queue_us="", task_start_us="", actor_state="Renewal")
        self.assertEqual(verify_actor(queued), ([1, None, None], "Renewal"))
        for changed in (dict(row, task_start_us=""), dict(row, actor_queue_us="10"),
                        dict(row, actor_state="arbitrary"), dict(row, actor_state=""),
                        dict(queued, task_start_us="1"), dict(queued, actor_ingress_us=""),
                        dict(queued, actor_ingress_us="13")):
            with self.assertRaises(AssertionError):
                verify_actor(changed)

    def test_lost_raw_read_is_rejected_before_audit(self):
        row = dict(window="1", phase="measure", rate="5000", seconds="60", concurrency="1024",
                   count="300000", elapsed_us="60000100", cpu_us="10000", fully_served="true")
        with self.assertRaisesRegex(AssertionError, "lost read"):
            verify_window(row, b"", [], [])

    def test_full_window_recomputes_success_tps_and_rejects_false_gate(self):
        row = dict(window="1", phase="measure", rate="5000", seconds="60", concurrency="1024",
                   count="300000", elapsed_us="60000100", cpu_us="500000", fully_served="true",
                   process_rss_bytes="1000000000", memory_current_bytes="1200000000",
                   memory_peak_bytes="1300000000", descriptors="16070", sqlite_descriptors="16000",
                   active_cells="2000", start_at_ms="100000", end_at_ms="160001", export_failed="0")
        seeds = [self.seed] * 2000
        raw = bytearray(300000 * ROW.size)
        for arrival in range(300000):
            scheduled = arrival * 200
            ROW.pack_into(raw, arrival * ROW.size, arrival, arrival % 2000, scheduled,
                          scheduled + 12, scheduled + 20, scheduled + 50, 2, 2, 1, self.digest)
        report = verify_window(row, raw, [], seeds)
        self.assertTrue(report["fully_served"])
        self.assertEqual(report["original_success_tps"], 5000)
        self.assertEqual(report["scheduled_latency"]["p99_ms"], 0.050)
        self.assertEqual(report["generator_delay"]["p99_ms"], 0.012)
        row["fully_served"] = "false"
        with self.assertRaisesRegex(AssertionError, "gate disagrees"):
            verify_window(row, raw, [], seeds)


if __name__ == "__main__":
    unittest.main()
