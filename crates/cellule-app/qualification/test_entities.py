"""Reject false capacity and durability claims in the entity workload receipt."""

import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from entities import destination, verify_capacity_windows, verify_follower_proof, verify_follower_roots, verify_object_operations, verify_root_coverage, verify_timing_evidence, verify_window


class EntityWindowEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.label = "entities-3-uniform-1"
        (self.root / f"{self.label}-window.tsv").write_text(
            "window_id\tnodes\tshape\trate_per_node\tconcurrency\tseconds\tstarted_ms\tended_ms\tstarted_boot_ms\tended_boot_ms\telapsed_us\n"
            "0\t3\tuniform\t1\t4\t10\t100000\t110000\t100000\t110000\t10000000\n")
        counts = [0] * 12
        lines = ["arrival\tscheduled_us\tstarted_us\telapsed_us\tentity\tkind\toutcome\tsequence\tread_sequence\tcount"]
        for arrival in range(30):
            entity = arrival % 12
            counts[entity] += 1
            sequence = counts[entity] * 2 + 1
            scheduled = arrival * 1_000_000 // 3
            lines.append(f"{arrival}\t{scheduled}\t{scheduled + 10}\t1000\t{entity}\twrite\tok\t{sequence}\t{sequence}\t{counts[entity]}")
        (self.root / f"{self.label}.tsv").write_text("\n".join(lines) + "\n")
        (self.root / f"{self.label}-readback.tsv").write_text(
            "entity\texpected\tactual\tsequence\n" + "".join(
                f"{entity}\t{count}\t{count}\t{count * 2 + 1}\n" for entity, count in enumerate(counts)))

    def change(self, suffix, update):
        path = self.root / f"{self.label}{suffix}.tsv"
        with path.open(newline="") as source:
            rows = list(csv.DictReader(source, delimiter="\t"))
        update(rows)
        with path.open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0], delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

    def verify(self):
        return verify_window(self.root, 3, "uniform", 1, 4, 0, {})

    def test_complete_window_has_independent_owner_work(self):
        result = self.verify()
        self.assertEqual((result["fully_served_arrivals"], result["acknowledged_writes_by_node"]), (True, [12, 10, 8]))

    def test_value_at_wrong_receipt_is_rejected(self):
        self.change("", lambda rows: rows[0].update(count="2"))
        with self.assertRaisesRegex(AssertionError, "read value disagrees"):
            self.verify()

    def test_wall_clock_adjustment_does_not_shorten_the_load_window(self):
        self.change("-window", lambda rows: rows[0].update(ended_ms="109898"))
        result = self.verify()
        self.assertEqual((result["fully_served_arrivals"], result["wall_clock_adjustment_ms"]), (True, -102))

    def test_missing_arrival_is_rejected(self):
        self.change("", lambda rows: rows.pop())
        with self.assertRaisesRegex(AssertionError, "missing or duplicate arrival"):
            self.verify()

    def test_missing_persisted_write_is_rejected(self):
        self.change("-readback", lambda rows: rows[0].update(actual="2", expected="2"))
        with self.assertRaisesRegex(AssertionError, "lost or duplicated write"):
            self.verify()

    def test_missed_arrival_is_not_fully_served(self):
        self.change("", lambda rows: rows[-1].update(started_us="10000000", elapsed_us="0",
                    outcome="scheduler_late", sequence="0", read_sequence="0", count="0"))
        self.change("-readback", lambda rows: rows[5].update(actual="2", expected="2"))
        result = self.verify()
        self.assertEqual((result["fully_served_arrivals"], result["completed_actions"]), (False, 29))


class EntityTimingEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "node-0-responses.tsv").write_text(
            "at_ms\tsource\tresponse_us\tconfirmation_us\n"
            "100001\tFleet\t1200\t900\n100002\tObject\t2000\t1800\n")
        (self.root / "node-0-executions.tsv").write_text(
            "at_ms\tqueue_wait_us\tworker_round_trip_us\tsucceeded\n"
            "100001\t100\t300\ttrue\n100002\t200\t400\ttrue\n")
        (self.root / "node-0-publications.tsv").write_text(
            "at_ms\tcell\tsequence\tqueue_wait_us\tpreparation_us\tauthority_us\ttotal_us\tsucceeded\n"
            "100003\tcell-1\t1\t100\t1000\t200\t1500\ttrue\n")
        (self.root / "node-0-phases.tsv").write_text(
            "at_ms\tphase\telapsed_us\tsucceeded\n100003\tCapture\t700\ttrue\n")
        (self.root / "node-0-captures.tsv").write_text(
            "at_ms\ttotal_us\tpreparation_us\tschema_check_us\twal_read_us\tpage_collection_us\tverification_us\tencode_us\tlocal_write_us\tfsync_us\tcheckpoint_us\twal_bytes\tltx_bytes\tsucceeded\n"
            "100003\t700\t100\t20\t100\t100\t50\t50\t50\t100\t50\t4096\t2048\ttrue\n")
        (self.root / "node-0-publication-costs.tsv").write_text(
            "at_ms\tobjects\tbytes\n100003\t2\t2048\n")
        (self.root / "node-0-follower-appends.tsv").write_text(
            "at_ms\tacknowledged\tbytes\n")
        (self.root / "node-0-follower-network.tsv").write_text(
            "at_ms\tacknowledged\tbytes\tduration_us\n")
        (self.root / "node-0-node-log-events.tsv").write_text(
            "at_ms\tepoch\tphase\tcovered_through\n")
        self.windows = [dict(nodes=3, started_ms=100000, ended_ms=110000, elapsed_us=10_000_000)]

    def test_response_winner_and_later_publication_are_separate(self):
        report = verify_timing_evidence(self.root, 0, self.windows)
        self.assertEqual(report["response_sources"], dict(Fleet=1, Object=1, Recorded=0))
        self.assertEqual(self.windows[0]["node_durability"][0]["published_roots_per_second"], 0.1)
        self.assertEqual(self.windows[0]["node_durability"][0]["uploaded_objects"], 2)
        self.assertEqual(self.windows[0]["node_durability"][0]["actor_queue"]["count"], 2)

    def test_incomplete_timing_evidence_is_rejected(self):
        (self.root / "node-0-publications.tsv").write_text(
            "at_ms\tcell\tsequence\tqueue_wait_us\tpreparation_us\tauthority_us\ttotal_us\tsucceeded\n")
        with self.assertRaisesRegex(AssertionError, "missing publication evidence"):
            verify_timing_evidence(self.root, 0, self.windows)

    def test_missing_execution_evidence_is_rejected(self):
        (self.root / "node-0-executions.tsv").write_text(
            "at_ms\tqueue_wait_us\tworker_round_trip_us\tsucceeded\n")
        with self.assertRaisesRegex(AssertionError, "missing command execution evidence"):
            verify_timing_evidence(self.root, 0, self.windows)

    def test_duplicate_publication_is_rejected(self):
        path = self.root / "node-0-publications.tsv"
        lines = path.read_text().splitlines()
        path.write_text("\n".join(lines + [lines[-1]]) + "\n")
        with self.assertRaisesRegex(AssertionError, "duplicate publication"):
            verify_timing_evidence(self.root, 0, self.windows)

    def test_active_marker_after_coverage_does_not_reset_covered_sequence(self):
        path = self.root / "node-0-node-log-events.tsv"
        path.write_text("at_ms\tepoch\tphase\tcovered_through\n"
                        "100001\t1\tenrolled\t0\n"
                        "100002\t1\tcoverage\t1\n"
                        "100003\t1\tactive\t0\n"
                        "100004\t1\tcoverage\t2\n"
                        "100005\t1\tclosed\t2\n")
        report = verify_timing_evidence(self.root, 0, self.windows)
        self.assertEqual(report["node_log_covered_through"], 2)
        self.assertEqual(self.windows[0]["node_durability"][0]["node_log_covered_through"], 2)

        path.write_text(path.read_text().replace("100004\t1\tcoverage\t2",
                                                 "100004\t1\tcoverage\t0"))
        with self.assertRaisesRegex(AssertionError, "node-log coverage regressed"):
            verify_timing_evidence(self.root, 0, self.windows)


class CapacityScheduleEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.schedule = self.root / "capacity-windows.tsv"
        self.write_schedule([
            (0, "uniform", 2, 8, "true"), (1, "uniform", 4, 16, "false"),
            (2, "hot", 2, 8, "true"), (3, "hot", 4, 16, "false"),
            (4, "skewed", 2, 8, "true"), (5, "skewed", 4, 16, "false"),
        ])

    def write_schedule(self, entries):
        self.schedule.write_text(
            "window_id\tshape\trate_per_node\tconcurrency\tfully_served\n" +
            "".join("\t".join(map(str, entry)) + "\n" for entry in entries))

    def write_complete_windows(self):
        counts = [0] * 12
        sequences = [1] * 12
        for window_id, shape, rate, concurrency, _ in [
            (0, "uniform", 2, 8, True), (1, "uniform", 4, 16, False),
            (2, "hot", 2, 8, True), (3, "hot", 4, 16, False),
            (4, "skewed", 2, 8, True), (5, "skewed", 4, 16, False),
        ]:
            label = f"capacity-3-{shape}-{rate}"
            started_ms = 100000 + window_id * 20000
            (self.root / f"{label}-window.tsv").write_text(
                "window_id\tnodes\tshape\trate_per_node\tconcurrency\tseconds\tstarted_ms\tended_ms\tstarted_boot_ms\tended_boot_ms\telapsed_us\n"
                f"{window_id}\t3\t{shape}\t{rate}\t{concurrency}\t10\t{started_ms}\t{started_ms + 10000}\t{started_ms}\t{started_ms + 10000}\t10000000\n")
            samples = ["arrival\tscheduled_us\tstarted_us\telapsed_us\tentity\tkind\toutcome\tsequence\tread_sequence\tcount"]
            planned = 30 * rate
            for arrival in range(planned):
                entity, kind = destination(shape, arrival, 12)
                scheduled = arrival * 1_000_000 // (3 * rate)
                if rate == 4 and arrival == planned - 1:
                    samples.append(f"{arrival}\t{scheduled}\t10000000\t0\t{entity}\t{kind}\tscheduler_late\t0\t0\t0")
                    continue
                sequence = 0
                if kind == "write":
                    counts[entity] += 1
                    sequences[entity] += 2
                    sequence = sequences[entity]
                samples.append(f"{arrival}\t{scheduled}\t{scheduled + 10}\t1000\t{entity}\t{kind}\tok\t{sequence}\t{sequences[entity]}\t{counts[entity]}")
            (self.root / f"{label}.tsv").write_text("\n".join(samples) + "\n")
            (self.root / f"{label}-readback.tsv").write_text(
                "entity\texpected\tactual\tsequence\n" +
                "".join(f"{entity}\t{counts[entity]}\t{counts[entity]}\t{sequences[entity]}\n"
                        for entity in range(12)))

    def test_complete_report_requires_all_shapes_and_overload(self):
        self.write_complete_windows()
        windows = verify_capacity_windows(self.root, {})
        self.assertEqual(len(windows), 6)
        self.assertEqual([window["fully_served_window"] for window in windows], [True, False] * 3)

    def test_completed_arrivals_with_slow_drain_are_not_supported(self):
        self.write_complete_windows()
        path = self.root / "capacity-3-uniform-2-window.tsv"
        path.write_text(path.read_text().replace("10000000\n", "12100000\n"))
        with self.assertRaisesRegex(AssertionError, "mislabeled fully served rate"):
            verify_capacity_windows(self.root, {})

    def test_incomplete_report_is_rejected(self):
        self.write_schedule([(0, "uniform", 1, 4, "true")])
        with self.assertRaisesRegex(AssertionError, "incomplete rate ramp"):
            verify_capacity_windows(self.root, {})

    def test_rate_mislabeled_as_fully_served_is_rejected(self):
        self.write_schedule([
            (0, "uniform", 2, 8, "true"), (1, "uniform", 4, 16, "false"),
            (2, "hot", 2, 8, "true"), (3, "hot", 4, 16, "false"),
            (4, "skewed", 2, 8, "true"), (5, "skewed", 4, 16, "false"),
        ])
        with patch("entities.verify_window", return_value=dict(fully_served_arrivals=False)):
            with self.assertRaisesRegex(AssertionError, "mislabeled fully served rate"):
                verify_capacity_windows(self.root, {})


class FollowerRootDrainEvidence(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.identity = {0: ("cell", "0", "1", "incarnation")}
        self.positions = {0: [1, 2]}
        self.before = [dict(entity="0", cell="cell", owner="0", epoch="1",
                            incarnation="incarnation", root_sequence="1", root_digest="Digest(" + "a" * 64 + ")",
                            root_txid="1", root_checksum="101", restored_digest="Digest(" + "c" * 64 + ")")]
        self.final = dict(entity="0", cell="cell", owner="0", epoch="1",
                          incarnation="incarnation", root_sequence="2", root_digest="Digest(" + "b" * 64 + ")",
                          root_txid="2", root_checksum="202", restored_digest="Digest(" + "d" * 64 + ")",
                          state="Idle", owner_present="false", restored_sequence="2", restored_count="2")
        (self.root / "stop").touch()
        (self.root / "node-0.done").touch()
        self.write_final()

    def write_final(self):
        with (self.root / "capacity-final-roots.tsv").open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=self.final, delimiter="\t")
            writer.writeheader()
            writer.writerow(self.final)

    def verify(self):
        return verify_follower_roots(self.root, self.before, self.positions, self.identity, 1)

    def test_follower_acknowledgements_are_covered_after_shutdown_drain(self):
        result = self.verify()
        self.assertEqual(result["verified_cells"], 1)
        self.assertEqual(result["pre_drain_root_lag_commits_by_entity"], {0: 1})

    def test_compacted_manifest_at_same_endpoint_requires_identical_restored_bytes(self):
        self.before[0].update(root_sequence="2", root_txid="2", root_checksum="202",
                              restored_digest=self.final["restored_digest"])
        result = self.verify()
        self.assertEqual(result["verified_cells"], 1)

    def test_publication_logs_cannot_replace_fresh_authority_roots(self):
        (self.root / "capacity-final-roots.tsv").unlink()
        with self.assertRaises(FileNotFoundError):
            self.verify()

    def test_missing_shutdown_or_owner_drain_is_rejected(self):
        for name, message in [("stop", "missing shutdown request"),
                              ("node-0.done", "missing completed owner drain")]:
            with self.subTest(name=name):
                (self.root / name).unlink()
                with self.assertRaisesRegex(AssertionError, message):
                    self.verify()
                (self.root / name).touch()

    def test_final_root_still_requires_every_acknowledged_sequence(self):
        self.final.update(root_sequence="1", restored_sequence="1", restored_count="1")
        self.write_final()
        with self.assertRaisesRegex(AssertionError, "published root does not cover writes"):
            self.verify()

    def test_changed_cell_owner_epoch_or_incarnation_is_rejected(self):
        for field in ("cell", "owner", "epoch", "incarnation"):
            for row in (self.before[0], self.final):
                with self.subTest(field=field, final=row is self.final):
                    original = row[field]
                    row[field] = "different"
                    self.write_final()
                    with self.assertRaises(AssertionError):
                        self.verify()
                    row[field] = original
                    self.write_final()

    def test_serving_owner_or_incomplete_restore_is_rejected(self):
        for field, value, message in [
            ("state", "Serving", "still has an owner"),
            ("owner_present", "true", "still has an owner"),
            ("restored_sequence", "1", "restored metadata disagrees"),
            ("restored_count", "1", "lost or duplicated write"),
            ("restored_count", "3", "lost or duplicated write"),
        ]:
            with self.subTest(field=field, value=value):
                original = self.final[field]
                self.final[field] = value
                self.write_final()
                with self.assertRaisesRegex(AssertionError, message):
                    self.verify()
                self.final[field] = original
                self.write_final()

    def test_root_regression_or_changed_bytes_at_same_sequence_is_rejected(self):
        self.before[0]["root_sequence"] = "3"
        with self.assertRaisesRegex(AssertionError, "drained root regressed"):
            self.verify()
        self.before[0].update(root_sequence="2", root_txid="2", root_checksum="202")
        with self.assertRaisesRegex(AssertionError, "same sequence changed restored database"):
            self.verify()

    def test_same_sequence_cannot_change_transaction_or_checksum(self):
        self.before[0].update(root_sequence="2", root_txid="2", root_checksum="202",
                              restored_digest=self.final["restored_digest"])
        for field, value in (("root_txid", "3"), ("root_checksum", "203")):
            with self.subTest(field=field):
                original = self.final[field]
                self.final[field] = value
                self.write_final()
                with self.assertRaisesRegex(AssertionError, "same sequence changed root position"):
                    self.verify()
                self.final[field] = original
                self.write_final()

    def test_root_transaction_cannot_regress_or_remain_at_an_advanced_sequence(self):
        for value, message in (("0", "invalid root position"),
                               ("1", "advanced sequence did not advance transaction")):
            with self.subTest(value=value):
                self.final["root_txid"] = value
                self.write_final()
                with self.assertRaisesRegex(AssertionError, message):
                    self.verify()
        self.before[0]["root_txid"] = "3"
        self.final["root_txid"] = "2"
        self.write_final()
        with self.assertRaisesRegex(AssertionError, "drained root transaction regressed"):
            self.verify()

    def test_positions_and_restored_digests_are_required_in_both_snapshots(self):
        for field in ("root_txid", "root_checksum", "restored_digest"):
            for row in (self.before[0], self.final):
                with self.subTest(field=field, final=row is self.final):
                    original = row.pop(field)
                    self.write_final()
                    with self.assertRaises(KeyError):
                        self.verify()
                    row[field] = original
                    self.write_final()

    def test_invalid_positions_or_restored_digests_are_rejected(self):
        for field, value, message in (("root_txid", "-1", "invalid root position"),
                                      ("root_txid", str(2**64), "invalid root position"),
                                      ("root_checksum", "-1", "invalid root position"),
                                      ("root_checksum", str(2**64), "invalid root position"),
                                      ("restored_digest", "Digest(" + "g" * 64 + ")", "invalid restored database digest")):
            for row in (self.before[0], self.final):
                with self.subTest(field=field, value=value, final=row is self.final):
                    original = row[field]
                    row[field] = value
                    self.write_final()
                    with self.assertRaisesRegex(AssertionError, message):
                        self.verify()
                    row[field] = original
                    self.write_final()

    def test_missing_or_duplicate_final_cell_is_rejected(self):
        path = self.root / "capacity-final-roots.tsv"
        lines = path.read_text().splitlines()
        for content in (lines[:1], lines + [lines[-1]]):
            with self.subTest(content=content):
                path.write_text("\n".join(content) + "\n")
                with self.assertRaises(AssertionError):
                    self.verify()

    def test_malformed_digest_is_rejected_in_both_snapshots(self):
        for value in ("a" * 64, "Digest(" + "a" * 63 + ")", "Digest(" + "g" * 64 + ")"):
            for row in (self.before[0], self.final):
                with self.subTest(value=value, final=row is self.final):
                    original = row["root_digest"]
                    row["root_digest"] = value
                    self.write_final()
                    with self.assertRaisesRegex(AssertionError, "invalid root digest"):
                        self.verify()
                    row["root_digest"] = original
                    self.write_final()


class ObjectOperationEvidence(unittest.TestCase):
    def test_missing_provider_operation_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "node-0-object-operations.tsv"
            path.write_text("at_ms\toperation\toutcome\tduration_us\tbytes_read\tbytes_written\n"
                            "100000\tput\tsuccess\t500\t0\t4096\n")
            expected = [dict(operation="put", outcome="success", count="2")]
            with self.assertRaisesRegex(AssertionError, "samples disagree"):
                verify_object_operations(Path(root), 0, expected)
            expected[0]["count"] = "1"
            self.assertEqual(len(verify_object_operations(Path(root), 0, expected)), 1)


class FollowerProofEvidence(unittest.TestCase):
    def test_follower_lane_requires_proof_and_acknowledged_append(self):
        resources = {0: dict(durability=dict(response_sources=dict(Fleet=0),
                                            acknowledged_follower_appends=1,
                                            acknowledged_network_appends=1,
                                            node_log_phases=dict(enrolled=1, active=1, closed=1),
                                            node_log_epochs=[1]))}
        with self.assertRaisesRegex(AssertionError, "no follower-proof responses"):
            verify_follower_proof(resources)
        resources[0]["durability"]["response_sources"]["Fleet"] = 1
        resources[0]["durability"]["acknowledged_follower_appends"] = 0
        with self.assertRaisesRegex(AssertionError, "missing acknowledged follower append"):
            verify_follower_proof(resources)
        resources[0]["durability"]["acknowledged_follower_appends"] = 2
        resources[0]["durability"]["acknowledged_network_appends"] = 0
        with self.assertRaisesRegex(AssertionError, "missing network follower append"):
            verify_follower_proof(resources)
        resources[0]["durability"]["acknowledged_network_appends"] = 2
        self.assertEqual(verify_follower_proof(resources),
                         dict(follower_proof_responses=1, follower_appends=2,
                              network_follower_appends=2))
        resources[0]["durability"]["node_log_phases"]["active"] = 0
        with self.assertRaisesRegex(AssertionError, "did not enroll, activate, and close"):
            verify_follower_proof(resources)

    def test_root_drain_requires_every_acknowledged_sequence(self):
        identity = {0: ("cell", "0", "1", "incarnation")}
        positions = {0: [1, 2]}
        roots = [dict(entity="0", cell="cell", owner="0", epoch="1",
                      incarnation="incarnation", root_sequence="1")]
        with self.assertRaisesRegex(AssertionError, "published root does not cover writes"):
            verify_root_coverage(roots, positions, identity, 1)
        roots[0]["root_sequence"] = "2"
        verify_root_coverage(roots, positions, identity, 1)


if __name__ == "__main__":
    unittest.main()
