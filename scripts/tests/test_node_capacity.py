import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "bench-node-capacity.py"
SPEC = importlib.util.spec_from_file_location("node_capacity", SCRIPT)
CAPACITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAPACITY)


class ColdAuditTests(unittest.TestCase):
    def test_duplicate_that_returns_a_new_receipt_is_rejected_even_with_correct_output(self):
        envelope = dict(id=1, total_cents=112, request_id="retained-id")
        receipt = dict(cell="a" * 64, incarnation="b" * 32, commit_sequence=7)
        output = dict(id=1, total_cents=112)
        record = dict(error=None, request=envelope, response=dict(output=output, receipt=receipt))
        bad = dict(status=201, body=dict(output=output, receipt=dict(receipt, commit_sequence=8)))
        with patch.object(CAPACITY, "records", return_value=iter([record])), patch.object(CAPACITY.bench, "request", return_value=bad):
            with self.assertRaisesRegex(RuntimeError, "cold identity replay changed its receipt"):
                CAPACITY.audit(SimpleNamespace(address="127.0.0.1:1"), Path("unused"), 1)

    def test_exact_duplicate_and_cold_read_keep_the_original_scoped_receipt(self):
        receipt = dict(cell="a" * 64, incarnation="b" * 32, commit_sequence=7)
        output = dict(id=1, total_cents=112)
        record = dict(error=None, request=dict(output, request_id="retained-id"), response=dict(output=output, receipt=receipt))
        replies = [dict(status=201, body=record["response"]), dict(status=200, body=record["response"])]
        with patch.object(CAPACITY, "records", return_value=iter([record])), patch.object(CAPACITY.bench, "request", side_effect=replies):
            self.assertEqual(CAPACITY.audit(SimpleNamespace(address="127.0.0.1:1"), Path("unused"), 1), dict(writes=1, reads=0))


class FollowerEvidenceTests(unittest.TestCase):
    def metrics(self):
        return dict(response_sources=dict(fleet=10, object=2, recorded=0),
                    submission_sources=dict(fleet=12, unsupported=0, unavailable=0, rejected=0),
                    node_log_append=dict(successes=4, failures=0))

    def test_object_winner_is_allowed_with_real_follower_proofs(self):
        CAPACITY.fleet.verify_durability(self.metrics(), CAPACITY.bench)

    def test_only_object_responses_or_failed_follower_paths_are_rejected(self):
        for section, key, value in [("response_sources", "fleet", 0),
                                    ("submission_sources", "unavailable", 1),
                                    ("submission_sources", "unsupported", 1),
                                    ("submission_sources", "rejected", 1),
                                    ("node_log_append", "failures", 1)]:
            with self.subTest(section=section, key=key):
                metrics = self.metrics()
                metrics[section][key] = value
                with self.assertRaises(RuntimeError):
                    CAPACITY.fleet.verify_durability(metrics, CAPACITY.bench)


if __name__ == "__main__":
    unittest.main()
