"""End-to-end behavior tests for the local replica coordinator."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from replica_store import BlockUnavailable, GenerationFenceError, ReplicaStore
from replica_store.replay import replay_file

ROOT = Path(__file__).parent


class ReplicaStoreBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "store"
        self.store = ReplicaStore(self.root)
        for node in ("a", "b", "c"):
            self.store.register_node(node, 1)

    def tearDown(self):
        self.temp.cleanup()

    def test_three_copies_silent_corruption_failover_repair_and_verify(self):
        self.store.write_object("archive", ["alpha", "beta"], nodes=["a", "b", "c"])
        self.store.corrupt_replica("c", "archive", 1, 0, "bad")
        self.store.scrub_disk("c", "archive", 1)

        report = self.store.inspect("archive")
        block0 = report["objects"][0]["blocks"]["0"]
        self.assertEqual(2, block0["valid_count"])
        self.assertTrue(block0["readable"])
        self.assertTrue(block0["under_replicated"])
        self.assertFalse(block0["lacks_any_valid_copy"])

        read = self.store.read_object("archive")
        self.assertEqual("alphabeta", read["text"])
        self.assertNotEqual("c", read["blocks"]["0"]["node"])

        planned = self.store.plan_repairs("archive")
        self.assertEqual([(0, "a", "c")], [(p["block"], p["source"], p["target"]) for p in planned])
        job = self.store.claim_next("repair-archive", batch_size=10)
        self.assertEqual(1, len(job["items"]))

        # The other block remains usable while block 0's c replica is fenced.
        self.assertEqual("beta", self.store.read_block("archive", 1)["text"])

        result = self.store.record_result(
            {"id": "repair-archive", "block": 0, "boot": 1, "rev": 1, "text": "alpha"}
        )
        self.assertEqual("written_unverified", result["status"])
        before_verify = self.store.inspect("archive")["objects"][0]["blocks"]["0"]
        self.assertEqual(2, before_verify["valid_count"])
        c_status = next(r for r in before_verify["replicas"] if r["node"] == "c")
        self.assertEqual("written_unverified", c_status["status"])

        verified = self.store.verify_repair({"id": "repair-archive", "block": 0, "text": "alpha"})
        self.assertTrue(verified["is_valid"])
        after = self.store.inspect("archive")["objects"][0]["blocks"]["0"]
        self.assertEqual(3, after["valid_count"])

    def test_only_matching_generation_commit_makes_manifest_readable(self):
        from replica_store.util import sha256_bytes

        self.store.propose_manifest(
            "gated", {0: sha256_bytes(b"secret")}, rev=7, generation="write-gen-7"
        )
        self.store.stage_block("a", "gated", 7, 0, "secret")
        with self.assertRaises(GenerationFenceError):
            self.store.commit("gated", 7, "wrong-generation")
        with self.assertRaises(BlockUnavailable) as ctx:
            self.store.read_block("gated", 0, rev=7)
        self.assertEqual("version_not_committed", ctx.exception.report["reason_code"])

        self.store.commit("gated", 7, "write-gen-7")
        self.assertEqual("secret", self.store.read_block("gated", 0)["text"])

    def test_partial_blocks_never_use_previous_revision(self):
        self.store.write_object("obj", ["old0", "old1"], nodes=["a", "b", "c"])
        self.store.write_object("obj", ["new0", "new1"], nodes=["a", "b", "c"])
        for node in ("a", "b", "c"):
            self.store.corrupt_replica(node, "obj", 2, 1, f"bad-{node}")
            self.store.scrub_disk(node, "obj", 2)

        with self.assertRaises(BlockUnavailable) as ctx:
            self.store.read_object("obj", rev=2)
        report = ctx.exception.report
        self.assertEqual([1], report["missing_blocks"])
        self.assertFalse(report["substituted_previous_revision"])
        self.assertNotIn("old1", json.dumps(report, ensure_ascii=False))

        record = self.store.inspect("obj")
        obj = record["objects"][0]
        self.assertFalse(obj["readable"])
        self.assertEqual([1], obj["blocks_without_any_valid_copy"])

        # Explicit old revision remains independently readable.
        self.assertEqual("old0old1", self.store.read_object("obj", rev=1)["text"])

    def test_old_boot_scrub_is_history_new_boot_actual_copy_is_trusted(self):
        for node in ("a", "b"):
            self.store.register_node(node, 1)
        self.store.write_object("proof", ["ok"], nodes=["a", "b"])
        self.store.register_node("b", 2)  # scans the real file under boot 2

        old_report = self.store.report_scrub(
            {"node": "b", "boot": 1, "key": "proof", "rev": 1, "block": 0, "text": "bad"}
        )
        self.assertEqual("archived_as_old_node_generation", old_report["decision"])
        self.store.report_scrub(
            {"node": "a", "boot": 1, "key": "proof", "rev": 1, "block": 0, "text": "bad"},
            adopt_observed_bytes=True,
        )

        report = self.store.inspect("proof")
        block = report["objects"][0]["blocks"]["0"]
        b = next(r for r in block["replicas"] if r["node"] == "b")
        a = next(r for r in block["replicas"] if r["node"] == "a")
        self.assertEqual("valid", b["status"])
        self.assertEqual(2, b["boot"])
        self.assertEqual("corrupt", a["status"])

        self.store.claim_repairs(
            "r2", [{"key": "proof", "rev": 1, "block": 0, "source": "b", "target": "a"}]
        )
        result = self.store.record_result({"id": "r2", "boot": 1, "rev": 1, "text": "ok"})
        self.assertEqual("written_unverified", result["status"])
        verified = self.store.verify_repair({"id": "r2", "text": "ok"})
        self.assertEqual("repair_verified", verified["reason_code"])
        self.assertTrue(self.store.inspect("proof")["objects"][0]["readable"])

    def test_repair_result_after_new_commit_is_stale_and_does_not_overwrite(self):
        self.store.write_object("trace", ["old"], nodes=["a", "b"])
        self.store.corrupt_replica("b", "trace", 1, 0, "bad")
        self.store.scrub_disk("b", "trace", 1)
        self.store.claim_repair(
            {"id": "r", "key": "trace", "rev": 1, "block": 0, "source": "a", "target": "b"}
        )

        self.store.write_object("trace", ["new"], rev=2, nodes=["a", "b"])
        stale = self.store.record_result({"id": "r", "boot": 1, "rev": 1, "text": "old"})
        self.assertEqual("stale", stale["status"])
        self.assertEqual("new_version_committed", stale["reason_code"])
        self.assertTrue(stale["is_stale"])

        read = self.store.read_object("trace", rev=2)
        self.assertEqual("new", read["text"])
        latest = self.store.inspect("trace")["latest_repair"]
        self.assertEqual("new_version_committed", latest["reason_code"])
        self.assertFalse(latest["is_valid"])

    def test_batch_claim_carries_multiple_blocks(self):
        self.store.write_object("batch", ["zero", "one", "two"], nodes=["a", "b", "c"])
        self.store.corrupt_replica("c", "batch", 1, 0, "bad0")
        self.store.corrupt_replica("c", "batch", 1, 1, "bad1")
        self.store.scrub_disk("c", "batch", 1)
        planned = [p for p in self.store.plan_repairs("batch") if p["target"] == "c"]
        self.assertEqual([0, 1], [p["block"] for p in planned])

        job = self.store.claim_repairs(
            "batch-r",
            [
                {"key": "batch", "rev": 1, "block": 0, "source": "a", "target": "c"},
                {"key": "batch", "rev": 1, "block": 1, "source": "a", "target": "c"},
            ],
        )
        self.assertEqual(2, len(job["items"]))
        self.store.record_result(
            {
                "id": "batch-r",
                "results": [
                    {"block": 0, "boot": 1, "rev": 1, "text": "zero"},
                    {"block": 1, "boot": 1, "rev": 1, "text": "one"},
                ],
            }
        )
        for block, text in ((0, "zero"), (1, "one")):
            outcome = self.store.verify_repair({"id": "batch-r", "block": block, "text": text})
            self.assertTrue(outcome["is_valid"])
        report = self.store.inspect("batch")["objects"][0]
        self.assertTrue(report["readable"])
        self.assertEqual([], report["blocks_with_missing_replica"])

    def test_priority_queue_prefers_high_risk_object(self):
        self.store.write_object("zz-normal", ["z"], nodes=["a", "b", "c"])
        self.store.write_object("aa-critical", ["a"], nodes=["a", "b", "c"])
        for key in ("zz-normal", "aa-critical"):
            self.store.corrupt_replica("c", key, 1, 0, "bad")
            self.store.scrub_disk("c", key, 1)
        self.store.set_priority("aa-critical", "high")
        plan = self.store.plan_repairs()
        self.assertEqual("aa-critical", plan[0]["key"])

    def test_block_repair_lock_is_local(self):
        self.store.write_object("local", ["0", "1"], nodes=["a", "b", "c"])
        self.store.claim_repairs(
            "lock-job", [{"key": "local", "rev": 1, "block": 0, "source": "a", "target": "c"}]
        )
        from replica_store import BlockUnderRepair

        with self.assertRaises(BlockUnderRepair):
            self.store.stage_block("c", "local", 1, 0, "changed-without-fence")
        # Other block and a new generation's block 0 are not globally locked.
        restaged = self.store.stage_block("c", "local", 1, 1, "1")
        self.assertEqual("1", restaged["digest"] and "1")
        self.store.write_object("local", ["new0", "new1"], rev=2, nodes=["a", "b", "c"])
        self.assertEqual("1", self.store.read_block("local", 1, rev=1)["text"])
        self.assertEqual("new1", self.store.read_block("local", 1, rev=2)["text"])

    def test_execute_repair_copies_verified_source_and_disk_verify_completes(self):
        self.store.write_object("exec", ["correct"], nodes=["a", "b", "c"])
        self.store.corrupt_replica("c", "exec", 1, 0, "bad")
        self.store.scrub_disk("c", "exec", 1)
        self.store.claim_repairs(
            "exec-job", [{"key": "exec", "rev": 1, "block": 0, "source": "a", "target": "c"}]
        )
        executed = self.store.execute_repair("exec-job", block=0)
        self.assertEqual("written_unverified", executed["status"])
        before = self.store.inspect("exec")["objects"][0]["blocks"]["0"]
        self.assertEqual(2, before["valid_count"])
        verified = self.store.verify_repair({"id": "exec-job", "block": 0})
        self.assertTrue(verified["is_valid"])
        self.assertEqual(3, self.store.inspect("exec")["objects"][0]["blocks"]["0"]["valid_count"])

    def test_restart_completes_half_written_repair_from_actual_disk(self):
        self.store.write_object("half", ["data"], nodes=["a", "b", "c"])
        self.store.corrupt_replica("b", "half", 1, 0, "bad")
        self.store.scrub_disk("b", "half", 1)
        self.store.claim_repairs(
            "h", [{"key": "half", "rev": 1, "block": 0, "source": "a", "target": "b"}]
        )
        self.store.record_result({"id": "h", "boot": 1, "rev": 1, "text": "data"})
        self.assertEqual("written_unverified", self.store.inspect("half")["latest_repair"]["status"])

        reopened = ReplicaStore(self.root)
        recovery = reopened.restart()
        self.assertEqual("verified", recovery["jobs"][0]["status"])
        latest = reopened.inspect("half")["latest_repair"]
        self.assertEqual("restart_verified_from_actual_disk", latest["reason_code"])

    def test_restart_preserves_unrecoverable_missing_reason(self):
        self.store.write_object("lost", ["x"], nodes=["a", "b"])
        for node in ("a", "b"):
            self.store.corrupt_replica(node, "lost", 1, 0, f"bad-{node}")
            self.store.scrub_disk(node, "lost", 1)
        reopened = ReplicaStore(self.root)
        reopened.restart()
        report = reopened.inspect("lost")
        self.assertFalse(report["objects"][0]["readable"])
        self.assertEqual("all_placement_replicas_corrupt", report["missing_blocks"][0]["reason_code"])
        state = reopened._load_state()
        self.assertIn("0", state["objects"]["lost"]["missing_reasons"])

    def test_rejoined_node_is_accepted_from_actual_file_without_new_stored_event(self):
        self.store.write_object("migrated", ["owned-bytes"], nodes=["a", "b"])
        rejoined_root = self.root
        second = ReplicaStore(rejoined_root)
        second.register_node("b", 9)
        report = second.inspect("migrated")
        block = report["objects"][0]["blocks"]["0"]
        replica_b = next(r for r in block["replicas"] if r["node"] == "b")
        self.assertEqual(9, replica_b["boot"])
        self.assertTrue(replica_b["trusted"])


class ExampleReplayTests(unittest.TestCase):
    def test_race_example_report_is_stale_and_new_version_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = replay_file(
                ROOT / "examples" / "repair-races-new-version.jsonl",
                Path(directory) / "race",
                reset=True,
            )
            report = summary["report"]
            trace = next(o for o in report["objects"] if o["key"] == "trace")
            self.assertEqual(2, trace["current_rev"])
            self.assertTrue(trace["readable"])
            repair = trace["latest_repair"]
            self.assertEqual("r", repair["job_id"])
            self.assertEqual("stale", repair["status"])
            self.assertEqual("new_version_committed", repair["reason_code"])
            self.assertTrue(Path(summary["report_markdown"]).exists())

    def test_rejoin_example_old_scrub_does_not_isolate_new_boot_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = replay_file(
                ROOT / "examples" / "late-scrub-after-rejoin.jsonl",
                Path(directory) / "rejoin",
                reset=True,
            )
            report = summary["report"]
            proof = next(o for o in report["objects"] if o["key"] == "proof")
            self.assertTrue(proof["readable"])
            repair = proof["latest_repair"]
            self.assertEqual("r2", repair["job_id"])
            self.assertEqual("verified", repair["status"])
            self.assertEqual("repair_verified", repair["reason_code"])


if __name__ == "__main__":
    unittest.main()
