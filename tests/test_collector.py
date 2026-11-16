"""Unit + integration tests for the reconciling collector.

Run with:  python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collector import Collector  # noqa: E402
from app.db import Store  # noqa: E402
from app.sources import SourceManager  # noqa: E402


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="collector-test-")
        self.store = Store(os.path.join(self.tmp, "test.db"))
        self.sources = SourceManager(self.store)
        self.collector = Collector(self.store)
        self.sources.create("s", "test", page_size=2)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, key, value, **kw):
        return self.sources.write("s", key, {"v": value}, **kw)

    def rec(self, key):
        r = self.store.record("s", key)
        return dict(r) if r else None

    def scan(self):
        started = self.collector.start_full_scan(self.sources, "s")
        return self.collector.run_full_scan(
            self.sources, "s", started["run_id"], started["scan_id"])


class RevisionOrdering(Base):
    def test_lower_revision_ignored_even_if_it_arrives_later(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.assertEqual(self.rec("a")["revision"], 1)
        # Re-deliver the same older event out of order.
        self.collector.ingest("s", [{
            "biz_key": "a", "revision": 1, "payload": {"key": "a", "v": 999},
            "content_hash": "deadbeef"}], via="pull-point")
        self.assertEqual(self.rec("a")["revision"], 1)
        self.assertNotEqual(self.rec("a")["content_hash"], "deadbeef")

    def test_newer_revision_wins_regardless_of_arrival_order(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        # Pull sees the newer revision first, push re-delivers the older.
        self.write("a", 2, suppress_push=True)
        self.collector.point_recollect(self.sources, "s", ["a"])
        self.assertEqual(self.rec("a")["revision"], 2)
        self.collector.ingest("s", [{
            "biz_key": "a", "revision": 1, "payload": {"key": "a", "v": 1},
            "content_hash": self.rec("a")["content_hash"]}], via="push")
        self.assertEqual(self.rec("a")["revision"], 2)

    def test_arrival_order_is_not_used(self):
        # Two independent writes; deliver the second event before the first.
        first = self.write("a", 1)
        second = self.write("a", 2)
        self.assertGreater(second["revision"], first["revision"])
        self.collector.ingest("s", [second, first], via="push")
        self.assertEqual(self.rec("a")["revision"], second["revision"])
        self.assertEqual(json.loads(self.rec("a")["payload"])["v"], 2)


class Duplicates(Base):
    def test_push_then_pull_is_counted_once(self):
        self.write("a", 1)
        self.write("b", 2)
        res = self.collector.drain_push(self.sources, "s")
        self.assertEqual(res["applied"], 2)
        scan = self.scan()
        self.assertEqual(scan["applied"], 0)
        self.assertEqual(scan["duplicate"], 2)
        self.assertEqual(scan["proof"]["complete"], True)
        self.assertEqual(
            self.store.one("SELECT COUNT(*) AS n FROM records")["n"], 2)

    def test_same_page_replayed_twice_is_idempotent(self):
        self.write("a", 1)
        self.write("b", 2)
        events = [
            {"biz_key": "a", "revision": 1, "payload": {"key": "a", "v": 1}},
            {"biz_key": "b", "revision": 2, "payload": {"key": "b", "v": 2}},
        ]
        from app.db import content_hash
        for e in events:
            e["content_hash"] = content_hash(e["payload"])
        first = self.collector.ingest("s", events, via="pull-full")
        second = self.collector.ingest("s", events, via="pull-full")
        self.assertEqual(first["applied"], 2)
        self.assertEqual(second["applied"], 0)
        self.assertEqual(second["duplicate"], 2)

    def test_equal_revision_equal_bytes_is_duplicate_not_conflict(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        rec = self.rec("a")
        counts = self.collector.ingest("s", [{
            "biz_key": "a", "revision": rec["revision"],
            "payload": json.loads(rec["payload"]),
            "content_hash": rec["content_hash"],
        }], via="pull-point")
        self.assertEqual(counts["duplicate"], 1)
        self.assertEqual(counts["conflict"], 0)


class AbsenceIsNotDeletion(Base):
    def test_empty_page_does_not_delete_local_record(self):
        self.write("a", 1)
        self.write("b", 2)
        self.collector.drain_push(self.sources, "s")
        # A page that contains only "a" must not remove "b".
        self.collector.ingest("s", [{
            "biz_key": "a", "revision": 1,
            "payload": {"key": "a", "v": 1},
            "content_hash": self.rec("a")["content_hash"],
        }], via="pull-full")
        self.assertIsNotNone(self.rec("b"))
        self.assertEqual(self.rec("b")["deleted"], 0)

    def test_point_not_found_keeps_record_and_opens_suspect(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        # Ask for a key that exists locally but never remotely.
        self.store.execute(
            """INSERT INTO records
               (source_id,biz_key,revision,payload,content_hash,deleted,via,
                first_via,confirmed_rev,updated_at,first_seen_at)
               VALUES('s','ghost',1,'{}','x',0,'push','push',0,0,0)""")
        res = self.collector.point_recollect(self.sources, "s", ["ghost"])
        self.assertEqual(res["not_found"], 1)
        self.assertIsNotNone(self.rec("ghost"))
        self.assertEqual(self.rec("ghost")["deleted"], 0)
        suspect = self.store.one(
            "SELECT * FROM suspects WHERE biz_key='ghost' AND resolved_at IS NULL")
        self.assertIsNotNone(suspect)

    def test_tombstone_with_higher_revision_deletes(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.write("a", 0, deleted=True)
        self.collector.drain_push(self.sources, "s")
        self.assertEqual(self.rec("a")["deleted"], 1)

    def test_stale_tombstone_does_not_delete(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.collector.ingest("s", [{
            "biz_key": "a", "revision": 1, "payload": {},
            "content_hash": "x", "deleted": True,
        }], via="pull-point")
        self.assertEqual(self.rec("a")["deleted"], 0)


class Conflicts(Base):
    def test_conflict_recorded_and_durable(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        info = self.sources.inject_conflict("s", "a")
        self.collector.drain_push(self.sources, "s")
        row = self.store.one(
            "SELECT * FROM conflicts WHERE biz_key='a' AND resolved_at IS NULL")
        self.assertIsNotNone(row)
        self.assertEqual(row["revision"], 1)
        self.assertEqual(self.rec("a")["revision"], 1)
        # Redelivery increments the counter but does not create a second row.
        self.collector.ingest("s", [{
            "biz_key": "a", "revision": 1,
            "payload": {"key": "a", "v": 1, "__fork__": "x"},
            "content_hash": info["content_hash"],
        }], via="push")
        rows = self.store.q(
            "SELECT * FROM conflicts WHERE biz_key='a'")
        self.assertEqual(len(rows), 1)

    def test_operator_keep_local_and_accept_remote(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.sources.inject_conflict("s", "a")
        self.collector.drain_push(self.sources, "s")
        row = self.store.one(
            "SELECT * FROM conflicts WHERE biz_key='a' AND resolved_at IS NULL")
        res = self.collector.resolve_conflict(
            self.sources, "s", "a", row["id"], "keep_local", "倒序到达不是依据")
        self.assertEqual(res["outcome"], "keep_local")
        self.assertEqual(self.rec("a")["content_hash"], row["local_hash"])
        self.assertIsNone(self.store.one(
            "SELECT * FROM conflicts WHERE resolved_at IS NULL"))

    def test_accept_remote_does_not_fake_pull_confirmation(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.sources.inject_conflict("s", "a")
        self.collector.drain_push(self.sources, "s")
        row = self.store.one(
            "SELECT * FROM conflicts WHERE biz_key='a' AND resolved_at IS NULL")
        self.collector.resolve_conflict(
            self.sources, "s", "a", row["id"], "accept_remote")
        rec = self.rec("a")
        self.assertEqual(rec["content_hash"], row["remote_hash"])
        self.assertIsNone(rec["confirm_hash"])


class PushGaps(Base):
    def test_sequence_jump_creates_gap_and_suspects(self):
        for i in range(3):
            self.write(f"a{i}", i)
        self.collector.drain_push(self.sources, "s")
        self.sources.set_outage("s", 2)
        self.write("a0", 10)
        self.write("a1", 11)
        self.write("a2", 12)
        res = self.collector.drain_push(self.sources, "s")
        self.assertEqual(res["gaps"], [[4, 5]])
        gap = self.store.one("SELECT * FROM gaps WHERE resolved_at IS NULL")
        self.assertEqual(gap["seq_from"], 4)
        self.assertEqual(gap["seq_to"], 5)
        keys = {r["biz_key"] for r in self.store.q(
            "SELECT biz_key FROM suspects WHERE resolved_at IS NULL")}
        self.assertEqual(keys, {"a0", "a1"})

    def test_tail_loss_is_detected_against_push_head(self):
        for i in range(2):
            self.write(f"a{i}", i)
        self.collector.drain_push(self.sources, "s")
        self.sources.set_outage("s", 2)
        self.write("a0", 9)
        self.write("a1", 10)
        res = self.collector.drain_push(self.sources, "s")
        self.assertEqual(res["gaps"], [[3, 4]])
        gap = self.store.one(
            "SELECT * FROM gaps WHERE reason='push_tail_not_delivered'")
        self.assertIsNotNone(gap)

    def test_max_batches_stop_is_not_a_gap(self):
        for i in range(10):
            self.write(f"a{i}", i)
        res = self.collector.drain_push(self.sources, "s", max_batches=1)
        self.assertEqual(res["gaps"], [])
        self.assertEqual(len(self.store.q("SELECT * FROM gaps")), 0)

    def test_full_scan_closes_gap_only_within_coverage(self):
        for i in range(3):
            self.write(f"a{i}", i)
        self.collector.drain_push(self.sources, "s")
        self.sources.set_outage("s", 2)
        self.write("a0", 10)
        self.write("a1", 11)
        self.collector.drain_push(self.sources, "s")
        self.assertIsNotNone(self.store.one(
            "SELECT * FROM gaps WHERE resolved_at IS NULL"))
        scan = self.scan()
        self.assertTrue(scan["proof"]["complete"])
        self.assertIsNone(self.store.one(
            "SELECT * FROM gaps WHERE resolved_at IS NULL"))


class PullScan(Base):
    def test_scan_proof_and_chain(self):
        for i in range(5):
            self.write(f"k{i}", i)
        scan = self.scan()
        self.assertTrue(scan["proof"]["complete"])
        self.assertEqual(scan["proof"]["coverage_seq"], 5)
        self.assertEqual(scan["proof"]["head_seq_at_cutoff"], 5)
        self.assertEqual(scan["proof"]["pages"], 3)  # 2 + 2 + terminal(1)
        # The chain is recomputable from the journal alone.
        verify = self.collector._verify_scan(scan["scan_id"])
        self.assertTrue(verify["ok"], verify["errors"])

    def test_tampered_page_breaks_verification(self):
        for i in range(4):
            self.write(f"k{i}", i)
        scan = self.scan()
        self.store.execute(
            "UPDATE requests SET page_hash='0000000000000000' "
            "WHERE scan_id=? AND page_index=2", (scan["scan_id"],))
        verify = self.collector._verify_scan(scan["scan_id"])
        self.assertFalse(verify["ok"])
        self.assertTrue(any("hash mismatch" in e for e in verify["errors"]))

    def test_pull_lag_reduces_coverage_watermark(self):
        self.sources.update("s", pull_delay_ms=5000)
        for i in range(3):
            self.write(f"k{i}", i)
        # Writes are accepted but not yet visible to pull.
        scan = self.scan()
        self.assertTrue(scan["proof"]["complete"])
        self.assertEqual(scan["proof"]["coverage_seq"], 0)
        self.assertEqual(scan["proof"]["head_seq_at_cutoff"], 3)
        self.assertEqual(scan["proof"]["lag_writes_at_cutoff"], 3)
        self.assertEqual(scan["proof"]["pages"], 1)  # only the empty page

    def test_scan_can_resume_after_terminal_page(self):
        for i in range(5):
            self.write(f"k{i}", i)
        started = self.collector.start_full_scan(self.sources, "s")
        crashed = self.collector.run_full_scan(
            self.sources, "s", started["run_id"], crash_after_pages=3)
        self.assertTrue(crashed["crashed"])
        resumed = self.collector.run_full_scan(
            self.sources, "s", started["run_id"], started["scan_id"])
        self.assertTrue(resumed["proof"]["complete"])
        # No extra page request: the terminal page is not re-read.
        pages = self.store.q(
            "SELECT page_index FROM requests WHERE scan_id=? ORDER BY page_index",
            (started["scan_id"],))
        self.assertEqual([p["page_index"] for p in pages], [1, 2, 3])

    def test_interrupted_scan_resumes_without_duplicate_pages(self):
        for i in range(5):
            self.write(f"k{i}", i)
        started = self.collector.start_full_scan(self.sources, "s")
        first = self.collector.run_full_scan(
            self.sources, "s", started["run_id"], crash_after_pages=2)
        self.assertTrue(first["crashed"])
        self.assertEqual(
            self.store.one("SELECT status FROM runs WHERE run_id=?",
                           (started["run_id"],))["status"], "interrupted")
        second = self.collector.run_full_scan(
            self.sources, "s", started["run_id"], started["scan_id"])
        self.assertTrue(second["proof"]["complete"])
        pages = self.store.q(
            "SELECT page_index FROM requests WHERE scan_id=? ORDER BY page_index",
            (started["scan_id"],))
        self.assertEqual([p["page_index"] for p in pages], [1, 2, 3])
        verify = self.collector._verify_scan(started["scan_id"])
        self.assertTrue(verify["ok"], verify["errors"])
        # The already-applied page 1/2 must not have been applied twice.
        applied = self.store.q(
            """SELECT COUNT(*) AS n FROM records WHERE source_id='s'""")
        self.assertEqual(applied[0]["n"], 5)

    def test_stopped_scan_keeps_checkpoint(self):
        for i in range(5):
            self.write(f"k{i}", i)
        started = self.collector.start_full_scan(self.sources, "s")
        self.collector.run_full_scan(
            self.sources, "s", started["run_id"], stop_after_pages=1)
        run = self.store.one("SELECT * FROM runs WHERE run_id=?",
                             (started["run_id"],))
        self.assertEqual(run["status"], "stopped")
        self.assertIn("after_key", run["cursor"])
        resumed = self.collector.run_full_scan(
            self.sources, "s", started["run_id"], started["scan_id"])
        self.assertTrue(resumed["proof"]["complete"])

    def test_pull_disabled_blocks_scan(self):
        self.write("a", 1)
        self.sources.update("s", pull_enabled=False)
        with self.assertRaises(ValueError):
            self.collector.start_full_scan(self.sources, "s")


class PointRecovery(Base):
    def test_revision_changing_during_recovery_is_not_lost(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        # The key is revised between the fetch and the confirming read.
        calls = {"n": 0}

        def hook(key, revision):
            if calls["n"] == 0:
                calls["n"] += 1
                self.write("a", 2)

        res = self.collector.point_recollect(
            self.sources, "s", ["a"], before_confirm=hook)
        self.assertEqual(self.rec("a")["revision"], 2)
        self.assertIn("a", res["unstable"])
        self.assertEqual(json.loads(self.rec("a")["payload"])["v"], 2)

    def test_point_run_resumes_after_crash(self):
        for i in range(4):
            self.write(f"a{i}", i)
        self.collector.drain_push(self.sources, "s")
        started = self.collector.point_recollect(
            self.sources, "s", ["a0", "a1", "a2", "a3"],
            crash_after_keys=2)
        self.assertTrue(started["crashed"])
        run_id = started["run_id"]
        resumed = self.collector.point_recollect(
            self.sources, "s", run_id=run_id)
        self.assertEqual(resumed["confirmed"], 2)
        self.assertEqual(resumed["skipped"], ["a0", "a1"])
        # Total point requests: 2 keys each first round + 2 keys each resume.
        rows = self.store.q(
            "SELECT COUNT(*) AS n FROM requests WHERE run_id=?", (run_id,))
        self.assertEqual(rows[0]["n"], 8)

    def test_confirm_requires_stable_second_read(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        res = self.collector.point_recollect(self.sources, "s", ["a"])
        self.assertEqual(res["confirmed"], 1)
        # Two point requests per key: fetch + stability re-read.
        self.assertEqual(res["requests"], 2)


class Conclusion(Base):
    def test_conclusion_never_exceeds_certificate(self):
        for i in range(3):
            self.write(f"k{i}", i)
        self.collector.drain_push(self.sources, "s")
        self.scan()
        self.write("k0", 99)
        status = self.collector.status(self.sources, "s")
        self.assertNotEqual(status["conclusion"]["status"], "proven")
        self.assertLessEqual(status["conclusion"]["coverage_seq"], 3)

    def test_push_only_tail_is_not_proven(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.scan()
        self.sources.set_outage("s", 1)
        self.write("a", 2)
        self.collector.drain_push(self.sources, "s")
        # Recover by point so the record is right, but push evidence is short.
        self.collector.point_recollect(self.sources, "s", ["a"])
        status = self.collector.status(self.sources, "s")
        self.assertEqual(status["truth"]["behind"], [])
        self.assertIn(status["conclusion"]["status"], ("open_items", "not_proven"))

    def test_plan_marks_full_when_gap_present(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.sources.set_outage("s", 1)
        self.write("b", 2)
        self.collector.drain_push(self.sources, "s")
        plan = self.collector.plan(self.sources, "s")
        self.assertEqual(plan["recommendation"]["strategy"], "full")
        self.assertEqual(plan["options"][0]["strategy"], "full")
        self.assertGreater(plan["options"][0]["requests"], 0)

    def test_plan_recommends_full_when_no_certificate_exists(self):
        # Nothing is wrong that a named-key list could describe, but without a
        # scan there is no coverage evidence at all.
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        plan = self.collector.plan(self.sources, "s")
        self.assertEqual(plan["targeted_keys"], [])
        self.assertEqual(plan["recommendation"]["strategy"], "full")
        self.assertIn("证书", plan["recommendation"]["reason"])

    def test_plan_recommends_full_for_open_conflicts(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.sources.inject_conflict("s", "a")
        self.collector.drain_push(self.sources, "s")
        plan = self.collector.plan(self.sources, "s")
        self.assertEqual(plan["open_items"]["conflicts"], 1)
        self.assertEqual(plan["recommendation"]["strategy"], "full")
        self.assertIn("冲突", plan["recommendation"]["reason"])

    def test_plan_prefers_point_when_only_named_keys(self):
        self.write("a", 1)
        self.collector.drain_push(self.sources, "s")
        self.scan()
        self.store.execute(
            """INSERT INTO suspects(source_id,biz_key,reason,detected_at)
               VALUES('s','a','equal_revision_conflict',0)""")
        plan = self.collector.plan(self.sources, "s")
        self.assertEqual(plan["recommendation"]["strategy"], "point")
        self.assertEqual(plan["options"][1]["requests"], 2)


if __name__ == "__main__":
    unittest.main()
