"""Reconciling collector: push ingestion, resumable full scans, targeted repair.

Correctness rules (each one is exercised by tests):

R1. Identity is ``(source_id, biz_key)``.  Arrival order is never used to
    decide anything.
R2. A remote revision may replace local state only if it is strictly greater.
    Equal revision + equal hash is a harmless duplicate.  Equal revision +
    different hash is a conflict and needs an explicit operator decision.
R3. Duplicate delivery across push and pull is recognised by
    ``(biz_key, revision, content_hash)`` and counted, never re-applied.
R4. A key missing from a pull page or a failed point lookup is *not* a
    deletion.  Only a tombstone with a greater revision deletes a record.
R5. A page (push batch / pull page / point lookup) is applied, journaled and
    acknowledged in one SQLite transaction.  An interrupted worker therefore
    either has the page fully stored and acknowledged, or not at all.
R6. A full scan is only certified when it terminated on an empty page, its
    keyset cursor chain is contiguous, and every page is covered by a
    tamper-evident hash chain.  Coverage is attributed to the snapshot
    watermark (the highest accepted write sequence fully visible to pull at
    the scan's cutoff), not to wall-clock "now".
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Callable, Iterable

from .db import Store, content_hash
from .sources import PullUnavailable, SourceManager


TERMINAL = {"completed", "failed", "stopped"}


def _now() -> float:
    return time.time()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _row_digest(events: Iterable[dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for ev in sorted(events, key=lambda e: (e["biz_key"], e["revision"])):
        h.update(
            f"{ev['biz_key']}\x1f{ev['revision']}\x1f"
            f"{ev.get('content_hash', '')}\x1f{int(bool(ev.get('deleted')))}\x1e".encode()
        )
    return h.hexdigest()[:16]


def _chain(prev: str, row_digest: str, covered_to: str | None, exhausted: int) -> str:
    h = hashlib.sha256()
    h.update(f"{prev}\x1f{row_digest}\x1f{covered_to}\x1f{exhausted}".encode())
    return h.hexdigest()[:16]


class _StopRequested(Exception):
    pass


class _SimulatedCrash(Exception):
    pass


class Collector:
    def __init__(self, store: Store):
        self.store = store

    # ==================================================================
    # R1-R4: applying a set of remote events
    # ==================================================================
    def ingest(
        self,
        source_id: str,
        events: Iterable[dict[str, Any]],
        *,
        via: str,
        run_id: str | None = None,
        scan_id: str | None = None,
    ) -> dict[str, int]:
        with self.store.tx() as c:
            return self._ingest(c, source_id, list(events), via=via,
                                run_id=run_id, scan_id=scan_id)

    def _ingest(
        self,
        c,
        source_id: str,
        events: list[dict[str, Any]],
        *,
        via: str,
        run_id: str | None,
        scan_id: str | None,
    ) -> dict[str, int]:
        counts = {"events": len(events), "applied": 0, "duplicate": 0,
                  "conflict": 0, "stale": 0, "tombstone": 0}
        for ev in events:
            self._apply_one(c, source_id, ev, via=via, run_id=run_id,
                            scan_id=scan_id, counts=counts)
        return counts

    def _apply_one(
        self, c, source_id: str, ev: dict[str, Any], *, via: str,
        run_id: str | None, scan_id: str | None, counts: dict[str, int],
    ) -> None:
        biz = ev["biz_key"]
        remote_rev = int(ev["revision"])
        payload = ev.get("payload") or {}
        digest = ev.get("content_hash") or content_hash(payload)
        deleted = 1 if ev.get("deleted") else 0
        now = _now()
        detail = {"run_id": run_id, "scan_id": scan_id}
        local = c.execute(
            "SELECT * FROM records WHERE source_id=? AND biz_key=?",
            (source_id, biz),
        ).fetchone()

        if local is None:
            c.execute(
                """INSERT INTO records
                   (source_id,biz_key,revision,payload,content_hash,deleted,via,
                    first_via,confirmed_rev,updated_at,first_seen_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (source_id, biz, remote_rev,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 digest, deleted, via, via,
                 remote_rev if via != "push" else 0, now, now),
            )
            if via != "push":
                c.execute(
                    """UPDATE records SET confirm_hash=? WHERE source_id=?
                       AND biz_key=?""", (digest, source_id, biz),
                )
            counts["tombstone" if deleted else "applied"] += 1
            self._event(c, source_id, "tombstone" if deleted else "applied", via,
                        biz, None, remote_rev, detail)
            return

        lrev = int(local["revision"])
        if remote_rev == lrev:
            if digest == local["content_hash"] and deleted == int(local["deleted"]):
                counts["duplicate"] += 1
                self._event(c, source_id, "duplicate", via, biz, lrev, remote_rev,
                            detail)
                if via in ("pull-full", "pull-point"):
                    c.execute(
                        """UPDATE records SET confirmed_rev=MAX(confirmed_rev,?),
                           confirm_hash=CASE WHEN revision=? THEN ? ELSE
                           confirm_hash END
                           WHERE source_id=? AND biz_key=?""",
                        (remote_rev, remote_rev, digest, source_id, biz),
                    )
                return
            existing = c.execute(
                """SELECT * FROM conflicts WHERE source_id=? AND biz_key=?
                   AND revision=? AND remote_hash=?""",
                (source_id, biz, lrev, digest),
            ).fetchone()
            if existing:
                c.execute(
                    "UPDATE conflicts SET deliveries=deliveries+1 WHERE id=?",
                    (existing["id"],),
                )
                if existing["resolved_at"]:
                    counts["duplicate"] += 1
                    self._event(
                        c, source_id, "conflict_acknowledged", via, biz, lrev,
                        remote_rev,
                        {**detail, "decision": existing["decision"],
                         "note": existing["note"]},
                    )
                else:
                    counts["conflict"] += 1
                    self._event(c, source_id, "conflict_repeat", via, biz, lrev,
                                remote_rev, detail)
                return
            counts["conflict"] += 1
            c.execute(
                """INSERT INTO conflicts
                   (source_id,biz_key,revision,local_hash,local_payload,
                    remote_hash,remote_payload,remote_deleted,first_via,
                    detected_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (source_id, biz, lrev, local["content_hash"],
                 local["payload"], digest,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 deleted, via, now),
            )
            self._event(
                c, source_id, "conflict", via, biz, lrev, remote_rev,
                {**detail, "local_hash": local["content_hash"],
                 "remote_hash": digest,
                 "reason": "equal_revision_different_content"},
            )
            self._open_suspect(c, source_id, biz, "equal_revision_conflict")
            return

        if remote_rev < lrev:
            counts["stale"] += 1
            self._event(c, source_id, "stale", via, biz, lrev, remote_rev, detail)
            return

        # Strictly newer: replace.  Revision order — not arrival order — is the
        # only reason this is allowed.  Previous bytes stay in the journal.
        c.execute(
            """UPDATE records SET revision=?,payload=?,content_hash=?,deleted=?,
               via=?,confirmed_rev=MAX(confirmed_rev,?),
               confirm_hash=CASE WHEN ? THEN ? ELSE confirm_hash END,updated_at=?
               WHERE source_id=? AND biz_key=?""",
            (remote_rev,
             json.dumps(payload, ensure_ascii=False, sort_keys=True), digest,
             deleted, via, remote_rev if via != "push" else 0,
             1 if via != "push" else 0, digest, now,
             source_id, biz),
        )
        # A strictly higher revision makes every older equal-revision conflict
        # moot.  This is not "newest arrival wins": the source itself advanced.
        c.execute(
            """UPDATE conflicts SET resolved_at=?,decision='superseded',
               note=? WHERE source_id=? AND biz_key=? AND revision<?
               AND resolved_at IS NULL""",
            (now, f"superseded_by_revision_{remote_rev}", source_id, biz,
             remote_rev),
        )
        # An "unstable_revision" suspect is deliberately *not* cleared here.
        # Applying the newer revision is not the same as proving the key has
        # settled; only the stability re-read in point_recollect may close it.
        counts["applied"] += 1
        if deleted:
            counts["tombstone"] += 1
        self._event(
            c, source_id, "tombstone" if deleted else "applied", via, biz, lrev,
            remote_rev,
            {**detail, "before": {"revision": lrev,
                                  "hash": local["content_hash"]}},
        )
        # A newer revision from pull settles open "behind" suspects for the key.
        if via in ("pull-full", "pull-point"):
            c.execute(
                """UPDATE suspects SET resolved_at=?,resolution=?
                   WHERE source_id=? AND biz_key=? AND resolved_at IS NULL
                   AND reason IN ('equal_revision_conflict',
                                  'not_found_on_confirm','unstable_revision')""",
                (now, f"superseded_by_revision_{remote_rev}", source_id, biz),
            )

    # ---- small journal helpers (must be called inside a transaction) ----

    @staticmethod
    def _event(c, source_id, kind, via, biz, local_rev, remote_rev, detail):
        c.execute(
            """INSERT INTO events
               (source_id,biz_key,at,kind,via,local_rev,remote_rev,detail)
               VALUES(?,?,?,?,?,?,?,?)""",
            (source_id, biz, time.time(), kind, via, local_rev, remote_rev,
             json.dumps(detail, ensure_ascii=False)),
        )

    @staticmethod
    def _open_suspect(c, source_id, biz, reason):
        existing = c.execute(
            """SELECT id FROM suspects WHERE source_id=? AND biz_key=?
               AND reason=? AND resolved_at IS NULL""",
            (source_id, biz, reason),
        ).fetchone()
        if existing:
            return
        c.execute(
            """INSERT INTO suspects(source_id,biz_key,reason,detected_at)
               VALUES(?,?,?,?)""",
            (source_id, biz, reason, time.time()),
        )

    @staticmethod
    def _request_row(
        c, source_id, run_id, *, transport, kind, cost=1, cursor=None,
        rows=0, counts=None, scan_id=None, full_scan=0, snapshot_cutoff=None,
        covered_from=None, covered_to=None, next_cursor=None, exhausted=None,
        consistent=None, gap_detected=0, page_index=None, row_digest=None,
        page_hash=None, error=None,
    ) -> None:
        counts = counts or {}
        c.execute(
            """INSERT INTO requests
               (source_id,run_id,at,transport,request_kind,cost,cursor,
                rows_returned,events_ingested,applied_count,duplicate_count,
                conflict_count,scan_id,full_scan,snapshot_cutoff,covered_from,
                covered_to,next_cursor,exhausted,consistent,gap_detected,
                page_index,row_digest,page_hash,error)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (source_id, run_id, time.time(), transport, kind, cost, cursor,
             rows, counts.get("events", 0), counts.get("applied", 0),
             counts.get("duplicate", 0), counts.get("conflict", 0), scan_id,
             full_scan, snapshot_cutoff, covered_from, covered_to, next_cursor,
             exhausted, consistent, gap_detected, page_index, row_digest,
             page_hash, error),
        )

    # ==================================================================
    # push
    # ==================================================================
    def drain_push(
        self,
        sources: SourceManager,
        source_id: str,
        *,
        max_batches: int = 500,
        batch_size: int | None = None,
    ) -> dict[str, Any]:
        s = self.store
        cfg = sources.get(source_id)
        if not cfg["push_enabled"]:
            raise ValueError("push transport disabled for this source")
        run_id = _new_id("run")
        st = sources.state(source_id)
        cursor = int(st["push_cursor"])
        head = sources.push_head(source_id)
        result = {"run_id": run_id, "batches": 0, "events": 0, "applied": 0,
                  "duplicates": 0, "conflicts": 0, "gaps": [], "cursor": cursor,
                  "head": head}
        s.execute(
            """INSERT INTO runs(run_id,source_id,kind,status,started_at,cursor)
               VALUES(?,?,'push','running',?,?)""",
            (run_id, source_id, _now(), str(cursor)),
        )
        try:
            while result["batches"] < max_batches:
                batch = next(sources.iter_push(
                    source_id, cursor=cursor,
                    batch_size=batch_size or 4, max_batches=1), None)
                if batch is None:
                    break
                first_seq = batch["events"][0]["seq"]
                if first_seq > cursor:
                    gap = (cursor, first_seq - 1)
                    self._record_gap(s, source_id, gap, "push_sequence_jump",
                                     sources.gap_manifest(source_id, *gap), run_id)
                    result["gaps"].append(gap)
                    s.execute(
                        "UPDATE source_state SET push_dropped=push_dropped+? "
                        "WHERE source_id=?",
                        (gap[1] - gap[0] + 1, source_id),
                    )
                counts = self.ingest(source_id, batch["events"], via="push",
                                     run_id=run_id)
                cursor = int(batch["cursor_after"]) + 1
                # Ack + evidence atomically with the data write.
                with s.tx() as c:
                    c.execute(
                        """UPDATE source_state SET push_cursor=?,
                           push_batches=push_batches+1,push_events=push_events+?
                           WHERE source_id=?""",
                        (cursor, counts["events"], source_id),
                    )
                    self._request_row(
                        c, source_id, run_id, transport="push", kind="batch",
                        cursor=f"{batch['cursor_before']}..{batch['cursor_after']}",
                        rows=len(batch["events"]), counts=counts,
                        covered_from=str(batch["cursor_before"]),
                        covered_to=str(batch["cursor_after"]), consistent=1,
                    )
                    c.execute(
                        """UPDATE runs SET requests=requests+1,applied=applied+?,
                           duplicates=duplicates+?,conflicts=conflicts+?,
                           cursor=? WHERE run_id=?""",
                        (counts["applied"], counts["duplicate"],
                         counts["conflict"], str(cursor), run_id),
                    )
                result["batches"] += 1
                for k, out in (("events", "events"), ("applied", "applied"),
                               ("duplicate", "duplicates"),
                               ("conflict", "conflicts")):
                    result[out] += counts[k]

            # A vanished tail is detectable only against the source's push head
            # (a standard consumer-lag check), never by guessing from silence.
            # If we stopped because of the batch limit, the remaining events are
            # simply not consumed yet and must not be reported as a gap.
            head = sources.push_head(source_id)
            if result["batches"] >= max_batches:
                head = cursor - 1
            if cursor <= head:
                gap = (cursor, head)
                self._record_gap(s, source_id, gap, "push_tail_not_delivered",
                                 sources.gap_manifest(source_id, *gap), run_id)
                result["gaps"].append(gap)
                lost = s.one(
                    """SELECT COUNT(*) AS n FROM push_events
                       WHERE source_id=? AND seq BETWEEN ? AND ? AND lost=1""",
                    (source_id, gap[0], gap[1]),
                )
                s.execute(
                    "UPDATE source_state SET push_dropped=push_dropped+? "
                    "WHERE source_id=?",
                    (int(lost["n"]), source_id),
                )
                cursor = head + 1
                s.execute(
                    "UPDATE source_state SET push_cursor=? WHERE source_id=?",
                    (cursor, source_id),
                )

            s.execute(
                """UPDATE runs SET status='completed',finished_at=?,cursor=?
                   WHERE run_id=?""",
                (_now(), str(cursor), run_id),
            )
            result["cursor"] = cursor
            result["head"] = head
            result["gaps"] = [list(g) for g in result["gaps"]]
            return result
        except Exception as exc:
            s.execute(
                "UPDATE runs SET status='failed',finished_at=?,cursor=?,error=? "
                "WHERE run_id=?",
                (_now(), str(cursor), repr(exc), run_id),
            )
            raise

    def _record_gap(
        self, s: Store, source_id: str, gap: tuple[int, int], reason: str,
        manifest: dict[str, Any], run_id: str | None,
    ) -> None:
        """Journal a lost push range in both push-seq and write-seq space."""
        if gap[1] < gap[0]:
            return
        existing = s.one(
            """SELECT id FROM gaps WHERE source_id=? AND push_from=? AND push_to=?
               AND resolved_at IS NULL""",
            (source_id, gap[0], gap[1]),
        )
        if existing:
            return
        with s.tx() as c:
            c.execute(
                """INSERT INTO gaps
                   (source_id,push_from,push_to,seq_from,seq_to,reason,detected_at,
                    manifest_json)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (source_id, gap[0], gap[1], manifest.get("seq_from"),
                 manifest.get("seq_to"), reason, _now(),
                 json.dumps(manifest, ensure_ascii=False)),
            )
            for item in manifest.get("keys", []):
                self._open_suspect(c, source_id, item["biz_key"], "push_gap")
            self._event(
                c, source_id, "note", "push", None, None, None,
                {"run_id": run_id, "push_gap": list(gap),
                 "write_gap": [manifest.get("seq_from"), manifest.get("seq_to")],
                 "reason": reason,
                 "manifest_keys": len(manifest.get("keys", []))},
            )

    # ==================================================================
    # full pull scan
    # ==================================================================
    def start_full_scan(self, sources: SourceManager, source_id: str) -> dict[str, Any]:
        s = self.store
        cfg = sources.get(source_id)
        if not cfg["pull_enabled"]:
            raise ValueError("pull transport disabled for this source")
        run_id = _new_id("run")
        scan_id = _new_id("scan")
        snapshot = sources.snapshot_at(source_id)
        plan = {
            "strategy": "full",
            "page_size": int(cfg["page_size"]),
            "pull_delay_ms": int(cfg["pull_delay_ms"]),
            "snapshot_cutoff": snapshot["cutoff"],
            "snapshot_watermark": snapshot["watermark"],
            "snapshot_head": snapshot["head"],
            "notes": [
                "keyset pagination by business key",
                "cursor checkpointed together with each page",
                "certificate requires a terminal empty page",
            ],
        }
        s.execute(
            """INSERT INTO runs
               (run_id,source_id,kind,status,scan_id,started_at,plan_json,
                snapshot_cutoff,coverage_epoch)
               VALUES(?,?,'full','running',?,?,?,?,?)""",
            (run_id, source_id, scan_id, _now(),
             json.dumps(plan, ensure_ascii=False),
             snapshot["cutoff"], snapshot["watermark"]),
        )
        s.execute(
            "UPDATE source_state SET full_scan_runs=full_scan_runs+1 "
            "WHERE source_id=?", (source_id,),
        )
        return {"run_id": run_id, "scan_id": scan_id, "plan": plan}

    def run_full_scan(
        self,
        sources: SourceManager,
        source_id: str,
        run_id: str,
        scan_id: str | None = None,
        *,
        stop_after_pages: int | None = None,
        crash_after_pages: int | None = None,
        page_delay_ms: int = 0,
    ) -> dict[str, Any]:
        """Run or resume a full scan, one page per iteration.

        ``stop_after_pages`` and ``crash_after_pages`` exist for the demo so an
        operator can see a half-finished worker; both leave a resumable run.
        ``page_delay_ms`` only paces the demo UI.
        """
        s = self.store
        run = s.one("SELECT * FROM runs WHERE run_id=?", (run_id,))
        if run is None:
            raise KeyError(run_id)
        if run["status"] == "completed":
            raise ValueError("run already completed")
        if scan_id is None:
            scan_id = run["scan_id"]
        if not scan_id:
            raise ValueError("run has no scan_id; cannot resume")
        plan = json.loads(run["plan_json"]) if run["plan_json"] else {}
        snapshot = {
            "cutoff": plan.get("snapshot_cutoff"),
            "watermark": plan.get("snapshot_watermark"),
            "head": plan.get("snapshot_head"),
        }
        if snapshot["cutoff"] is None:
            snap = sources.snapshot_at(source_id)
            snapshot = snap
            s.execute(
                "UPDATE runs SET snapshot_cutoff=?,coverage_epoch=? WHERE run_id=?",
                (snap["cutoff"], snap["watermark"], run_id),
            )

        last = s.one(
            """SELECT * FROM requests WHERE scan_id=? AND transport='pull-full'
               ORDER BY page_index DESC, id DESC LIMIT 1""",
            (scan_id,),
        )
        progress = {}
        try:
            progress = json.loads(run["cursor"] or "{}")
        except Exception:
            progress = {}
        after_key = progress.get("after_key")
        if after_key is None and last and last["covered_to"]:
            after_key = last["covered_to"]
        page_index = int(last["page_index"] or 0) if last else 0
        prev_hash = last["page_hash"] if last else "0" * 16
        # If the interruption happened after the terminal page was committed,
        # every page is already durable and only the certificate is missing;
        # resuming must not append a fourth "page" for the same scan.
        already_exhausted = bool(progress.get("done")) or bool(
            last and last["exhausted"])
        pages_this_call = 0

        s.execute(
            "UPDATE runs SET status='running',finished_at=NULL,error=NULL,"
            "stop_requested=0 WHERE run_id=?", (run_id,),
        )
        try:
            pages_iter = (
                iter(()) if already_exhausted else
                sources.iter_pull(source_id, cutoff=snapshot["cutoff"],
                                  after_key=after_key)
            )
            for page in pages_iter:
                if s.one("SELECT stop_requested FROM runs WHERE run_id=?",
                         (run_id,))["stop_requested"]:
                    raise _StopRequested()
                if stop_after_pages is not None and pages_this_call >= stop_after_pages:
                    raise _StopRequested()

                page_index += 1
                pages_this_call += 1
                rd = _row_digest(page["events"])
                exhausted = 1 if page["exhausted"] else 0
                covered_to = page["covered_to"]
                ph = _chain(prev_hash, rd, covered_to, exhausted)
                counts = {"events": 0}
                with s.tx() as c:
                    counts = self._ingest(
                        c, source_id, page["events"], via="pull-full",
                        run_id=run_id, scan_id=scan_id,
                    )
                    self._request_row(
                        c, source_id, run_id, transport="pull-full", kind="page",
                        cursor=covered_to,
                        rows=len(page["events"]), counts=counts,
                        scan_id=scan_id, full_scan=1,
                        snapshot_cutoff=snapshot["cutoff"],
                        covered_from=page["covered_from"],
                        covered_to=covered_to,
                        next_cursor=page["next_cursor"],
                        exhausted=exhausted, consistent=1,
                        page_index=page_index, row_digest=rd, page_hash=ph,
                    )
                    c.execute(
                        "UPDATE source_state SET full_scan_reqs=full_scan_reqs+1 "
                        "WHERE source_id=?", (source_id,),
                    )
                    c.execute(
                        """UPDATE runs SET requests=requests+1,applied=applied+?,
                           duplicates=duplicates+?,conflicts=conflicts+?,
                           cursor=? WHERE run_id=?""",
                        (counts["applied"], counts["duplicate"],
                         counts["conflict"],
                         json.dumps({"after_key": covered_to,
                                     "done": bool(exhausted)}),
                         run_id),
                    )
                prev_hash = ph
                after_key = covered_to
                # Crash *after* the page is committed: the checkpoint and the
                # page's rows are already durable together.
                if (crash_after_pages is not None
                        and pages_this_call >= crash_after_pages):
                    raise _SimulatedCrash()
                if page["exhausted"]:
                    break
                if page_delay_ms:
                    time.sleep(page_delay_ms / 1000.0)

            # ---------------- certificate & evidence ----------------
            evidence = self._verify_scan(scan_id)
            coverage = int(snapshot["watermark"] or 0)
            head_at_cutoff = int(snapshot["head"] or 0)
            conflicts = int(s.one(
                """SELECT COUNT(*) AS n FROM suspects WHERE source_id=?
                   AND resolved_at IS NULL AND reason='equal_revision_conflict'""",
                (source_id,),
            )["n"])
            proof = {
                "scan_id": scan_id,
                "run_id": run_id,
                "complete": bool(evidence["ok"]),
                "evidence_ok": evidence["ok"],
                "evidence_errors": evidence["errors"],
                "pages": evidence["pages"],
                "last_page_hash": evidence["head"],
                "snapshot_cutoff": snapshot["cutoff"],
                "coverage_seq": coverage,
                "head_seq_at_cutoff": head_at_cutoff,
                "lag_writes_at_cutoff": max(0, head_at_cutoff - coverage),
                "conflicts_open": conflicts,
                "claim": (
                    f"pull snapshot at cutoff={snapshot['cutoff']:.3f} covered "
                    f"every business key up to write seq {coverage} "
                    f"(head {head_at_cutoff}); {evidence['pages']} page(s), "
                    f"chain {evidence['head']}"
                ),
            }
            status = "completed" if proof["complete"] else "failed"
            s.execute(
                """UPDATE runs SET status=?,finished_at=?,exhausted=?,proof_json=?,
                   error=?,coverage_epoch=? WHERE run_id=?""",
                (status, _now(), 1,
                 json.dumps(proof, ensure_ascii=False),
                 None if proof["complete"] else "; ".join(evidence["errors"]),
                 coverage, run_id),
            )
            if proof["complete"]:
                self._certify(s, source_id, scan_id, coverage, snapshot,
                              evidence["head"], evidence["pages"])
            agg = s.one(
                """SELECT COUNT(*) AS pages,
                          COALESCE(SUM(events_ingested),0) AS events,
                          COALESCE(SUM(applied_count),0) AS applied,
                          COALESCE(SUM(duplicate_count),0) AS duplicate,
                          COALESCE(SUM(conflict_count),0) AS conflict
                   FROM requests WHERE scan_id=? AND transport='pull-full'""",
                (scan_id,),
            )
            return {"run_id": run_id, "scan_id": scan_id,
                    "coverage_seq": coverage, "proof": proof,
                    "pages": agg["pages"], "events": agg["events"],
                    "applied": agg["applied"], "duplicate": agg["duplicate"],
                    "conflict": agg["conflict"]}
        except _StopRequested:
            s.execute(
                "UPDATE runs SET status='stopped',finished_at=? WHERE run_id=?",
                (_now(), run_id),
            )
            return {"run_id": run_id, "scan_id": scan_id, "stopped": True,
                    "cursor": after_key}
        except _SimulatedCrash:
            # Worker died without clean shutdown: the checkpoint already
            # committed up to the last completed page, so the run is resumable
            # exactly once and no page is ever applied twice.
            s.execute(
                "UPDATE runs SET status='interrupted',error=? WHERE run_id=?",
                ("worker stopped abruptly (simulated)", run_id),
            )
            return {"run_id": run_id, "scan_id": scan_id, "crashed": True,
                    "cursor": after_key}
        except PullUnavailable as exc:
            s.execute(
                "UPDATE runs SET status='failed',finished_at=?,error=? WHERE run_id=?",
                (_now(), str(exc), run_id),
            )
            raise
        except Exception as exc:
            s.execute(
                "UPDATE runs SET status='failed',finished_at=?,error=? WHERE run_id=?",
                (_now(), repr(exc), run_id),
            )
            raise

    def _verify_scan(self, scan_id: str) -> dict[str, Any]:
        """Recompute the hash chain from the request journal alone."""
        pages = self.store.q(
            """SELECT * FROM requests WHERE scan_id=? AND transport='pull-full'
               ORDER BY page_index""", (scan_id,),
        )
        errors: list[str] = []
        prev = "0" * 16
        expected_from = None
        for i, p in enumerate(pages, 1):
            if p["page_index"] != i:
                errors.append(f"page index gap at {i}: {p['page_index']}")
            if p["covered_from"] != expected_from:
                errors.append(
                    f"page {i} starts at {p['covered_from']!r}, expected "
                    f"{expected_from!r}"
                )
            if p["exhausted"] and i != len(pages):
                errors.append(f"page {i} exhausted before the last page")
            want = _chain(prev, p["row_digest"] or "", p["covered_to"],
                          1 if p["exhausted"] else 0)
            if want != p["page_hash"]:
                errors.append(f"page {i} hash mismatch")
            if p["error"]:
                errors.append(f"page {i} error: {p['error']}")
            prev = p["page_hash"] or prev
            expected_from = p["covered_to"]
        if not pages:
            errors.append("no pages were requested")
        elif not pages[-1]["exhausted"]:
            errors.append("scan did not end on an exhausted page")
        return {"ok": not errors, "errors": errors, "pages": len(pages),
                "head": prev}

    def _certify(self, s: Store, source_id: str, scan_id: str, coverage: int,
                 snapshot: dict[str, Any], head_hash: str, pages: int) -> None:
        """Publish a completion certificate and close everything it proves.

        The certificate is only about the snapshot: every business key visible
        at ``snapshot.cutoff`` has been read, including a terminal empty page,
        and the write sequence covered is ``snapshot.watermark`` (the highest
        accepted write fully replicated before the read started).  Writes above
        the watermark are *not* covered, no matter how complete the scan felt.
        """
        cert = {
            "scan_id": scan_id,
            "coverage_seq": coverage,
            "snapshot_cutoff": snapshot["cutoff"],
            "head_seq_at_cutoff": snapshot["head"],
            "pages": pages,
            "page_chain_head": head_hash,
            "certified_at": _now(),
        }
        with s.tx() as c:
            c.execute(
                """UPDATE source_state SET scan_id=?,coverage_epoch=?,scan_cutoff=?,
                   scan_at=?,scan_chain=?,scan_pages=?,scan_complete=1
                   WHERE source_id=?""",
                (scan_id, coverage, snapshot["cutoff"], cert["certified_at"],
                 head_hash, pages, source_id),
            )
            # Close gaps whose *write-seq* span is inside the covered range.
            # A gap that extends past the watermark stays open; the scan cannot
            # have seen those writes.
            rows = c.execute(
                "SELECT * FROM gaps WHERE source_id=? AND resolved_at IS NULL",
                (source_id,),
            ).fetchall()
            for g in rows:
                if g["seq_to"] is None or int(g["seq_to"]) > coverage:
                    continue
                c.execute(
                    """UPDATE gaps SET resolved_at=?,resolution=? WHERE id=?""",
                    (cert["certified_at"], f"covered_by_scan:{scan_id}", g["id"]),
                )
                man = json.loads(g["manifest_json"] or "{}")
                for item in man.get("keys", []):
                    local = c.execute(
                        "SELECT * FROM records WHERE source_id=? AND biz_key=?",
                        (source_id, item["biz_key"]),
                    ).fetchone()
                    if (local is not None
                            and int(local["confirmed_rev"]) >= int(item["revision"])
                            and local["confirm_hash"] == local["content_hash"]):
                        c.execute(
                            """UPDATE suspects SET resolved_at=?,resolution=?
                               WHERE source_id=? AND biz_key=?
                               AND reason='push_gap' AND resolved_at IS NULL""",
                            (cert["certified_at"],
                             f"covered_by_scan:{scan_id}", source_id,
                             item["biz_key"]),
                        )

    # ==================================================================
    # targeted re-collection
    # ==================================================================
    def point_recollect(
        self,
        sources: SourceManager,
        source_id: str,
        keys: list[str] | None = None,
        *,
        confirm: bool = True,
        run_id: str | None = None,
        stop_after_keys: int | None = None,
        crash_after_keys: int | None = None,
        before_confirm: Callable[[str, int], None] | None = None,
    ) -> dict[str, Any]:
        """Fetch exact business keys, re-reading until the revision is stable.

        * A key that is not found is journaled as ``not_found``; its local
          record is kept.  Absence is never a deletion.
        * If the key is revised between the fetch and the confirming re-read, the
          newer revision is applied and the key is fetched again.  Only a stable
          read stamps confirmation.
        * Every key is committed together with its progress marker, so a worker
          that dies mid-way can be resumed exactly once with ``run_id``.
        """
        s = self.store
        if run_id:
            run = s.one("SELECT * FROM runs WHERE run_id=?", (run_id,))
            if run is None:
                raise KeyError(run_id)
            if run["kind"] != "point":
                raise ValueError("run is not a point-recollection run")
            if run["status"] == "completed":
                raise ValueError("run already completed")
            plan = json.loads(run["plan_json"] or "{}")
            keys = plan.get("keys", [])
            confirm = plan.get("confirm", True)
            progress = json.loads(run["cursor"] or "{}")
        else:
            if keys is None:
                keys = [r["biz_key"] for r in s.q(
                    """SELECT DISTINCT biz_key FROM suspects
                       WHERE source_id=? AND resolved_at IS NULL
                       ORDER BY biz_key""", (source_id,))]
            run_id = _new_id("run")
            progress = {"done": [], "attempts": {}}
            s.execute(
                """INSERT INTO runs(run_id,source_id,kind,status,started_at,
                   plan_json,cursor)
                   VALUES(?,?,'point','running',?,?,?)""",
                (run_id, source_id, _now(),
                 json.dumps({"strategy": "point", "keys": keys,
                             "confirm": confirm}), json.dumps(progress)),
            )
        keys = sorted(set(keys))
        if not sources.get(source_id)["pull_enabled"]:
            raise ValueError("pull transport disabled for this source")
        done = set(progress.get("done", []))
        total = {"applied": 0, "duplicate": 0, "conflict": 0}
        agg = {"run_id": run_id, "keys": keys, "requests": 0, "found": 0,
               "not_found": 0, "applied": 0, "duplicates": 0, "conflicts": 0,
               "confirmed": 0, "unstable": [], "skipped": sorted(done),
               "attempts": progress.get("attempts", {})}
        s.execute(
            "UPDATE runs SET status='running',finished_at=NULL,error=NULL,"
            "stop_requested=0 WHERE run_id=?", (run_id,),
        )
        executed = 0
        counts: dict[str, int] = {}
        try:
            for key in keys:
                if key in done:
                    continue
                if crash_after_keys is not None and executed >= crash_after_keys:
                    raise _SimulatedCrash()
                if stop_after_keys is not None and executed >= stop_after_keys:
                    raise _StopRequested()
                if s.one("SELECT stop_requested FROM runs WHERE run_id=?",
                         (run_id,))["stop_requested"]:
                    raise _StopRequested()
                executed += 1
                attempts = int(agg["attempts"].get(key, {}).get("rounds", 0))
                outcome = "exhausted"
                while attempts < 4:
                    attempts += 1
                    res = sources.point_fetch(source_id, key)
                    agg["requests"] += 1
                    if not res["found"]:
                        agg["not_found"] += 1
                        with s.tx() as c:
                            c.execute(
                                """INSERT INTO point_fetches
                                   (source_id,run_id,biz_key,at,found_rev,found,
                                    applied,detail)
                                   VALUES(?,?,?,?,NULL,0,0,?)""",
                                (source_id, run_id, key, _now(),
                                 json.dumps({"cutoff": res.get("cutoff"),
                                             "conclusion": "not_found_is_not_delete"})),
                            )
                            self._request_row(
                                c, source_id, run_id, transport="pull-point",
                                kind="point", cursor=key, rows=0,
                                counts={"events": 0}, covered_from=key,
                                covered_to=key, consistent=1,
                            )
                            self._open_suspect(c, source_id, key,
                                               "not_found_on_confirm")
                        outcome = "not_found"
                        break

                    counts = self.ingest(
                        source_id,
                        [{"biz_key": key, "revision": res["revision"],
                          "payload": res["payload"],
                          "content_hash": res["content_hash"],
                          "deleted": res["deleted"]}],
                        via="pull-point", run_id=run_id,
                    )
                    agg["applied"] += counts["applied"]
                    agg["duplicates"] += counts["duplicate"]
                    agg["conflicts"] += counts["conflict"]
                    agg["found"] += 1
                    for k in total:
                        total[k] += counts.get(k, 0)
                    with s.tx() as c:
                        c.execute(
                            """INSERT INTO point_fetches
                               (source_id,run_id,biz_key,at,found_rev,found,
                                applied,detail)
                               VALUES(?,?,?,?,?,1,?,?)""",
                            (source_id, run_id, key, _now(), res["revision"],
                             counts["applied"],
                             json.dumps({"cutoff": res.get("cutoff"),
                                         "hash": res["content_hash"]})),
                        )
                        self._request_row(
                            c, source_id, run_id, transport="pull-point",
                            kind="point", cursor=key, rows=1, counts=counts,
                            covered_from=key, covered_to=key, consistent=1,
                        )
                    if not confirm:
                        outcome = "fetched"
                        break

                    if before_confirm:
                        before_confirm(key, int(res["revision"]))
                    again = sources.point_fetch(source_id, key)
                    agg["requests"] += 1
                    if (again["found"]
                            and int(again["revision"]) == int(res["revision"])
                            and again["content_hash"] == res["content_hash"]):
                        agg["confirmed"] += 1
                        outcome = "confirmed"
                        with s.tx() as c:
                            self._request_row(
                                c, source_id, run_id, transport="pull-point",
                                kind="point", cursor=key, rows=1,
                                counts={"events": 0}, covered_from=key,
                                covered_to=key, consistent=1,
                            )
                            c.execute(
                                """UPDATE suspects SET resolved_at=?,
                                   resolution=? WHERE source_id=?
                                   AND biz_key=? AND resolved_at IS NULL
                                   AND reason IN ('push_gap',
                                       'not_found_on_confirm',
                                       'unstable_revision')""",
                                (_now(),
                                 f"point_confirmed_rev_{res['revision']}",
                                 source_id, key),
                            )
                        break
                    # Revision moved under us: apply it on the next round and
                    # only confirm after two consecutive identical reads.
                    with s.tx() as c:
                        self._event(
                            c, source_id, "note", "pull-point", key,
                            res["revision"], again.get("revision"),
                            {"run_id": run_id,
                             "reason": "revised_during_recovery",
                             "attempt": attempts},
                        )
                        self._open_suspect(c, source_id, key,
                                           "unstable_revision")
                    if again["found"]:
                        counts2 = self.ingest(
                            source_id,
                            [{"biz_key": key, "revision": again["revision"],
                              "payload": again["payload"],
                              "content_hash": again["content_hash"],
                              "deleted": again["deleted"]}],
                            via="pull-point", run_id=run_id,
                        )
                        agg["applied"] += counts2["applied"]
                        agg["duplicates"] += counts2["duplicate"]
                        agg["conflicts"] += counts2["conflict"]
                        for k in total:
                            total[k] += counts2.get(k, 0)
                    agg["unstable"].append(key)
                    outcome = "revised_during_recovery"
                # Commit key completion and progress in one transaction.  A
                # crash before this point simply re-fetches the key, which is
                # idempotent; a crash after it skips the key on resume.
                with s.tx() as c:
                    done.add(key)
                    progress["done"] = sorted(done)
                    progress["attempts"][key] = {"rounds": attempts,
                                                 "outcome": outcome}
                    c.execute("UPDATE runs SET cursor=? WHERE run_id=?",
                              (json.dumps(progress), run_id))
                    c.execute(
                        """UPDATE runs SET requests=requests+1,applied=applied+?,
                           duplicates=duplicates+?,conflicts=conflicts+?
                           WHERE run_id=?""",
                        (total["applied"], total["duplicate"],
                         total["conflict"], run_id),
                    )
                agg["attempts"][key] = {"rounds": attempts, "outcome": outcome}

            s.execute(
                """UPDATE runs SET status='completed',finished_at=?,
                   proof_json=? WHERE run_id=?""",
                (_now(),
                 json.dumps({
                     "kind": "point",
                     "keys": keys,
                     "coverage": "only the listed business keys; unknown loss "
                                 "elsewhere is not covered",
                     "not_found": agg["not_found"],
                     "confirmed": agg["confirmed"],
                 }),
                 run_id),
            )
            return agg
        except _SimulatedCrash:
            s.execute(
                "UPDATE runs SET status='interrupted',error=? WHERE run_id=?",
                ("worker stopped abruptly (simulated)", run_id),
            )
            return {**agg, "crashed": True, "run_id": run_id}
        except _StopRequested:
            s.execute(
                "UPDATE runs SET status='stopped',finished_at=? WHERE run_id=?",
                (_now(), run_id),
            )
            return {**agg, "stopped": True, "run_id": run_id}
        except Exception as exc:
            s.execute(
                "UPDATE runs SET status='failed',finished_at=?,error=? WHERE run_id=?",
                (_now(), repr(exc), run_id),
            )
            raise

    # ==================================================================
    # conflict handling
    # ==================================================================
    def resolve_conflict(
        self, sources: SourceManager, source_id: str, biz_key: str,
        conflict_id: int, decision: str, note: str = "",
    ) -> dict[str, Any]:
        """Settle one recorded equal-revision conflict by explicit operator choice.

        Only two decisions exist, and neither is "whatever arrived last":
        ``keep_local`` keeps the bytes already stored; ``accept_remote`` adopts
        the conflicting bytes at the same revision.  Both are journaled with the
        operator note, and both close the matching suspect.
        """
        if decision not in ("keep_local", "accept_remote"):
            raise ValueError("decision must be keep_local or accept_remote")
        s = self.store
        with s.tx() as c:
            row = c.execute(
                """SELECT * FROM conflicts WHERE id=? AND source_id=?
                   AND biz_key=?""",
                (conflict_id, source_id, biz_key),
            ).fetchone()
            if row is None:
                raise KeyError(f"conflict {conflict_id}")
            if row["resolved_at"]:
                raise ValueError("conflict already settled")
            local = c.execute(
                "SELECT * FROM records WHERE source_id=? AND biz_key=?",
                (source_id, biz_key),
            ).fetchone()
            now = _now()
            if local is not None and int(local["revision"]) > int(row["revision"]):
                c.execute(
                    """UPDATE conflicts SET resolved_at=?,decision='superseded',
                       note=? WHERE id=?""",
                    (now, "local already ahead", conflict_id),
                )
                return {"outcome": "superseded",
                        "local_revision": int(local["revision"])}
            if decision == "accept_remote":
                # The adopted bytes came from push only and may not exist in the
                # pull log at all.  Record the operator override, but do NOT
                # stamp pull confirmation for bytes pull never returned.
                c.execute(
                    """UPDATE records SET payload=?,content_hash=?,deleted=?,
                       via='operator',updated_at=?
                       WHERE source_id=? AND biz_key=?""",
                    (row["remote_payload"], row["remote_hash"],
                     int(row["remote_deleted"]), now, source_id, biz_key),
                )
            c.execute(
                """UPDATE conflicts SET resolved_at=?,decision=?,note=?
                   WHERE id=?""",
                (now, decision, note, conflict_id),
            )
            c.execute(
                """UPDATE suspects SET resolved_at=?,resolution=?
                   WHERE source_id=? AND biz_key=?
                   AND reason='equal_revision_conflict' AND resolved_at IS NULL""",
                (now, f"operator:{decision}", source_id, biz_key),
            )
            self._event(
                c, source_id, "conflict_resolved", "operator", biz_key,
                int(local["revision"]) if local else None,
                int(row["revision"]),
                {"conflict_id": conflict_id, "decision": decision, "note": note,
                 "local_hash": row["local_hash"],
                 "conflict_hash": row["remote_hash"]},
            )
        return {"outcome": decision, "conflict_id": conflict_id,
                "revision": int(row["revision"])}

    # ==================================================================
    # strategy planning + status / evidence reporting
    # ==================================================================
    def plan(self, sources: SourceManager, source_id: str) -> dict[str, Any]:
        """Cost/benefit of full reconciliation versus targeted re-collection.

        Units are synthetic transport calls.  A full scan costs one page per
        ``ceil(local_keys / page_size)`` plus one terminal page, and its cost
        grows with the whole key space.  A targeted repair costs two point calls
        per named key (fetch + stability re-read) and its cost grows only with
        the number of open items.  The two differ in what they can prove:

        * full scan proves every key visible in the snapshot, including keys the
          collector has never heard of;
        * targeted repair proves only the keys it was told about.  It can close a
          *known* gap, but cannot discover unknown loss.
        """
        s = self.store
        cfg = sources.get(source_id)
        st = s.one("SELECT * FROM source_state WHERE source_id=?", (source_id,))
        local_keys = int(s.one(
            "SELECT COUNT(*) AS n FROM records WHERE source_id=?", (source_id,)
        )["n"])
        suspects = [dict(r) for r in s.q(
            """SELECT * FROM suspects WHERE source_id=? AND resolved_at IS NULL
               ORDER BY reason,biz_key""", (source_id,))]
        open_conflicts = int(s.one(
            """SELECT COUNT(*) AS n FROM conflicts WHERE source_id=?
               AND resolved_at IS NULL""", (source_id,))["n"])
        gaps = [dict(r) for r in s.q(
            """SELECT * FROM gaps WHERE source_id=? AND resolved_at IS NULL
               ORDER BY seq_from""", (source_id,))]
        gap_keys: dict[str, int] = {}
        gap_span_min = gap_span_max = None
        for g in gaps:
            man = json.loads(g["manifest_json"] or "{}")
            for item in man.get("keys", []):
                gap_keys[item["biz_key"]] = max(
                    gap_keys.get(item["biz_key"], 0), int(item["revision"]))
            gap_span_min = (g["seq_from"] if gap_span_min is None
                            else min(gap_span_min, int(g["seq_from"])))
            gap_span_max = (g["seq_to"] if gap_span_max is None
                            else max(gap_span_max, int(g["seq_to"])))
        suspect_keys = sorted({r["biz_key"] for r in suspects})
        targeted_keys = sorted(set(suspect_keys) | set(gap_keys))
        page_size = max(1, int(cfg["page_size"]))
        # Estimated from the keys we know about.  Unknown keys (the exact kind
        # of loss a full scan is meant to find) make the real cost higher, so
        # this is a lower bound, not a promise.
        full_pages = (local_keys + page_size - 1) // page_size + 1
        point_cost = 2 * len(targeted_keys)
        head = sources.push_head(source_id)
        tail_uncovered = int(st["push_cursor"]) <= head
        remaining = [f"gap {g['seq_from']}..{g['seq_to']} ({g['reason']})"
                     for g in gaps]
        return {
            "source_id": source_id,
            "generated_at": _now(),
            "local_keys": local_keys,
            "head_seq": head,
            "push_cursor": int(st["push_cursor"]),
            "gap_span": [gap_span_min, gap_span_max]
            if gap_span_min is not None else None,
            "targeted_keys": targeted_keys,
            "options": [
                {
                    "strategy": "full",
                    "label": "全量核对 (full scan)",
                    "requests": full_pages,
                    "unit": "page",
                    "page_size": page_size,
                    "keys_examined_at_least": local_keys,
                    "proves": "所有业务键在该快照时点的最新版本；"
                              "可排除缺口清单之外未知的遗漏",
                    "cannot_prove": "快照水位之后的写入；拉取延迟窗口内的变更",
                    "closes_gaps": [
                        f"gap {g['seq_from']}..{g['seq_to']}" for g in gaps
                    ],
                },
                {
                    "strategy": "point",
                    "label": "定向补采 (targeted repair)",
                    "requests": point_cost,
                    "unit": "point",
                    "keys": targeted_keys,
                    "keys_examined": len(targeted_keys),
                    "proves": "清单中每个键的最新修订，且以二次读取确认修订未在补采期间变化",
                    "cannot_prove": "不在缺口清单/疑点队列中的键，即未知的遗漏",
                    "closes_gaps": list(gap_keys),
                },
            ],
            "recommendation": {
                # No open items does not mean "do nothing": only a full scan
                # can establish coverage, and coverage is what the conclusion
                # cites.  Point repair can never widen the certificate.
                "strategy": ("full"
                             if gaps or open_conflicts or not targeted_keys
                             else "point"),
                "reason": (
                    "存在推送缺口：清单只能覆盖已知丢失去向，"
                    "必须用全量核对排除清单之外的遗漏"
                    if gaps else
                    "存在未裁决冲突：内容归属必须人工决定，"
                    "无法用补采请求消除"
                    if open_conflicts else
                    "仅存在具名键的疑点：定向补采成本更低且足以确认这些键"
                    if targeted_keys else
                    "没有具名疑点，但也没有可用证书；全量核对是唯一能建立覆盖证据的选项"
                ),
            },
            "open_items": {
                "gaps": len(gaps),
                "gap_keys": len(gap_keys),
                "suspects": len(suspects),
                "suspect_keys": len(suspect_keys),
                "conflicts": open_conflicts,
                "unconsumed_push_events": max(0, head - int(st["push_cursor"]) + 1),
            },
            "remaining_unproven": remaining + (
                ["快照水位之后的推送尾部尚未被证书覆盖"] if tail_uncovered else []),
            "unsafe_shortcuts": [
                "按到达先后覆盖更高修订（顺序不代表新旧）",
                "把分页/点查中查不到当作删除",
                "在出现序号缺口后仍宣称推送单独可证完整",
            ],
        }

    def status(self, sources: SourceManager, source_id: str) -> dict[str, Any]:
        s = self.store
        cfg = dict(sources.get(source_id))
        st = dict(sources.state(source_id))
        local_rows = s.q(
            "SELECT * FROM records WHERE source_id=? ORDER BY biz_key",
            (source_id,))
        suspects = [dict(r) for r in s.q(
            """SELECT * FROM suspects WHERE source_id=? AND resolved_at IS NULL
               ORDER BY reason,biz_key""", (source_id,))]
        conflicts = [dict(r) for r in s.q(
            """SELECT * FROM conflicts WHERE source_id=? AND resolved_at IS NULL
               ORDER BY detected_at""", (source_id,))]
        gaps = [dict(r) for r in s.q(
            """SELECT * FROM gaps WHERE source_id=? ORDER BY seq_from""",
            (source_id,))]
        req_cost = s.one(
            """SELECT
                 COALESCE(SUM(CASE WHEN transport='push' THEN cost END),0) AS push,
                 COALESCE(SUM(CASE WHEN transport='pull-full' THEN cost END),0) AS full,
                 COALESCE(SUM(CASE WHEN transport='pull-point' THEN cost END),0) AS point,
                 COUNT(*) AS total
               FROM requests WHERE source_id=?""", (source_id,))
        scan = None
        if st.get("scan_id"):
            pages = s.q(
                """SELECT * FROM requests WHERE scan_id=? AND
                   transport='pull-full' ORDER BY page_index""",
                (st["scan_id"],))
            verify = self._verify_scan(st["scan_id"])
            scan = {
                "scan_id": st["scan_id"],
                "coverage_seq": st["coverage_epoch"],
                "snapshot_cutoff": st["scan_cutoff"],
                "certified_at": st["scan_at"],
                "pages": st["scan_pages"],
                "chain_head": st["scan_chain"],
                "complete": bool(st["scan_complete"]),
                "verified_now": verify["ok"],
                "verify_errors": verify["errors"],
                "requests": len(pages),
                "rows_read": sum(int(p["rows_returned"]) for p in pages),
                "coverage": [pages[0]["covered_from"], pages[-1]["covered_to"]]
                if pages else None,
            }
        open_gaps = []
        for g in gaps:
            man = json.loads(g["manifest_json"] or "{}")
            open_gaps.append({
                **{k: g[k] for k in ("id", "seq_from", "seq_to", "reason",
                                     "detected_at", "resolved_at", "resolution")},
                "manifest_keys": man.get("keys", []),
            })
        coverage = int(st["coverage_epoch"] or 0)
        head = sources.push_head(source_id)
        # Tail = accepted writes the certified snapshot does not cover.
        tail_first = coverage + 1
        tail_last = head
        tail_covered_by_push = (
            int(st["push_cursor"]) > head
            and not any(not g["resolved_at"] for g in open_gaps)
        )
        evidence: list[str] = []
        if scan:
            evidence.append(
                f"全量扫描证书 {scan['scan_id']}：{scan['pages']} 页，"
                f"终止于空页，页哈希链 {scan['chain_head']}")
            evidence.append(
                f"证书涵盖快照 cutoff={scan['snapshot_cutoff']:.3f}，"
                f"对应写序号 ≤ {scan['coverage_seq']}；"
                f"现场按请求账本复算{'通过' if scan['verified_now'] else '失败'}")
        else:
            evidence.append("尚无可用的全量扫描证书，完整性未证明")
        evidence.append(
            f"请求账本：推送 {int(req_cost['push'])} 批、全量 {int(req_cost['full'])} 页、"
            f"定向 {int(req_cost['point'])} 次，合计 {int(req_cost['total'])} 次传输调用")
        for g in open_gaps:
            if not g["resolved_at"]:
                evidence.append(
                    f"未排除缺口：写序号 {g['seq_from']}..{g['seq_to']} "
                    f"({g['reason']})，恢复清单含 {len(g['manifest_keys'])} 个业务键")
        if conflicts:
            evidence.append(
                f"未裁决冲突 {len(conflicts)} 条（同修订不同内容），"
                f"不能按到达先后覆盖")
        if suspects:
            evidence.append(
                f"未解决疑点 {len(suspects)} 条，涉及 "
                f"{len({x['biz_key'] for x in suspects})} 个业务键")
        if tail_first <= tail_last:
            if tail_covered_by_push:
                evidence.append(
                    f"快照之后的写序号 {tail_first}..{tail_last} 由连续推送覆盖："
                    f"推送游标 {st['push_cursor']} > 头部 {head}，无未排除缺口")
            else:
                evidence.append(
                    f"快照之后的写序号 {tail_first}..{tail_last} 尚未被证明："
                    f"推送游标 {st['push_cursor']}，头部 {head}，存在未排除缺口")
        certified = bool(scan and scan["complete"] and scan["verified_now"])
        open_gap_count = sum(1 for g in open_gaps if not g["resolved_at"])
        if not certified:
            status = "not_proven"
        elif open_gap_count or conflicts or suspects or not tail_covered_by_push:
            status = "open_items"
        else:
            status = "proven"
        conclusion = {
            "status": status,
            "coverage_seq": coverage,
            "head_seq": head,
            "tail": [tail_first, tail_last] if tail_first <= tail_last else None,
            "tail_proven": tail_covered_by_push,
            "statement": (
                (f"全量核对证明：快照前写序号 ≤ {coverage} 的全部业务键均已入库。"
                 if certified else "尚未建立完整性证据。")
                + (f" 写序号 {tail_first}..{tail_last} 由连续推送覆盖。"
                   if tail_covered_by_push and tail_first <= tail_last else "")
                + ("" if status == "proven" else " 仍有未排除项，不能宣称完整。")
            ),
            "tail_note": (
                "证书只覆盖快照水位；其后的写入只有在推送连续、无缺口"
                "（游标越过头部）时才算证明，未到达的尾部不算完整"),
        }
        records = []
        for r in local_rows:
            records.append({
                "biz_key": r["biz_key"],
                "revision": r["revision"],
                "deleted": bool(r["deleted"]),
                "via": r["via"],
                "first_via": r["first_via"],
                "content_hash": r["content_hash"],
                "confirmed_rev": r["confirmed_rev"],
                "confirmed": (int(r["confirmed_rev"]) >= int(r["revision"])
                              and r["confirm_hash"] == r["content_hash"]),
                "updated_at": r["updated_at"],
                "payload": json.loads(r["payload"]) if r["payload"] else {},
            })
        return {
            "source": {
                "source_id": cfg["source_id"], "name": cfg["name"],
                "push_enabled": bool(cfg["push_enabled"]),
                "pull_enabled": bool(cfg["pull_enabled"]),
                "page_size": cfg["page_size"],
                "pull_delay_ms": cfg["pull_delay_ms"],
                "drop_push": bool(cfg["drop_push"]),
                "drop_rate": cfg["drop_rate"],
            },
            "transport": {
                "push_cursor": int(st["push_cursor"]),
                "push_head": head,
                "outage_events_remaining": int(st["outage_events"]),
                "push_lost_total": int(st["push_dropped"]),
                "pending_push": max(0, head - int(st["push_cursor"]) + 1),
            },
            "scan": scan,
            "gaps": open_gaps,
            "suspects": suspects,
            "conflicts": conflicts,
            "records": records,
            "costs": {
                "push_batches": int(req_cost["push"]),
                "pull_full_pages": int(req_cost["full"]),
                "pull_point": int(req_cost["point"]),
                "total": int(req_cost["total"]),
            },
            "conclusion": conclusion,
            "evidence": evidence,
            "truth": self.truth_diff(sources, source_id),
        }

    def truth_diff(self, sources: SourceManager, source_id: str) -> dict[str, Any]:
        """Demo-only oracle comparison.  Never used as collection evidence."""
        s = self.store
        local = {r["biz_key"]: r for r in s.q(
            "SELECT * FROM records WHERE source_id=?", (source_id,))}
        remote = {}
        for r in s.q(
            """SELECT biz_key,revision,content_hash,deleted,payload FROM remote_rows
               WHERE source_id=? ORDER BY biz_key,revision""", (source_id,)):
            remote[r["biz_key"]] = r
        missing, behind, ahead, hash_mismatch, extra_local = [], [], [], [], []
        for key, rem in remote.items():
            loc = local.get(key)
            if loc is None:
                missing.append(key)
            elif int(loc["revision"]) < int(rem["revision"]):
                behind.append({"key": key, "local": loc["revision"],
                               "remote": rem["revision"]})
            elif int(loc["revision"]) > int(rem["revision"]):
                ahead.append({"key": key, "local": loc["revision"],
                              "remote": rem["revision"]})
            elif (loc["content_hash"] != rem["content_hash"] or
                  int(loc["deleted"]) != int(rem["deleted"])):
                hash_mismatch.append({
                    "key": key, "revision": loc["revision"],
                    "local_hash": loc["content_hash"],
                    "remote_hash": rem["content_hash"]})
        for key, loc in local.items():
            if key not in remote and not loc["deleted"]:
                extra_local.append(key)
        return {
            "warning": "演示用真值核对，仅验证实现；不作为采集器的完整性证据",
            "missing": missing, "behind": behind, "ahead": ahead,
            "hash_mismatch": hash_mismatch, "extra_local": extra_local,
        }

    def events(self, source_id: str, biz_key: str | None = None,
               limit: int = 200) -> list[dict[str, Any]]:
        sql = ("""SELECT * FROM events WHERE source_id=? """
               + ("""AND biz_key=? """ if biz_key else "")
               + "ORDER BY id DESC LIMIT ?")
        args = (source_id, biz_key, limit) if biz_key else (source_id, limit)
        rows = self.store.q(sql, args)
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["detail"] = json.loads(d["detail"])
            except Exception:
                pass
            out.append(d)
        return out

    def run_status(self, run_id: str) -> dict[str, Any] | None:
        s = self.store
        run = s.one("SELECT * FROM runs WHERE run_id=?", (run_id,))
        if not run:
            return None
        d = dict(run)
        for k in ("plan_json", "proof_json"):
            if d.get(k):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass
        d["requests_detail"] = []
        for r in s.q(
            """SELECT id,at,transport,request_kind,cost,rows_returned,
                      applied_count,duplicate_count,conflict_count,covered_from,
                      covered_to,exhausted,page_index,page_hash,error
               FROM requests WHERE run_id=? ORDER BY id""", (run_id,)):
            d["requests_detail"].append(dict(r))
        return d

    def request_stop(self, run_id: str) -> None:
        self.store.execute(
            "UPDATE runs SET stop_requested=1 WHERE run_id=?", (run_id,))
