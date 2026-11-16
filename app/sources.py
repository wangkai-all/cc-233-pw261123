"""Synthetic upstream sources with two independent transports.

The same append-only revision log is exposed through:

* **push** — a contiguous sequence stream.  It is fast, but the synthetic
  upstream can mark a span of events as lost (outage) or randomly lose a
  fraction of events (unreliable network).  The collector only learns about a
  loss when a later event exposes a jump in the sequence, so a lost tail is
  inherently unprovable from push alone.
* **pull** — keyset pagination over business keys with a simulated replication
  delay.  It is eventually complete, but the snapshot used by a scan may stop
  short of the newest accepted writes.

Everything is invented local data.  The class deliberately exposes only the
operations a real client would have: snapshot reads, point lookups, and a
small recovery manifest of business keys + revisions for a sequence range.
"""

from __future__ import annotations

import json
import random
import time
from typing import Any, Iterator

from .db import Store, content_hash


class PullUnavailable(RuntimeError):
    pass


class SourceManager:
    def __init__(self, store: Store):
        self.store = store
        self._rng = random.Random(0xC011EC7)

    # ------------------------------------------------------------------
    # lifecycle / configuration
    # ------------------------------------------------------------------
    def create(
        self,
        source_id: str,
        name: str,
        *,
        page_size: int = 3,
        pull_delay_ms: int = 0,
        drop_push: bool = False,
        drop_rate: int = 0,
    ) -> None:
        s = self.store
        with s.tx() as c:
            c.execute(
                """INSERT OR IGNORE INTO sources
                   (source_id,name,push_enabled,pull_enabled,drop_push,drop_rate,
                    page_size,pull_delay_ms,created_at)
                   VALUES(?,?,1,1,?,?,?,?,?)""",
                (source_id, name, int(drop_push), int(drop_rate), int(page_size),
                 int(pull_delay_ms), time.time()),
            )
            c.execute(
                "INSERT OR IGNORE INTO source_state(source_id) VALUES(?)",
                (source_id,),
            )

    def update(self, source_id: str, **fields: Any) -> None:
        allowed = {
            "name", "push_enabled", "pull_enabled", "drop_push", "drop_rate",
            "page_size", "pull_delay_ms",
        }
        assignments, args = [], []
        for k, v in fields.items():
            if k not in allowed:
                continue
            assignments.append(f"{k}=?")
            args.append(int(v) if isinstance(v, bool) else v)
        if not assignments:
            return
        args.append(source_id)
        self.store.execute(
            f"UPDATE sources SET {', '.join(assignments)} WHERE source_id=?", args
        )

    def get(self, source_id: str):
        return self.store.require_source(source_id)

    def state(self, source_id: str):
        row = self.store.one(
            "SELECT * FROM source_state WHERE source_id=?", (source_id,)
        )
        if row is None:
            raise KeyError(source_id)
        return row

    # ------------------------------------------------------------------
    # synthetic writes
    # ------------------------------------------------------------------
    def write(
        self,
        source_id: str,
        biz_key: str,
        fields: dict[str, Any],
        *,
        deleted: bool = False,
        suppress_push: bool = False,
    ) -> dict[str, Any]:
        """Accept one upstream write and enqueue it on the push stream."""
        s = self.store
        cfg = s.require_source(source_id)
        now = time.time()
        payload = {} if deleted else {"key": biz_key, **fields}
        digest = content_hash(payload)
        with s.tx() as c:
            st = c.execute(
                "SELECT next_seq,next_push_seq,outage_events FROM source_state "
                "WHERE source_id=?", (source_id,),
            ).fetchone()
            seq = int(st["next_seq"])
            push_seq = int(st["next_push_seq"])
            rev_row = c.execute(
                "SELECT COALESCE(MAX(revision),0)+1 AS r FROM remote_rows WHERE source_id=?",
                (source_id,),
            ).fetchone()
            revision = int(rev_row["r"])
            visible_at = now + int(cfg["pull_delay_ms"]) / 1000.0
            prev = c.execute(
                "SELECT MAX(visible_at) AS v FROM remote_rows WHERE source_id=?",
                (source_id,),
            ).fetchone()
            if prev and prev["v"] is not None:
                visible_at = max(visible_at, float(prev["v"]))
            c.execute(
                """INSERT INTO remote_rows
                   (source_id,seq,revision,biz_key,payload,content_hash,deleted,
                    accepted_at,visible_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (source_id, seq, revision, biz_key,
                 json.dumps(payload, ensure_ascii=False), digest,
                 1 if deleted else 0, now, visible_at),
            )
            lost = 0
            if suppress_push:
                lost = 1
            elif int(st["outage_events"]) > 0:
                lost = 1
                c.execute(
                    "UPDATE source_state SET outage_events=outage_events-1 WHERE source_id=?",
                    (source_id,),
                )
            elif cfg["drop_push"] and self._rng.randrange(100) < int(cfg["drop_rate"]):
                lost = 1
            c.execute(
                """INSERT INTO push_events
                   (source_id,seq,remote_seq,biz_key,revision,payload,content_hash,
                    deleted,lost,enqueued_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (source_id, push_seq, seq, biz_key, revision,
                 json.dumps(payload, ensure_ascii=False), digest,
                 1 if deleted else 0, lost, now),
            )
            c.execute(
                """UPDATE source_state SET next_seq=?,next_push_seq=?
                   WHERE source_id=?""",
                (seq + 1, push_seq + 1, source_id),
            )
        return {
            "seq": seq,
            "revision": revision,
            "biz_key": biz_key,
            "payload": payload,
            "content_hash": digest,
            "deleted": deleted,
            "push_lost": bool(lost),
            "push_suppressed": bool(suppress_push),
            "accepted_at": now,
            "visible_at": visible_at,
        }

    def push_head(self, source_id: str) -> int:
        """Highest assignable push sequence (delivered, lost, or pending).

        This is the upstream's watermark, not merely "the last event I stored":
        a lost event still consumes a sequence number, which is exactly what
        makes a vanished tail observable.
        """
        row = self.store.one(
            """SELECT MAX(COALESCE(MAX(p.seq),0), MAX(s.next_push_seq)-1) AS h
               FROM source_state s LEFT JOIN push_events p
                 ON p.source_id=s.source_id
               WHERE s.source_id=?""",
            (source_id,),
        )
        return int(row["h"] or 0)

    def set_outage(self, source_id: str, count: int) -> None:
        """Make the next ``count`` push events vanish upstream."""
        self.store.execute(
            "UPDATE source_state SET outage_events=? WHERE source_id=?",
            (max(0, int(count)), source_id),
        )

    def inject_conflict(self, source_id: str, biz_key: str) -> dict[str, Any]:
        """Push a rogue event with an existing revision but different bytes.

        This never touches the remote revision log: a later pull cannot
        reproduce it, so the conflict can only be settled by an explicit
        operator decision, never by arrival order.
        """
        s = self.store
        local = s.record(source_id, biz_key)
        if local is None:
            raise ValueError("record not found")
        payload = json.loads(local["payload"]) if local["payload"] else {}
        payload = {**payload, "__fork__": f"rogue-{int(time.time() * 1000) % 100000}"}
        digest = content_hash(payload)
        with s.tx() as c:
            st = c.execute(
                "SELECT next_push_seq FROM source_state WHERE source_id=?",
                (source_id,),
            ).fetchone()
            push_seq = int(st["next_push_seq"])
            c.execute(
                """INSERT INTO push_events
                   (source_id,seq,remote_seq,biz_key,revision,payload,content_hash,
                    deleted,lost,enqueued_at)
                   VALUES(?,?,NULL,?,?,?,?,0,0,?)""",
                (source_id, push_seq, biz_key, int(local["revision"]),
                 json.dumps(payload, ensure_ascii=False), digest, time.time()),
            )
            c.execute(
                "UPDATE source_state SET next_push_seq=? WHERE source_id=?",
                (push_seq + 1, source_id),
            )
        return {"seq": push_seq, "revision": int(local["revision"]),
                "content_hash": digest}

    # ------------------------------------------------------------------
    # push transport
    # ------------------------------------------------------------------
    def iter_push(
        self,
        source_id: str,
        *,
        cursor: int | None = None,
        batch_size: int = 4,
        max_batches: int = 500,
    ) -> Iterator[dict[str, Any]]:
        """Yield contiguous runs of non-lost push events.

        A run stops at the first sequence discontinuity, so the collector can
        see exactly where the stream jumped.
        """
        s = self.store
        cfg = s.require_source(source_id)
        if not cfg["push_enabled"]:
            return
        if cursor is None:
            cursor = int(self.state(source_id)["push_cursor"])
        batch_no = 0
        while batch_no < max_batches:
            rows = s.q(
                """SELECT * FROM push_events
                   WHERE source_id=? AND seq>=? AND lost=0
                   ORDER BY seq LIMIT ?""",
                (source_id, cursor, 64),
            )
            if not rows:
                return
            run = [rows[0]]
            for r in rows[1:]:
                if r["seq"] != run[-1]["seq"] + 1 or len(run) >= batch_size:
                    break
                run.append(r)
            batch_no += 1
            yield {
                "cursor_before": cursor,
                "cursor_after": int(run[-1]["seq"]),
                "events": [self._event_from_row(r) for r in run],
            }
            cursor = int(run[-1]["seq"]) + 1

    @staticmethod
    def _event_from_row(r) -> dict[str, Any]:
        return {
            "seq": int(r["seq"]),
            "biz_key": r["biz_key"],
            "revision": int(r["revision"]),
            "payload": json.loads(r["payload"]),
            "content_hash": r["content_hash"],
            "deleted": bool(r["deleted"]),
        }

    # ------------------------------------------------------------------
    # pull transport
    # ------------------------------------------------------------------
    def snapshot_at(self, source_id: str, cutoff: float | None = None) -> dict[str, Any]:
        """Return the frozen snapshot descriptor for a pull read."""
        cutoff = cutoff if cutoff is not None else time.time()
        row = self.store.one(
            "SELECT MIN(seq) AS first_invisible FROM remote_rows "
            "WHERE source_id=? AND visible_at>?",
            (source_id, cutoff),
        )
        max_row = self.store.one(
            "SELECT COALESCE(MAX(seq),0) AS head FROM remote_rows WHERE source_id=?",
            (source_id,),
        )
        if row and row["first_invisible"] is not None:
            watermark = int(row["first_invisible"]) - 1
        else:
            watermark = int(max_row["head"])
        return {"cutoff": cutoff, "watermark": watermark,
                "head": int(max_row["head"])}

    def iter_pull(
        self,
        source_id: str,
        *,
        cutoff: float,
        after_key: str | None = None,
        page_size: int | None = None,
        max_pages: int = 100_000,
    ) -> Iterator[dict[str, Any]]:
        """Keyset pagination over the newest revision visible at ``cutoff``."""
        s = self.store
        cfg = s.require_source(source_id)
        if not cfg["pull_enabled"]:
            raise PullUnavailable("pull transport disabled")
        snapshot = self.snapshot_at(source_id, cutoff)
        size = int(page_size or cfg["page_size"])
        key = after_key or ""
        for page_no in range(1, max_pages + 1):
            rows = s.q(
                """SELECT r.* FROM remote_rows r
                   JOIN (SELECT biz_key, MAX(revision) AS mr
                         FROM remote_rows
                         WHERE source_id=? AND visible_at<=?
                         GROUP BY biz_key) m
                     ON m.biz_key=r.biz_key AND m.mr=r.revision
                   WHERE r.source_id=? AND r.biz_key>?
                   ORDER BY r.biz_key LIMIT ?""",
                (source_id, cutoff, source_id, key, size),
            )
            if not rows:
                yield {
                    "events": [],
                    "snapshot": snapshot,
                    "page_no": page_no,
                    "covered_from": key or None,
                    "covered_to": None,
                    "next_cursor": None,
                    "exhausted": True,
                }
                return
            events = [self._event_from_row(r) for r in rows]
            last_key = rows[-1]["biz_key"]
            exhausted = len(rows) < size
            yield {
                "events": events,
                "snapshot": snapshot,
                "page_no": page_no,
                "covered_from": key or None,
                "covered_to": last_key,
                "next_cursor": None if exhausted else last_key,
                "exhausted": exhausted,
            }
            if exhausted:
                return
            key = last_key

    def point_fetch(self, source_id: str, biz_key: str) -> dict[str, Any]:
        """Exact-key lookup.  Absence is reported, never treated as deletion."""
        cfg = self.store.require_source(source_id)
        if not cfg["pull_enabled"]:
            raise PullUnavailable("pull transport disabled")
        cutoff = time.time()
        snapshot = self.snapshot_at(source_id, cutoff)
        row = self.store.one(
            """SELECT * FROM remote_rows
               WHERE source_id=? AND biz_key=? AND visible_at<=?
               ORDER BY revision DESC LIMIT 1""",
            (source_id, biz_key, cutoff),
        )
        if not row:
            return {"found": False, "biz_key": biz_key, **snapshot}
        ev = self._event_from_row(row)
        ev.update({"found": True, **snapshot})
        return ev

    def gap_manifest(
        self, source_id: str, push_from: int, push_to: int,
        *, visible_at: float | None = None,
    ) -> dict[str, Any]:
        """Recovery manifest for a lost push range.

        Returns the span in *accepted-write* sequence space (metadata the
        upstream can always produce) and, for each business key, the newest
        revision it accepted in that range.  When ``visible_at`` is given, only
        writes already replicated to the pull transport are listed; keys still
        in flight stay in the gap for the next reconciliation pass.
        """
        span = self.store.one(
            """SELECT MIN(remote_seq) AS a, MAX(remote_seq) AS b
               FROM push_events WHERE source_id=? AND seq BETWEEN ? AND ?""",
            (source_id, push_from, push_to),
        )
        seq_from = int(span["a"]) if span and span["a"] is not None else None
        seq_to = int(span["b"]) if span and span["b"] is not None else None
        where_vis = "AND r.visible_at<=?" if visible_at is not None else ""
        args: list[Any] = [source_id, push_from, push_to]
        if visible_at is not None:
            args.append(visible_at)
        rows = self.store.q(
            f"""SELECT r.biz_key, MAX(r.revision) AS revision,
                       COUNT(*) AS writes
                FROM push_events p JOIN remote_rows r
                  ON r.source_id=p.source_id AND r.seq=p.remote_seq
                WHERE p.source_id=? AND p.seq BETWEEN ? AND ? {where_vis}
                GROUP BY r.biz_key ORDER BY r.biz_key""",
            args,
        )
        keys = [
            {"biz_key": r["biz_key"], "revision": int(r["revision"]),
             "writes": int(r["writes"])}
            for r in rows
        ]
        return {
            "keys": keys,
            "max_rev": {k["biz_key"]: k["revision"] for k in keys},
            "push_from": push_from,
            "push_to": push_to,
            "seq_from": seq_from,
            "seq_to": seq_to,
        }

    # ------------------------------------------------------------------
    # read-only helpers for the demo / truth panel
    # ------------------------------------------------------------------
    def latest_remote(self, source_id: str, biz_key: str) -> dict[str, Any] | None:
        row = self.store.one(
            """SELECT * FROM remote_rows WHERE source_id=? AND biz_key=?
               ORDER BY revision DESC LIMIT 1""",
            (source_id, biz_key),
        )
        if not row:
            return None
        ev = self._event_from_row(row)
        ev["accepted_at"] = row["accepted_at"]
        ev["visible_at"] = row["visible_at"]
        return ev
