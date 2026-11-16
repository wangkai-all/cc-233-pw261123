"""SQLite persistence for the collection demo.

Data model in one paragraph: a *source* is a synthetic upstream exposed through
two independent transports (push and keyset pull).  ``remote_rows`` is the
upstream truth as an append-only revision log with two timestamps: when the
write was accepted, and when it became visible to the pull transport.  Local
state lives in ``records`` keyed by business key.  Every request and every
decision that matters is journaled, so the completeness claim shown in the UI
can point at concrete evidence instead of a green checkmark.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import zlib
from contextlib import contextmanager
from typing import Any, Iterable


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sources (
    source_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    push_enabled    INTEGER NOT NULL DEFAULT 1,
    pull_enabled    INTEGER NOT NULL DEFAULT 1,
    -- optional chaos: randomly lose a whole push batch with this probability
    drop_push       INTEGER NOT NULL DEFAULT 0,
    drop_rate       INTEGER NOT NULL DEFAULT 0,     -- percent
    page_size       INTEGER NOT NULL DEFAULT 3,
    -- replication delay of the pull transport, in milliseconds
    pull_delay_ms   INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS source_state (
    source_id       TEXT PRIMARY KEY,
    next_seq        INTEGER NOT NULL DEFAULT 1,     -- next accepted write seq
    next_push_seq   INTEGER NOT NULL DEFAULT 1,     -- next push-stream seq
    push_cursor     INTEGER NOT NULL DEFAULT 1,     -- next push seq to consume
    outage_events   INTEGER NOT NULL DEFAULT 0,     -- next N push events vanish
    full_scan_runs  INTEGER NOT NULL DEFAULT 0,
    full_scan_reqs  INTEGER NOT NULL DEFAULT 0,
    point_reqs      INTEGER NOT NULL DEFAULT 0,
    push_batches    INTEGER NOT NULL DEFAULT 0,
    push_dropped    INTEGER NOT NULL DEFAULT 0,
    push_events     INTEGER NOT NULL DEFAULT 0,
    -- certificate of the last finished full scan (evidence watermark)
    scan_id         TEXT,
    coverage_epoch  INTEGER,        -- highest accepted write seq the scan covered
    scan_cutoff     REAL,           -- snapshot time of that scan
    scan_at         REAL,
    scan_chain      TEXT,
    scan_pages      INTEGER,
    scan_complete   INTEGER,
    FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
);

-- Append-only upstream revision log.  ``seq`` is the accepted-write sequence
-- used by coverage watermarks; ``revision`` is the business revision that the
-- collector orders by; ``visible_at`` models replication lag for pull.
CREATE TABLE IF NOT EXISTS remote_rows (
    source_id       TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    revision        INTEGER NOT NULL,
    biz_key         TEXT NOT NULL,
    payload         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    deleted         INTEGER NOT NULL DEFAULT 0,
    accepted_at     REAL NOT NULL,
    visible_at      REAL NOT NULL,
    PRIMARY KEY (source_id, seq),
    UNIQUE (source_id, revision),
    FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_remote_key ON remote_rows(source_id, biz_key, revision);
CREATE INDEX IF NOT EXISTS idx_remote_vis ON remote_rows(source_id, visible_at);
CREATE INDEX IF NOT EXISTS idx_remote_acc ON remote_rows(source_id, accepted_at);

-- Push stream.  push seq order is delivery order; remote_seq maps an event back
-- to the accepted write it came from (both are 1:1 in normal operation).
CREATE TABLE IF NOT EXISTS push_events (
    source_id       TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    remote_seq      INTEGER,            -- accepted-write seq this event maps to
    biz_key         TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    payload         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    deleted         INTEGER NOT NULL DEFAULT 0,
    lost            INTEGER NOT NULL DEFAULT 0,
    enqueued_at     REAL NOT NULL,
    PRIMARY KEY (source_id, seq),
    FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
);

-- Losses the push transport reported (or that the collector inferred from a
-- sequence jump).  Mapped into accepted-write space via remote_seq.
CREATE TABLE IF NOT EXISTS gaps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    push_from       INTEGER NOT NULL,
    push_to         INTEGER NOT NULL,
    seq_from        INTEGER,
    seq_to          INTEGER,
    reason          TEXT NOT NULL,
    detected_at     REAL NOT NULL,
    resolved_at     REAL,
    resolution      TEXT,
    manifest_json   TEXT
);

CREATE TABLE IF NOT EXISTS records (
    source_id       TEXT NOT NULL,
    biz_key         TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    payload         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    deleted         INTEGER NOT NULL DEFAULT 0,
    via             TEXT NOT NULL,          -- push | pull-full | pull-point
    first_via       TEXT NOT NULL,
    confirmed_rev   INTEGER NOT NULL DEFAULT 0, -- highest rev stamped by pull
    confirm_hash    TEXT,                   -- bytes the pull transport confirmed
    updated_at      REAL NOT NULL,
    first_seen_at   REAL NOT NULL,
    PRIMARY KEY (source_id, biz_key)
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    biz_key         TEXT,
    at              REAL NOT NULL,
    kind            TEXT NOT NULL, -- applied|conflict|duplicate|stale|tombstone|suspect|note
    via             TEXT NOT NULL,
    local_rev       INTEGER,
    remote_rev      INTEGER,
    detail          TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id, id);
CREATE INDEX IF NOT EXISTS idx_events_key ON events(source_id, biz_key, id);

-- Operator work queue produced by anomalies; a cheap point fetch can close it.
CREATE TABLE IF NOT EXISTS suspects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    biz_key         TEXT NOT NULL,
    reason          TEXT NOT NULL,
    detected_at     REAL NOT NULL,
    resolved_at     REAL,
    resolution      TEXT
);
CREATE INDEX IF NOT EXISTS idx_suspects_open ON suspects(source_id, resolved_at);

CREATE TABLE IF NOT EXISTS requests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    at              REAL NOT NULL,
    transport       TEXT NOT NULL,          -- push | pull-full | pull-point
    request_kind    TEXT NOT NULL,          -- batch | page | point
    cost            INTEGER NOT NULL DEFAULT 1, -- synthetic request cost units
    cursor          TEXT,
    rows_returned   INTEGER NOT NULL,
    events_ingested INTEGER NOT NULL,
    applied_count   INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    conflict_count  INTEGER NOT NULL,
    scan_id         TEXT,
    full_scan       INTEGER NOT NULL DEFAULT 0,
    snapshot_cutoff REAL,
    covered_from    TEXT,
    covered_to      TEXT,
    next_cursor     TEXT,
    exhausted       INTEGER,
    consistent      INTEGER,
    gap_detected    INTEGER NOT NULL DEFAULT 0,
    page_index      INTEGER,
    row_digest      TEXT,
    page_hash       TEXT,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_requests_source ON requests(source_id, id);
CREATE INDEX IF NOT EXISTS idx_requests_run ON requests(run_id, id);
CREATE INDEX IF NOT EXISTS idx_requests_scan ON requests(scan_id, id);

CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL,
    kind            TEXT NOT NULL,          -- full | point | push
    status          TEXT NOT NULL,          -- running|completed|failed|stopped
    scan_id         TEXT,
    started_at      REAL NOT NULL,
    finished_at     REAL,
    stop_requested  INTEGER NOT NULL DEFAULT 0,
    cursor          TEXT,
    plan_json       TEXT,
    proof_json      TEXT,
    snapshot_cutoff REAL,
    coverage_epoch  INTEGER,
    requests        INTEGER NOT NULL DEFAULT 0,
    applied         INTEGER NOT NULL DEFAULT 0,
    duplicates      INTEGER NOT NULL DEFAULT 0,
    conflicts       INTEGER NOT NULL DEFAULT 0,
    exhausted       INTEGER,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_source ON runs(source_id, started_at);

CREATE TABLE IF NOT EXISTS point_fetches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    run_id          TEXT,
    biz_key         TEXT NOT NULL,
    at              REAL NOT NULL,
    found_rev       INTEGER,
    found           INTEGER NOT NULL,
    applied         INTEGER NOT NULL,
    detail          TEXT
);

-- Equal-revision/different-bytes observations.  A conflict is never settled by
-- arrival order; the operator closes it, and the closing decision is durable.
CREATE TABLE IF NOT EXISTS conflicts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    biz_key         TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    local_hash      TEXT NOT NULL,
    local_payload   TEXT NOT NULL,
    remote_hash     TEXT NOT NULL,
    remote_payload  TEXT NOT NULL,
    remote_deleted  INTEGER NOT NULL DEFAULT 0,
    first_via       TEXT NOT NULL,
    detected_at     REAL NOT NULL,
    deliveries      INTEGER NOT NULL DEFAULT 1,
    resolved_at     REAL,
    decision        TEXT,                   -- keep_local | accept_remote
    note            TEXT NOT NULL DEFAULT '',
    UNIQUE (source_id, biz_key, revision, remote_hash)
);

CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""


def content_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"{zlib.crc32(blob.encode('utf-8')) & 0xFFFFFFFF:08x}"


class Store:
    """Thin, thread-safe SQLite wrapper.  One connection guarded by a lock."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @contextmanager
    def tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def q(self, sql: str, args: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, tuple(args)))

    def one(self, sql: str, args: Iterable[Any] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, tuple(args)).fetchone()

    def execute(self, sql: str, args: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, tuple(args))
            self._conn.commit()
            return cur

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- helpers -------------------------------------------------------

    def get_kv(self, k: str, default: Any = None) -> Any:
        row = self.one("SELECT v FROM kv WHERE k=?", (k,))
        if not row:
            return default
        try:
            return json.loads(row["v"])
        except Exception:
            return default

    def set_kv(self, k: str, v: Any) -> None:
        self.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (k, json.dumps(v, ensure_ascii=False)),
        )

    def event(self, source_id: str, kind: str, via: str, **kw: Any) -> None:
        self.execute(
            """INSERT INTO events(source_id,biz_key,at,kind,via,local_rev,remote_rev,detail)
               VALUES(?,?,?,?,?,?,?,?)""",
            (
                source_id,
                kw.get("biz_key"),
                time.time(),
                kind,
                via,
                kw.get("local_rev"),
                kw.get("remote_rev"),
                json.dumps(kw.get("detail", {}), ensure_ascii=False),
            ),
        )

    def source(self, source_id: str) -> sqlite3.Row | None:
        return self.one("SELECT * FROM sources WHERE source_id=?", (source_id,))

    def require_source(self, source_id: str) -> sqlite3.Row:
        row = self.source(source_id)
        if row is None:
            raise KeyError(source_id)
        return row

    def record(self, source_id: str, biz_key: str) -> sqlite3.Row | None:
        return self.one(
            "SELECT * FROM records WHERE source_id=? AND biz_key=?",
            (source_id, biz_key),
        )
