"""Tiny HTTP server: JSON API for the collector + static demo UI.

No third-party dependencies.  Long work (full scans, point repair) runs in a
worker thread with a synchronous default for tests; the UI polls run status.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .collector import Collector
from .db import Store
from .scenarios import ensure_sources, scenario
from .sources import SourceManager

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class App:
    def __init__(self, db_path: str):
        self.store = Store(db_path)
        self.sources = SourceManager(self.store)
        self.collector = Collector(self.store)
        self.runs: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
        ensure_sources(self.store, self.sources)

    # -- background workers ------------------------------------------------
    def spawn(self, run_id: str, fn, *args, **kwargs) -> None:
        def target():
            try:
                fn(*args, **kwargs)
            except Exception:
                traceback.print_exc()
            finally:
                with self._lock:
                    self.runs.pop(run_id, None)

        t = threading.Thread(target=target, daemon=True)
        with self._lock:
            self.runs[run_id] = t
        t.start()

    def running(self, run_id: str) -> bool:
        with self._lock:
            t = self.runs.get(run_id)
        return bool(t and t.is_alive())


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ReconcileDemo/1.0"

        @property
        def app(self):
            return app

        # -- helpers ---------------------------------------------------
        def _send(self, code: int, body: bytes, ctype: str,
                  extra: dict[str, str] | None = None) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def json(self, data: Any, code: int = 200) -> None:
            self._send(code, json.dumps(data, ensure_ascii=False).encode(),
                       "application/json; charset=utf-8")

        def error(self, code: int, message: str) -> None:
            self.json({"error": message}, code)

        def body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw)
            except Exception:
                raise ValueError("body must be JSON")
            if not isinstance(data, dict):
                raise ValueError("body must be a JSON object")
            return data

        def log_message(self, fmt, *args):  # quiet by default
            if os.environ.get("DEMO_VERBOSE"):
                sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

        # -- routing ---------------------------------------------------
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            try:
                if path == "/" or path == "/index.html":
                    return self._static("index.html")
                if path.startswith("/static/"):
                    return self._static(path[len("/static/"):])
                if path == "/api/health":
                    return self.json({"ok": True})
                if path == "/api/sources":
                    return self.json(self.sources_payload())
                if path.startswith("/api/sources/"):
                    parts = path.strip("/").split("/")
                    source_id = parts[2]
                    if len(parts) == 3:
                        return self.json(self.source_payload(source_id))
                    if len(parts) == 4 and parts[3] == "status":
                        return self.json(self.app.collector.status(
                            self.app.sources, source_id))
                    if len(parts) == 4 and parts[3] == "plan":
                        return self.json(self.app.collector.plan(
                            self.app.sources, source_id))
                    if len(parts) == 4 and parts[3] == "events":
                        qs = urllib.parse.parse_qs(parsed.query)
                        key = qs.get("key", [None])[0]
                        limit = int(qs.get("limit", ["100"])[0])
                        return self.json(self.app.collector.events(
                            source_id, key, limit))
                    if len(parts) == 4 and parts[3] == "runs":
                        rows = self.app.store.q(
                            """SELECT run_id,kind,status,started_at,finished_at,
                                      requests,applied,duplicates,conflicts,
                                      coverage_epoch,exhausted,error,cursor
                               FROM runs WHERE source_id=?
                               ORDER BY started_at DESC LIMIT 25""", (source_id,))
                        out = []
                        for r in rows:
                            d = dict(r)
                            d["worker_alive"] = self.app.running(d["run_id"])
                            out.append(d)
                        return self.json(out)
                    if len(parts) == 4 and parts[3] == "requests":
                        rows = self.app.store.q(
                            """SELECT id,at,run_id,transport,request_kind,cost,
                                      rows_returned,applied_count,
                                      duplicate_count,conflict_count,
                                      covered_from,covered_to,exhausted,
                                      page_index,page_hash,gap_detected,error
                               FROM requests WHERE source_id=?
                               ORDER BY id DESC LIMIT 200""", (source_id,))
                        return self.json([dict(r) for r in rows])
                if path.startswith("/api/runs/"):
                    run_id = path.strip("/").split("/")[2]
                    run = self.app.collector.run_status(run_id)
                    if run is None:
                        return self.error(404, "run not found")
                    run["worker_alive"] = self.app.running(run_id)
                    return self.json(run)
                if path == "/api/scenarios":
                    return self.json({
                        "scenarios": [
                            {"id": "baseline", "label": "基线：推送直送",
                             "blurb": "无断档，推送覆盖全部写入"},
                            {"id": "push_gap", "label": "推送断档",
                             "blurb": "丢失一段推送后又有新写入，序号跳变暴露缺口"},
                            {"id": "tail_stall", "label": "尾部停滞",
                             "blurb": "写入被上游接受、推送事件全部丢失，且无后续事件"},
                            {"id": "conflict", "label": "同修订冲突",
                             "blurb": "同一修订、不同内容经推送到达"},
                            {"id": "high_revision", "label": "高版本迟到",
                             "blurb": "远端先产生更高修订，推送被扣住，只能靠拉取取回"},
                            {"id": "pull_lag", "label": "拉取延迟",
                             "blurb": "写入刚发生，拉取快照尚不可见"},
                        ]
                    })
                return self.error(404, "not found")
            except KeyError as exc:
                return self.error(404, f"unknown source: {exc}")
            except ValueError as exc:
                return self.error(400, str(exc))
            except Exception as exc:
                traceback.print_exc()
                return self.error(500, repr(exc))

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            try:
                payload = self.body()
                if path == "/api/source/config":
                    source_id = payload.pop("source_id", None)
                    if not source_id:
                        raise ValueError("source_id required")
                    self.app.sources.update(source_id, **payload)
                    return self.json({"ok": True,
                                      "source": dict(self.app.sources.get(source_id))})
                if path == "/api/sim/write":
                    return self.json(self.do_write(payload))
                if path == "/api/sim/outage":
                    return self.json(self.do_outage(payload))
                if path == "/api/sim/inject-conflict":
                    return self.json(self.do_inject_conflict(payload))
                if path == "/api/scenario":
                    return self.json(self.do_scenario(payload))
                if path == "/api/push/drain":
                    return self.json(self.do_drain(payload))
                if path == "/api/scan/full":
                    return self.json(self.do_full_scan(payload))
                if path == "/api/scan/resume":
                    return self.json(self.do_resume(payload))
                if path == "/api/point/run":
                    return self.json(self.do_point(payload))
                if path == "/api/conflict/resolve":
                    return self.json(self.do_resolve(payload))
                if path.startswith("/api/runs/") and path.endswith("/stop"):
                    run_id = path.strip("/").split("/")[2]
                    self.app.collector.request_stop(run_id)
                    return self.json({"ok": True, "stop_requested": run_id})
                if path == "/api/reset":
                    return self.json(self.do_reset(payload))
                return self.error(404, "not found")
            except KeyError as exc:
                return self.error(404, f"unknown: {exc}")
            except ValueError as exc:
                return self.error(400, str(exc))
            except Exception as exc:
                traceback.print_exc()
                return self.error(500, repr(exc))

        # -- static ----------------------------------------------------
        def _static(self, rel: str) -> None:
            safe = os.path.normpath(rel)
            if safe.startswith("..") or os.path.isabs(safe):
                return self.error(403, "forbidden")
            full = os.path.join(STATIC_DIR, safe)
            if not os.path.isfile(full):
                return self.error(404, "not found")
            ext = os.path.splitext(full)[1].lower()
            ctype = {".html": "text/html; charset=utf-8",
                     ".css": "text/css; charset=utf-8",
                     ".js": "application/javascript; charset=utf-8",
                     ".svg": "image/svg+xml"}.get(ext, "application/octet-stream")
            with open(full, "rb") as fh:
                return self._send(200, fh.read(), ctype)

        # -- payloads --------------------------------------------------
        def sources_payload(self) -> list[dict]:
            rows = self.app.store.q("SELECT * FROM sources ORDER BY source_id")
            out = []
            for r in rows:
                st = self.app.sources.state(r["source_id"])
                out.append({
                    "source_id": r["source_id"], "name": r["name"],
                    "push_enabled": bool(r["push_enabled"]),
                    "pull_enabled": bool(r["pull_enabled"]),
                    "page_size": r["page_size"],
                    "pull_delay_ms": r["pull_delay_ms"],
                    "drop_push": bool(r["drop_push"]),
                    "drop_rate": r["drop_rate"],
                    "push_cursor": st["push_cursor"],
                    "push_head": self.app.sources.push_head(r["source_id"]),
                    "coverage_epoch": st["coverage_epoch"],
                })
            return out

        def source_payload(self, source_id: str) -> dict:
            r = self.app.sources.get(source_id)
            st = self.app.sources.state(source_id)
            keys = self.app.store.get_kv("seed_keys", [])
            return {"source": dict(r), "state": dict(st), "seed_keys": keys}

        def do_write(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            key = (p.get("biz_key") or "").strip()
            if not key:
                raise ValueError("biz_key required")
            fields = p.get("fields") or {}
            if not isinstance(fields, dict):
                raise ValueError("fields must be an object")
            fields.setdefault("note", p.get("note") or "manual")
            rev = self.app.sources.write(source_id, key, fields)
            return {"ok": True, "write": rev}

        def do_outage(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            count = int(p.get("count", 3))
            self.app.sources.set_outage(source_id, count)
            return {"ok": True, "outage": count}

        def do_inject_conflict(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            key = p.get("biz_key")
            if not key:
                raise ValueError("biz_key required")
            return {"ok": True, "injected": self.app.sources.inject_conflict(
                source_id, key)}

        def do_scenario(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            name = p.get("name") or ""
            return {"ok": True, **scenario(self.app.store, self.app.sources,
                                           self.app.collector, source_id, name)}

        def do_drain(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            result = self.app.collector.drain_push(
                self.app.sources, source_id,
                max_batches=int(p.get("max_batches", 500)))
            return {"ok": True, "result": result}

        def do_full_scan(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            started = self.app.collector.start_full_scan(
                self.app.sources, source_id)
            run_id = started["run_id"]
            stop_after = p.get("stop_after_pages")
            crash_after = p.get("crash_after_pages")
            delay = int(p.get("page_delay_ms") or 0)
            background = bool(p.get("background"))
            kwargs = dict(
                stop_after_pages=int(stop_after) if stop_after is not None else None,
                crash_after_pages=int(crash_after) if crash_after is not None else None,
                page_delay_ms=delay,
            )
            if background or delay:
                self.app.spawn(
                    run_id, self.app.collector.run_full_scan,
                    self.app.sources, source_id, run_id, started["scan_id"],
                    **kwargs)
                return {"ok": True, "run_id": run_id,
                        "scan_id": started["scan_id"], "plan": started["plan"],
                        "background": True}
            result = self.app.collector.run_full_scan(
                self.app.sources, source_id, run_id, started["scan_id"], **kwargs)
            return {"ok": True, **result, "plan": started["plan"]}

        def do_resume(self, p: dict) -> dict:
            run_id = p.get("run_id")
            if not run_id:
                raise ValueError("run_id required")
            run = self.app.collector.run_status(run_id)
            if run is None:
                raise KeyError(run_id)
            source_id = run["source_id"]
            if run["status"] == "completed":
                raise ValueError("run already completed")
            if run["kind"] == "point":
                result = self.app.collector.point_recollect(
                    self.app.sources, source_id, run_id=run_id)
                return {"ok": True, **result, "resumed": True}
            if run["kind"] != "full":
                raise ValueError("only full and point runs can be resumed")
            delay = int(p.get("page_delay_ms") or 0)
            if p.get("background") or delay:
                self.app.spawn(
                    run_id, self.app.collector.run_full_scan,
                    self.app.sources, source_id, run_id, run.get("scan_id"),
                    page_delay_ms=delay)
                return {"ok": True, "run_id": run_id,
                        "scan_id": run.get("scan_id"), "background": True,
                        "resumed": True}
            result = self.app.collector.run_full_scan(
                self.app.sources, source_id, run_id, run.get("scan_id"),
                page_delay_ms=delay)
            return {"ok": True, **result, "resumed": True}

        def do_point(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            keys = p.get("keys")
            if keys is not None and not isinstance(keys, list):
                raise ValueError("keys must be a list")
            before = p.get("revise_before_confirm") or {}
            fired: set[str] = set()
            # Demo hook: revise a key once between fetch and confirm, to prove
            # the stability loop cannot silently keep stale data.
            def hook(key: str, revision: int) -> None:
                spec = before.get(key)
                if not spec or key in fired:
                    return
                fired.add(key)
                self.app.sources.write(source_id, key,
                                       {"revision_note": spec,
                                        "name": f"补采期间修订·{key}"})
            result = self.app.collector.point_recollect(
                self.app.sources, source_id, keys,
                confirm=bool(p.get("confirm", True)),
                run_id=p.get("run_id"),
                stop_after_keys=p.get("stop_after_keys"),
                crash_after_keys=p.get("crash_after_keys"),
                before_confirm=hook if before else None,
            )
            return {"ok": True, **result}

        def do_resolve(self, p: dict) -> dict:
            source_id = p.get("source_id") or "orders"
            key = p.get("biz_key")
            conflict_id = int(p.get("conflict_id"))
            decision = p.get("decision")
            note = p.get("note") or ""
            return {"ok": True, **self.app.collector.resolve_conflict(
                self.app.sources, source_id, key, conflict_id, decision, note)}

        def do_reset(self, p: dict) -> dict:
            source_id = p.get("source_id")
            s = self.app.store
            ids = [source_id] if source_id else [
                r["source_id"] for r in s.q("SELECT source_id FROM sources")]
            with s.tx() as c:
                for sid in ids:
                    for table in ("records", "events", "suspects", "conflicts",
                                  "requests", "gaps", "runs", "point_fetches",
                                  "remote_rows", "push_events"):
                        c.execute(f"DELETE FROM {table} WHERE source_id=?", (sid,))
                    c.execute(
                        """UPDATE source_state SET next_seq=1,next_push_seq=1,
                           push_cursor=1,outage_events=0,full_scan_runs=0,
                           full_scan_reqs=0,point_reqs=0,push_batches=0,
                           push_dropped=0,push_events=0,scan_id=NULL,
                           coverage_epoch=NULL,scan_cutoff=NULL,scan_at=NULL,
                           scan_chain=NULL,scan_pages=NULL,scan_complete=NULL
                           WHERE source_id=?""", (sid,))
                    cfg = s.require_source(sid)
                    s.execute(
                        "UPDATE sources SET pull_delay_ms=0,drop_push=0 WHERE source_id=?",
                        (sid,))
            s.execute("DELETE FROM kv WHERE k='seeded'")
            ensure_sources(s, self.app.sources)
            return {"ok": True, "reset": ids}

    return Handler


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8765):
    app = App(db_path)
    handler = make_handler(app)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"collector demo on http://{host}:{port}  (db={db_path})")
    return httpd, app


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8765"))
    db_path = os.environ.get(
        "DB_PATH", os.path.join(os.path.dirname(STATIC_DIR), "..", "data",
                                "collector.db"))
    if "--port" in argv:
        port = int(argv[argv.index("--port") + 1])
    if "--db" in argv:
        db_path = argv[argv.index("--db") + 1]
    httpd, _ = serve(db_path, host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
