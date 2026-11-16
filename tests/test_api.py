"""HTTP-level tests: exercise the same endpoints the demo UI calls."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from app.server import serve


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="collector-api-")
        cls.httpd, cls.app = serve(os.path.join(cls.tmp, "api.db"),
                                   port=0)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.app.store.close()

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def get(self, path):
        with urllib.request.urlopen(self.url(path)) as r:
            return json.load(r)

    def post(self, path, body=None):
        req = urllib.request.Request(
            self.url(path), data=json.dumps(body or {}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.load(r)

    def test_index_and_assets(self):
        with urllib.request.urlopen(self.url("/")) as r:
            self.assertIn("双通道采集", r.read().decode())
        for asset in ("/static/app.js", "/static/style.css"):
            with urllib.request.urlopen(self.url(asset)) as r:
                self.assertEqual(r.status, 200)

    def test_full_demo_flow(self):
        self.post("/api/reset")
        sources = self.get("/api/sources")
        self.assertEqual({s["source_id"] for s in sources},
                         {"orders", "inventory"})
        drain = self.post("/api/push/drain", {"source_id": "orders"})
        self.assertEqual(drain["result"]["gaps"], [])
        scan = self.post("/api/scan/full", {"source_id": "orders"})
        self.assertTrue(scan["proof"]["complete"])
        status = self.get("/api/sources/orders/status")
        self.assertEqual(status["conclusion"]["status"], "proven")
        self.assertEqual(status["truth"]["behind"], [])

        # Simulate a gap, then verify the API exposes it as unproven.
        self.post("/api/scenario", {"source_id": "orders", "name": "push_gap"})
        self.post("/api/push/drain", {"source_id": "orders"})
        status = self.get("/api/sources/orders/status")
        self.assertNotEqual(status["conclusion"]["status"], "proven")
        open_gaps = [g for g in status["gaps"] if not g["resolved_at"]]
        self.assertEqual(len(open_gaps), 1)
        plan = self.get("/api/sources/orders/plan")
        self.assertEqual(plan["recommendation"]["strategy"], "full")

        # Targeted repair closes the named keys but does not certify coverage.
        point = self.post("/api/point/run", {"source_id": "orders"})
        self.assertGreater(point["requests"], 0)
        status = self.get("/api/sources/orders/status")
        self.assertEqual(status["truth"]["behind"], [])
        self.assertNotEqual(status["conclusion"]["status"], "proven")

        # A full scan certifies the recovered range.
        scan = self.post("/api/scan/full", {"source_id": "orders"})
        self.assertTrue(scan["proof"]["complete"])
        status = self.get("/api/sources/orders/status")
        self.assertEqual(status["conclusion"]["status"], "proven")

    def test_interrupted_scan_resumes_over_http(self):
        self.post("/api/reset", {})
        started = self.post("/api/scan/full", {"crash_after_pages": 1})
        self.assertTrue(started["crashed"])
        run_id = started["run_id"]
        run = self.get("/api/runs/" + run_id)
        self.assertEqual(run["status"], "interrupted")
        resumed = self.post("/api/scan/resume", {"run_id": run_id})
        self.assertTrue(resumed["proof"]["complete"])
        run = self.get("/api/runs/" + run_id)
        self.assertEqual(run["status"], "completed")

    def test_conflict_is_not_auto_resolved(self):
        self.post("/api/reset", {})
        self.post("/api/push/drain", {})
        self.post("/api/scenario", {"name": "conflict"})
        self.post("/api/push/drain", {})
        status = self.get("/api/sources/orders/status")
        self.assertEqual(len(status["conflicts"]), 1)
        c = status["conflicts"][0]
        resolved = self.post("/api/conflict/resolve", {
            "source_id": "orders", "biz_key": c["biz_key"],
            "conflict_id": c["id"], "decision": "keep_local"})
        self.assertEqual(resolved["outcome"], "keep_local")
        status = self.get("/api/sources/orders/status")
        self.assertEqual(status["conflicts"], [])

    def test_point_run_can_resume_over_http(self):
        self.post("/api/reset", {})
        self.post("/api/push/drain", {})
        crash = self.post("/api/point/run", {
            "source_id": "orders", "keys": ["C-1001", "C-1002", "C-1003"],
            "crash_after_keys": 1})
        self.assertTrue(crash["crashed"])
        run = self.get("/api/runs/" + crash["run_id"])
        self.assertEqual(run["status"], "interrupted")
        resumed = self.post("/api/scan/resume", {"run_id": crash["run_id"]})
        self.assertEqual(resumed["confirmed"], 2)
        self.assertEqual(resumed["skipped"], ["C-1001"])

    def test_validation_errors(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.post("/api/scan/full", {"source_id": "nope"})
        self.assertEqual(ctx.exception.code, 404)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.post("/api/point/run", {"source_id": "orders",
                                         "keys": "not-a-list"})
        self.assertEqual(ctx.exception.code, 400)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.get("/static/../server.py")
        self.assertEqual(ctx.exception.code, 403)


if __name__ == "__main__":
    unittest.main()
