"""triage.py against canned meta and kubectl output; no network.

  python3 -m unittest discover -s .agents/skills/serving-incident/scripts -p 'test_*.py'
"""

import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import triage  # noqa: E402

END = 1_800_000_000  # a bucket boundary for 10m buckets
B0 = END - 1200
B1 = END - 600


def fake_run(cmd, timeout):
    joined = " ".join(cmd)
    if cmd[0] == "meta":
        sql = next(c for c in cmd if c.startswith("--sql="))
        if "envoy_access_logs" in sql:
            raise RuntimeError("scuba: permission denied")
        if "GROUP BY b" in sql:
            return json.dumps([{"b": str(B0), "total": "100", "err": "0"}, {"b": str(B1), "total": "200", "err": "10"}])
        if "exception_type AS e" in sql:
            return "noise\n" + json.dumps([{"e": "ParseError", "ck": "/ckpts/example-model-ckpt7", "n": "10"}])
        if "decode_host" in sql:
            return json.dumps([{"h": "http://10.0.0.9:30000", "total": "12", "err": "9"}])
        return "[]"
    if "trafficmap" in joined:
        raise RuntimeError('Error from server (NotFound): trafficmaps "x" not found')
    if "query_range" in joined:
        return json.dumps({"data": {"result": [
            {"metric": {"status": "200"}, "values": [[B1 + 600, "90"]]},
            {"metric": {"status": "503"}, "values": [[B1 + 600, "10"]]},
        ]}})
    if "status.podIP=10.0.0.9" in joined:
        return "team-a/example-engine-3" if "member-b" in joined else ""
    if "pods" in joined:
        return json.dumps({"items": [{"metadata": {"name": "example-engine-3"}, "status": {"containerStatuses": [
            {"name": "ome-container", "ready": False, "lastState": {"terminated": {"finishedAt": "2027-01-15T07:55:00Z", "reason": "Error", "exitCode": 1}}},
        ]}}]})
    raise AssertionError(joined)


class TriageTest(unittest.TestCase):
    def run_main(self):
        argv = ["triage.py", "--isvc", "example-model", "--namespace", "team-a", "--clusters", "member-a,member-b",
                "--hub", "hub-a", "--proxy-tier", "proxy_tier_a", "--since", "20", "--until", str(END), "--prom-retention", "20"]
        out = io.StringIO()
        with mock.patch.object(triage, "run", fake_run), mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(out):
            code = triage.main()
        return code, out.getvalue()

    def test_grid_rows_and_notes(self):
        code, out = self.run_main()
        proxy = next(line for line in out.splitlines() if line.startswith("proxy "))
        self.assertEqual(proxy.split()[1:], ["0.0", "5.0"])
        router = next(line for line in out.splitlines() if line.startswith("router member-a"))
        self.assertEqual(router.split()[-1], "10.0")
        self.assertIn("ParseError  on example-model-ckpt7", out)
        self.assertIn("single-cluster service", out)
        self.assertIn("1 pods, 1 not ready", out)

    def test_failed_probe_is_unreadable_not_healthy(self):
        code, out = self.run_main()
        self.assertEqual(code, 1)
        self.assertIn("sidecar: UNREADABLE (scuba: permission denied)", out)
        self.assertFalse(any(line.startswith("sidecar ") for line in out.splitlines()))

    def test_hot_engine_is_located(self):
        _, out = self.run_main()
        self.assertIn("hot engine 10.0.0.9: 9/12 errors -> member-b team-a/example-engine-3", out)


if __name__ == "__main__":
    unittest.main()
