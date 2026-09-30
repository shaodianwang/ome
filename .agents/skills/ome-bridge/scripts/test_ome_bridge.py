"""ome_bridge.py against a local fake bridge that verifies every signature.

  python3 -m unittest discover -s .agents/skills/ome-bridge/scripts -p 'test_*.py'
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import io
import json
import os
import shlex
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ome_bridge  # noqa: E402

KEY = "k3y-under-test-7f1c2d9e"
KEY_CMD = f"{shlex.quote(sys.executable)} -c \"print('{KEY}')\""


class FakeBridge(BaseHTTPRequestHandler):
    """Answers from `server.routes`; records every request; 401 on a bad signature."""

    def log_message(self, *_: object) -> None:
        pass

    def _handle(self, method: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        ts = self.headers.get("X-OME-Bot-Timestamp", "")
        sig = self.headers.get("X-OME-Bot-Signature", "")
        want = hmac.new(KEY.encode(), ts.encode() + b"\n" + body, hashlib.sha256).hexdigest()
        self.server.requests.append({
            "method": method, "path": self.path, "body": body, "ts": ts,
            "endpoint": self.headers.get("service_endpoint"),
        })
        if not ts.isdigit() or abs(time.time() - int(ts)) > 300 or not hmac.compare_digest(want, sig):
            self._reply(401, b"signature mismatch", "text/plain")
            return
        status, payload, delay = self.server.routes.get((method, self.path), (404, b"404 page not found", 0))
        if delay:
            time.sleep(delay)
        if isinstance(payload, (dict, list)):
            self._reply(status, json.dumps(payload).encode(), "application/json")
        else:
            self._reply(status, payload, "text/plain")

    def _reply(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")


class BridgeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeBridge)
        cls.server.daemon_threads = True
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}/ome-bot"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        self.server.routes = {}
        self.server.requests = []
        self.env = mock.patch.dict(os.environ, {"OME_BRIDGE_URL": self.base, "OME_BRIDGE_KEY_CMD": KEY_CMD})
        self.env.start()
        for name in ("OME_BOT_ACTOR_FBID", "OME_BOT_SLACK_USER", "OME_BOT_CHANNEL_ID", "OME_BOT_REQUEST_ID",
                     "OME_BRIDGE_SERVICE_ENDPOINT"):
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        self.env.stop()

    def route(self, method: str, path: str, status: int, payload: object, delay: float = 0) -> None:
        self.server.routes[(method, "/ome-bot" + path)] = (status, payload, delay)

    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = ome_bridge.main(list(argv))
            except SystemExit as exc:
                code = exc.code
        out, err = stdout.getvalue(), stderr.getvalue()
        self.assertNotIn(KEY, out)
        self.assertNotIn(KEY, err)
        return code, out, err

    def last_body(self) -> dict:
        return json.loads(self.server.requests[-1]["body"])


class RoutingHeaderTest(BridgeTestCase):
    def test_service_endpoint_header_sent_when_set(self) -> None:
        self.route("GET", "/healthz", 200, b"")
        with mock.patch.dict(os.environ, {"OME_BRIDGE_SERVICE_ENDPOINT": "ome-bot.ome-bot.example.mks"}):
            code, _, _ = self.run_cli("health")
        self.assertEqual(code, 0)
        self.assertEqual(self.server.requests[-1]["endpoint"], "ome-bot.ome-bot.example.mks")

    def test_no_service_endpoint_header_when_unset(self) -> None:
        self.route("GET", "/healthz", 200, b"")
        code, _, _ = self.run_cli("health")
        self.assertEqual(code, 0)
        self.assertIsNone(self.server.requests[-1]["endpoint"])


class SubcommandTest(BridgeTestCase):
    def test_health_empty_body_is_ok(self) -> None:
        self.route("GET", "/healthz", 200, b"")
        code, out, _ = self.run_cli("health")
        self.assertEqual(code, 0)
        self.assertEqual(out.strip(), "ok")
        self.assertEqual(self.server.requests[-1]["body"], b"")

    def test_clusters_prints_text(self) -> None:
        self.route("GET", "/v1/clusters", 200, {"ok": True, "text": "*Installed clusters*", "errorClass": ""})
        code, out, _ = self.run_cli("clusters")
        self.assertEqual(code, 0)
        self.assertEqual(out.strip(), "*Installed clusters*")
        self.assertEqual(self.server.requests[-1]["method"], "GET")

    def test_release_prints_text(self) -> None:
        self.route("GET", "/v1/release", 200, {"ok": True, "text": "STAGE  FREIGHT", "errorClass": ""})
        code, out, _ = self.run_cli("release")
        self.assertEqual((code, out.strip()), (0, "STAGE  FREIGHT"))

    def test_command_body_carries_actor_channel_and_request(self) -> None:
        os.environ.update({"OME_BOT_ACTOR_FBID": "42", "OME_BOT_SLACK_USER": "U0TEST",
                           "OME_BOT_CHANNEL_ID": "C0TEST", "OME_BOT_REQUEST_ID": "req-1"})
        self.route("POST", "/v1/command", 200, {"ok": True, "text": "`kubectl ome get isvc -A`", "errorClass": ""})
        code, out, _ = self.run_cli("command", "get isvc -A")
        self.assertEqual(code, 0)
        self.assertIn("get isvc -A", out)
        self.assertEqual(self.last_body(), {"actor": {"employeeFbid": 42, "slackUserId": "U0TEST"},
                                            "channelId": "C0TEST", "requestId": "req-1", "text": "get isvc -A"})

    def test_command_generates_request_id_and_joins_tokens(self) -> None:
        self.route("POST", "/v1/command", 200, {"ok": True, "text": "x", "errorClass": ""})
        code, _, _ = self.run_cli("command", "status", "chat", "-n", "team a", "--cluster", "c1")
        self.assertEqual(code, 0)
        body = self.last_body()
        self.assertEqual(body["text"], "status chat -n 'team a' --cluster c1")
        self.assertEqual(len(body["requestId"]), 36)
        self.assertEqual(body["actor"], {})

    def test_tool_passes_args_verbatim_and_prints_output(self) -> None:
        self.route("POST", "/v1/tool", 200, {"ok": True, "text": "```\nrendered\n```", "errorClass": "",
                                             "result": {"tool": "builds", "exitCode": 0, "output": "b1 passed\n"}})
        code, out, _ = self.run_cli("tool", "builds", "list", "--limit", "5", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(out.strip(), "b1 passed")
        body = self.last_body()
        self.assertEqual((body["tool"], body["args"]), ("builds", ["list", "--limit", "5", "--json"]))

    def test_json_prints_raw_response(self) -> None:
        payload = {"ok": True, "text": "t", "errorClass": "", "clusters": [{"name": "c1"}]}
        self.route("GET", "/v1/clusters", 200, payload)
        code, out, _ = self.run_cli("--json", "clusters")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), payload)

    def test_json_after_subcommand(self) -> None:
        self.route("GET", "/v1/release", 200, {"ok": True, "text": "t", "errorClass": ""})
        code, out, _ = self.run_cli("release", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["text"], "t")

    def test_truncated_tool_output_is_flagged(self) -> None:
        self.route("POST", "/v1/tool", 200, {"ok": True, "text": "", "errorClass": "",
                                             "result": {"exitCode": 0, "output": "a", "truncated": True}})
        code, _, err = self.run_cli("tool", "docs", "read", "oeps/STATUS.md")
        self.assertEqual(code, 0)
        self.assertIn("truncated", err)


class RefusalTest(BridgeTestCase):
    def test_command_not_a_verb(self) -> None:
        self.route("POST", "/v1/command", 200, {"ok": False, "text": ":no_entry: nope", "errorClass": "not_a_verb",
                                                "error": "\"why is\" is not a read-only kubectl-ome verb"})
        code, out, _ = self.run_cli("command", "why is chat down")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=not_a_verb", out)
        self.assertIn("not a read-only kubectl-ome verb", out)

    def test_tool_subcommand_not_allowed(self) -> None:
        self.route("POST", "/v1/tool", 200, {"ok": False, "text": "", "errorClass": "subcommand_not_allowed",
                                             "error": "retry is not allowed"})
        code, out, _ = self.run_cli("tool", "ci", "retry", "123", "lint")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=subcommand_not_allowed", out)

    def test_json_refusal_prints_body_and_class(self) -> None:
        self.route("POST", "/v1/command", 200, {"ok": False, "text": "", "errorClass": "needs_cluster", "error": "e"})
        code, out, _ = self.run_cli("--json", "command", "logs chat")
        self.assertEqual(code, 2)
        self.assertIn('"errorClass": "needs_cluster"', out)
        self.assertIn("errorClass=needs_cluster", out)

    def test_busy_429(self) -> None:
        self.route("POST", "/v1/command", 429, {"ok": False, "text": "busy", "errorClass": "busy", "error": "too many"})
        code, out, _ = self.run_cli("command", "get isvc -A")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=busy", out)

    def test_busy_429_without_json_body(self) -> None:
        self.route("GET", "/v1/clusters", 429, b"slow down")
        code, out, _ = self.run_cli("clusters")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=busy", out)

    def test_release_not_configured(self) -> None:
        self.route("GET", "/v1/release", 404, {"ok": False, "text": "", "errorClass": "not_configured",
                                               "error": "kargo is not configured"})
        code, out, _ = self.run_cli("release")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=not_configured", out)

    def test_endpoint_missing_reads_as_not_configured(self) -> None:
        code, out, _ = self.run_cli("release")
        self.assertEqual(code, 2)
        self.assertIn("errorClass=not_configured", out)


class CLIFailureTest(BridgeTestCase):
    def test_command_ok_false_without_class(self) -> None:
        self.route("POST", "/v1/command", 200, {"ok": False, "text": "*c1* — exit 1\nerror: not found", "errorClass": ""})
        code, out, err = self.run_cli("command", "status chat -n team-a")
        self.assertEqual(code, 3)
        self.assertIn("error: not found", out)
        self.assertIn("ran and failed", err)

    def test_tool_nonzero_exit(self) -> None:
        self.route("POST", "/v1/tool", 200, {"ok": False, "text": "", "errorClass": "",
                                             "result": {"exitCode": 2, "output": "no such build", "error": "exit status 2"}})
        code, out, err = self.run_cli("tool", "ci", "status", "999999")
        self.assertEqual(code, 3)
        self.assertIn("no such build", out)
        self.assertIn("exit 2", err)

    def test_tool_ok_true_but_nonzero_exit(self) -> None:
        self.route("POST", "/v1/tool", 200, {"ok": True, "text": "", "errorClass": "",
                                             "result": {"exitCode": 1, "output": "partial"}})
        code, _, _ = self.run_cli("tool", "builds", "diff", "1")
        self.assertEqual(code, 3)


class TransportTest(BridgeTestCase):
    def test_server_error(self) -> None:
        self.route("GET", "/v1/clusters", 500, b"boom")
        code, _, err = self.run_cli("clusters")
        self.assertEqual(code, 1)
        self.assertIn("HTTP 500", err)

    def test_wrong_key_is_auth_failure(self) -> None:
        os.environ["OME_BRIDGE_KEY_CMD"] = f"{shlex.quote(sys.executable)} -c \"print('wrong')\""
        self.route("GET", "/v1/clusters", 200, {"ok": True, "text": "t"})
        code, _, err = self.run_cli("clusters")
        self.assertEqual(code, 1)
        self.assertIn("HTTP 401", err)

    def test_connection_refused(self) -> None:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        os.environ["OME_BRIDGE_URL"] = f"http://127.0.0.1:{port}"
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("cannot reach the bridge", err)

    def test_timeout(self) -> None:
        self.route("GET", "/v1/clusters", 200, {"ok": True, "text": "t"}, delay=2)
        code, _, err = self.run_cli("--timeout", "0.5", "clusters")
        self.assertEqual(code, 1)
        self.assertIn("ome_bridge:", err)

    def test_non_json_success_body(self) -> None:
        self.route("GET", "/v1/clusters", 200, b"<html>")
        code, _, err = self.run_cli("clusters")
        self.assertEqual(code, 1)
        self.assertIn("not a JSON object", err)

    def test_key_command_failure_hides_stdout(self) -> None:
        script = f"import sys; print('{KEY}'); print('denied', file=sys.stderr); sys.exit(3)"
        os.environ["OME_BRIDGE_KEY_CMD"] = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("exited 3: denied", err)
        self.assertEqual(self.server.requests, [])

    def test_key_command_missing_binary(self) -> None:
        os.environ["OME_BRIDGE_KEY_CMD"] = "/nonexistent/secrets-cli get key"
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("not found", err)

    def test_key_command_empty_output(self) -> None:
        os.environ["OME_BRIDGE_KEY_CMD"] = f"{shlex.quote(sys.executable)} -c pass"
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("printed no key", err)

    def test_missing_url(self) -> None:
        del os.environ["OME_BRIDGE_URL"]
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("OME_BRIDGE_URL is not set", err)

    def test_missing_key_cmd(self) -> None:
        del os.environ["OME_BRIDGE_KEY_CMD"]
        code, _, err = self.run_cli("health")
        self.assertEqual(code, 1)
        self.assertIn("OME_BRIDGE_KEY_CMD is not set", err)

    def test_non_numeric_actor(self) -> None:
        os.environ["OME_BOT_ACTOR_FBID"] = "someone"
        code, _, err = self.run_cli("command", "get isvc -A")
        self.assertEqual(code, 1)
        self.assertIn("numeric", err)

    def test_usage_error_is_not_a_refusal(self) -> None:
        code, _, _ = self.run_cli("command")
        self.assertEqual(code, 1)
        code, _, _ = self.run_cli("frobnicate")
        self.assertEqual(code, 1)

    def test_key_echoed_by_bridge_is_redacted(self) -> None:
        self.route("GET", "/v1/clusters", 200, {"ok": True, "text": f"leak {KEY} here"})
        code, out, _ = self.run_cli("clusters")
        self.assertEqual(code, 0)
        self.assertIn("[redacted]", out)


if __name__ == "__main__":
    unittest.main()
