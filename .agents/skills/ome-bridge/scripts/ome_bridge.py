#!/usr/bin/env python3
"""Call the ome-bot bridge: signed, read-only requests from the OME bot's agent.

The bridge runs read-only kubectl-ome verbs, allowlisted CI tools and the docs
tool with its own credentials; this helper only builds, signs and sends the
request and maps the answer to an exit code. Standard library only.

  ome_bridge.py health                          # GET /healthz
  ome_bridge.py clusters                        # GET /v1/clusters (fleet table)
  ome_bridge.py command "get isvc -A"           # POST /v1/command
  ome_bridge.py command "status chat -n team-a --cluster <name>"
  ome_bridge.py tool builds list --limit 5      # POST /v1/tool
  ome_bridge.py tool docs search RatioBalanced
  ome_bridge.py release                         # GET /v1/release
  ome_bridge.py playbooks --dir /tmp/ome-playbooks
                                                # copy the repository's current
                                                # skills out of the bridge's docs
  ome_bridge.py --json clusters                 # raw response JSON

Global flags (--json, --timeout) go before the subcommand; everything after
`tool <name>` is passed to the tool verbatim. `playbooks` reads every bot
skill (SKILL.md and the references it names) through the docs tool, which
serves the repository as of the bridge's last build, and writes them under
--dir, one directory per skill; it prints the files it wrote.

Environment:
  OME_BRIDGE_URL       base URL of the bridge, including any path prefix (required)
  OME_BRIDGE_KEY_CMD   command whose stdout is the HMAC key; split with shlex,
                       run without a shell (required). The key is never printed.
  OME_BRIDGE_SERVICE_ENDPOINT
                       SMC tier name sent as the service_endpoint header; the
                       MKS HTTP frontend in front of the bridge routes on it
                       (optional)
  OME_BOT_ACTOR_FBID   numeric id of the person who asked (optional)
  OME_BOT_SLACK_USER   Slack user id of the person who asked (optional)
  OME_BOT_CHANNEL_ID   Slack channel the question came from (optional)
  OME_BOT_REQUEST_ID   request id shared by every call of one answer; a fresh
                       uuid4 per call when unset

Exit codes:
  0  ok
  1  the request did not complete: missing configuration or bad usage, the key
     command failed, connection error or timeout, HTTP 401/403 or 5xx
  2  the bridge refused: ok=false with an errorClass, HTTP 429 (busy) or 404
     (not_configured); the errorClass and error are printed
  3  the CLI or tool ran and exited non-zero (ok=false without an errorClass,
     or result.exitCode != 0)
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, NoReturn

EXIT_OK = 0
EXIT_TRANSPORT = 1
EXIT_REFUSED = 2
EXIT_CLI_FAILED = 3

DEFAULT_TIMEOUT_SECONDS = 90.0
DEFAULT_PLAYBOOK_DIR = "/tmp/ome-playbooks"

# The bot's skills, as the docs tool's `skills` root holds them.
PLAYBOOKS = ("ome-bridge", "ome-answer", "ome-recipe", "ome-oncall", "ome-docs", "ome-fleet", "ome-memory")
# A reference a playbook names, optionally qualified with the bot skill it
# lives in. A path into another skill's tree (`skills/ci-triage/references/...`)
# is a docs-tool path for the model to read, not a playbook file.
REFERENCE_RE = re.compile(r"(?<![\w/-])(?:(ome-[a-z]+)/)?references/([A-Za-z0-9_.-]+\.md)")
# The docs tool ends a capped read with this line; the next page starts there.
TRUNCATED_RE = re.compile(r"… truncated after line \d+; continue with --from (\d+)\n?\Z")
MAX_PAGES = 50

# errorClass reported for a refusal whose body does not carry one, keyed by
# HTTP status.
STATUS_CLASS = {400: "malformed", 404: "not_configured", 413: "malformed", 429: "busy"}


class BridgeError(Exception):
    """A failure before a usable response exists; always exit code 1."""


class _Parser(argparse.ArgumentParser):
    # Usage errors exit 1 so exit 2 always means "the bridge refused".
    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(EXIT_TRANSPORT, f"{self.prog}: error: {message}\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                        help="print the raw response JSON")
    common.add_argument("--timeout", type=float, default=argparse.SUPPRESS,
                        help=f"seconds to wait for the key command and for the bridge, default {DEFAULT_TIMEOUT_SECONDS:g}")

    parser = _Parser(prog="ome_bridge.py", description=__doc__,
                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true", help="print the raw response JSON")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS,
                        help=f"seconds to wait for the key command and for the bridge (default {DEFAULT_TIMEOUT_SECONDS:g})")
    sub = parser.add_subparsers(dest="cmd", required=True, parser_class=_Parser)
    sub.add_parser("health", parents=[common], help="GET /healthz")
    sub.add_parser("clusters", parents=[common], help="GET /v1/clusters")
    sub.add_parser("release", parents=[common], help="GET /v1/release")
    p = sub.add_parser("command", parents=[common], help="POST /v1/command")
    p.add_argument("text", nargs=argparse.REMAINDER,
                   help='the command, preferably one quoted argument: "get isvc -A"')
    p = sub.add_parser("tool", parents=[common], help="POST /v1/tool")
    p.add_argument("name", help="tool name: builds, ci, docs")
    p.add_argument("args", nargs=argparse.REMAINDER, help="subcommand and arguments, verbatim")
    p = sub.add_parser("playbooks", parents=[common],
                       help="copy the bot's skills out of the bridge's docs tool into --dir")
    p.add_argument("--dir", default=DEFAULT_PLAYBOOK_DIR,
                   help=f"where to write them, one directory per skill (default {DEFAULT_PLAYBOOK_DIR})")
    args = parser.parse_args(argv)
    if args.cmd == "command" and not "".join(args.text).strip():
        parser.error("command needs the command text")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def command_text(parts: list[str]) -> str:
    """One argument is sent as is; several are re-quoted for the bridge's tokenizer."""
    if len(parts) == 1:
        return parts[0]
    return shlex.join(parts)


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise BridgeError(f"{name} is not set; see the ome-bridge skill for the environment it needs")
    return value


def read_key(timeout: float) -> bytes:
    raw = required_env("OME_BRIDGE_KEY_CMD")
    try:
        argv = shlex.split(raw)
    except ValueError as exc:
        raise BridgeError(f"OME_BRIDGE_KEY_CMD cannot be parsed: {exc}") from None
    if not argv:
        raise BridgeError("OME_BRIDGE_KEY_CMD is empty")
    try:
        proc = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
    except FileNotFoundError:
        raise BridgeError(f"key command {argv[0]!r} not found") from None
    except subprocess.TimeoutExpired:
        raise BridgeError(f"key command {argv[0]!r} timed out after {timeout:g}s") from None
    except OSError as exc:
        raise BridgeError(f"key command {argv[0]!r} failed to start: {exc.strerror}") from None
    if proc.returncode != 0:
        # stdout may hold part of a key; only stderr is ever shown.
        detail = proc.stderr.decode(errors="replace").strip()[-300:]
        raise BridgeError(f"key command {argv[0]!r} exited {proc.returncode}" + (f": {detail}" if detail else ""))
    key = proc.stdout.strip()
    if not key:
        raise BridgeError(f"key command {argv[0]!r} printed no key")
    return key


def sign(key: bytes, timestamp: str, body: bytes) -> str:
    return hmac.new(key, timestamp.encode() + b"\n" + body, hashlib.sha256).hexdigest()


def request_meta() -> dict[str, Any]:
    actor: dict[str, Any] = {}
    fbid = os.environ.get("OME_BOT_ACTOR_FBID", "").strip()
    if fbid:
        if not fbid.isdigit():
            raise BridgeError("OME_BOT_ACTOR_FBID must be numeric")
        actor["employeeFbid"] = int(fbid)
    slack_user = os.environ.get("OME_BOT_SLACK_USER", "").strip()
    if slack_user:
        actor["slackUserId"] = slack_user
    return {
        "actor": actor,
        "channelId": os.environ.get("OME_BOT_CHANNEL_ID", "").strip(),
        "requestId": os.environ.get("OME_BOT_REQUEST_ID", "").strip() or str(uuid.uuid4()),
    }


def build_request(args: argparse.Namespace) -> tuple[str, str, bytes]:
    """Method, path and body for one subcommand."""
    if args.cmd == "health":
        return "GET", "/healthz", b""
    if args.cmd == "clusters":
        return "GET", "/v1/clusters", b""
    if args.cmd == "release":
        return "GET", "/v1/release", b""
    payload = request_meta()
    if args.cmd == "command":
        payload["text"] = command_text(args.text)
        path = "/v1/command"
    else:
        payload["tool"] = args.name
        payload["args"] = list(args.args)
        path = "/v1/tool"
    return "POST", path, json.dumps(payload, separators=(",", ":")).encode()


def routing_headers() -> dict[str, str]:
    """The MKS HTTP frontend picks the backend from the service_endpoint header;
    a request without it never reaches the bridge and answers 4xx."""
    endpoint = os.environ.get("OME_BRIDGE_SERVICE_ENDPOINT", "").strip()
    return {"service_endpoint": endpoint} if endpoint else {}


def send(base_url: str, key: bytes, method: str, path: str, body: bytes, timeout: float) -> tuple[int, bytes]:
    timestamp = str(int(time.time()))
    headers = {
        "X-OME-Bot-Timestamp": timestamp,
        "X-OME-Bot-Signature": sign(key, timestamp, body),
        "Accept": "application/json",
        "User-Agent": "ome-bridge-skill",
        **routing_headers(),
    }
    if method == "POST":
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base_url.rstrip("/") + path, data=body if method == "POST" else None,
                                 headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except urllib.error.URLError as exc:
        raise BridgeError(f"cannot reach the bridge: {exc.reason}") from None
    except (TimeoutError, OSError) as exc:
        raise BridgeError(f"bridge request failed: {exc}") from None


def decode(body: bytes) -> dict[str, Any] | None:
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    return doc if isinstance(doc, dict) else None


class Output:
    """Writes to stdout and stderr with the key scrubbed from every line."""

    def __init__(self, key: bytes | None) -> None:
        self.key = key.decode(errors="replace") if key else ""

    def _scrub(self, text: str) -> str:
        return text.replace(self.key, "[redacted]") if self.key else text

    def out(self, text: str) -> None:
        if text:
            print(self._scrub(text.rstrip("\n")))

    def err(self, text: str) -> None:
        print(self._scrub(text.rstrip("\n")), file=sys.stderr)


def report(args: argparse.Namespace, status: int, body: bytes, out: Output) -> int:
    doc = decode(body)
    raw = body.decode(errors="replace").strip()

    if status in (401, 403):
        out.err(f"ome_bridge: HTTP {status} auth failure: {raw or 'no detail'}")
        return EXIT_TRANSPORT
    if status >= 500:
        detail = (doc or {}).get("error") or raw or "no detail"
        out.err(f"ome_bridge: HTTP {status} from the bridge: {detail}")
        return EXIT_TRANSPORT
    if status >= 300:
        cls = ((doc or {}).get("errorClass") or STATUS_CLASS.get(status) or f"http_{status}")
        detail = (doc or {}).get("error") or (doc or {}).get("text") or raw or f"HTTP {status}"
        if args.json and doc is not None:
            out.out(raw)
        out.out(f"refused errorClass={cls}: {detail}")
        return EXIT_REFUSED

    if doc is None:
        if args.cmd == "health" and not raw:
            doc, raw = {"ok": True, "text": "ok"}, json.dumps({"ok": True, "text": "ok"})
        else:
            out.err(f"ome_bridge: HTTP {status} with a body that is not a JSON object: {raw[:300]}")
            return EXIT_TRANSPORT

    result = doc.get("result") if isinstance(doc.get("result"), dict) else None
    text = doc.get("text") or ""
    if args.cmd == "tool" and result is not None and result.get("output") is not None:
        text = result.get("output") or ""

    if doc.get("ok") is not True and doc.get("errorClass"):
        if args.json:
            out.out(raw)
        detail = doc.get("error") or text or "no detail"
        out.out(f"refused errorClass={doc['errorClass']}: {detail}")
        return EXIT_REFUSED

    out.out(raw if args.json else text)
    if result is not None and result.get("truncated"):
        out.err("ome_bridge: tool output truncated by the bridge; narrow the query")
    exit_code = result.get("exitCode", 0) if result is not None else 0
    if doc.get("ok") is not True or exit_code not in (0, None):
        detail = (result or {}).get("error") or doc.get("error") or ""
        suffix = f" exit {exit_code}" if result is not None else ""
        out.err(f"ome_bridge: {args.cmd} ran and failed{suffix}" + (f": {detail}" if detail else ""))
        return EXIT_CLI_FAILED
    return EXIT_OK


def docs_read(base_url: str, key: bytes, timeout: float, path: str) -> str:
    """One file of the docs tool, whole: follows the truncation marker page by page."""
    chunks: list[str] = []
    start = 1
    for _ in range(MAX_PAGES):
        payload = request_meta()
        payload["tool"] = "docs"
        payload["args"] = ["read", path, "--from", str(start)]
        body = json.dumps(payload, separators=(",", ":")).encode()
        status, resp = send(base_url, key, "POST", "/v1/tool", body, timeout)
        doc = decode(resp)
        result = (doc or {}).get("result") if isinstance((doc or {}).get("result"), dict) else None
        if status != 200 or doc is None or doc.get("ok") is not True or result is None:
            detail = (doc or {}).get("error") or (doc or {}).get("errorClass") or resp.decode(errors="replace")[:200]
            raise BridgeError(f"docs read {path} failed (HTTP {status}): {detail}")
        text = result.get("output") or ""
        marker = TRUNCATED_RE.search(text)
        if marker is None:
            chunks.append(text)
            return "".join(chunks)
        chunks.append(text[:marker.start()])
        start = int(marker.group(1))
    raise BridgeError(f"docs read {path}: more than {MAX_PAGES} pages")


def write_playbook(out_dir: str, rel: str, text: str) -> str:
    target = Path(out_dir) / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return str(target)


def fetch_playbooks(base_url: str, key: bytes, timeout: float, out_dir: str, out: Output) -> int:
    """Every bot skill and the references it names, from the docs tool's `skills` root."""
    written: list[str] = []
    fetched: set[str] = set()
    for skill in PLAYBOOKS:
        text = docs_read(base_url, key, timeout, f"skills/{skill}/SKILL.md")
        written.append(write_playbook(out_dir, f"{skill}/SKILL.md", text))
        for owner, name in sorted(set(REFERENCE_RE.findall(text))):
            rel = f"{owner or skill}/references/{name}"
            if rel in fetched:
                continue
            fetched.add(rel)
            try:
                ref_text = docs_read(base_url, key, timeout, f"skills/{rel}")
            except BridgeError as exc:
                # A reference the docs cannot serve costs that file, not the run.
                out.err(f"ome_bridge: {exc}")
                continue
            written.append(write_playbook(out_dir, rel, ref_text))
    for path in written:
        out.out(path)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    out = Output(None)
    try:
        base_url = required_env("OME_BRIDGE_URL")
        if args.cmd != "playbooks":
            method, path, body = build_request(args)
        key = read_key(args.timeout)
        out = Output(key)
        if args.cmd == "playbooks":
            return fetch_playbooks(base_url, key, args.timeout, args.dir, out)
        status, resp = send(base_url, key, method, path, body, args.timeout)
    except BridgeError as exc:
        out.err(f"ome_bridge: {exc}")
        return EXIT_TRANSPORT
    return report(args, status, resp, out)


if __name__ == "__main__":
    sys.exit(main())
