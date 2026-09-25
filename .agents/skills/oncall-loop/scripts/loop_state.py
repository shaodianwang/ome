#!/usr/bin/env python3
"""Local state for the oncall loop: watermark, handled builds, iteration log.

Standard library only, no network. Reads the JSON that
`ci-triage/scripts/main_builds.py list --json` prints on stdin where a build
list is needed. State lives under ~/.cache/ome-agent.noindex/scratch/oncall-loop/
(override: OME_ONCALL_STATE_DIR), never under the repository.

  main_builds.py list --json | loop_state.py init [--reset]      # watermark = newest build
  loop_state.py init --watermark N [--reset]
  main_builds.py list --json | loop_state.py new [--json]         # unhandled red builds above the watermark
  main_builds.py list --json | loop_state.py greens --since N     # consecutive green builds after N
  loop_state.py mark <build> <status> [--keys a,b] [--issue URL] [--pr URL] [--note TEXT]
  loop_state.py show | prune [--keep N] | log TEXT
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

STATUSES = (
    "triage-dispatched", "flaky-known", "infra-recovered", "awaiting-retry", "issue-open",
    "pr-open", "escalated", "user-handled", "closed",
)
TERMINAL_STATUSES = {"flaky-known", "infra-recovered", "user-handled", "closed"}
POLICY = (
    "Dispatch ci-triage only for standard main builds above the watermark; builds at or "
    "below it are user-handled. Dedupe by build number; one write-up per breakage."
)


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def state_dir() -> Path:
    root = os.environ.get("OME_ONCALL_STATE_DIR") or str(Path.home() / ".cache" / "ome-agent.noindex" / "scratch" / "oncall-loop")
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def state_path() -> Path:
    return state_dir() / "state.json"


def load_state() -> dict:
    path = state_path()
    if not path.is_file():
        sys.stderr.write(f"loop_state: {path} does not exist; run `init` first\n")
        sys.exit(2)
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    path = state_path()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_rows() -> list[dict]:
    if sys.stdin.isatty():
        sys.stderr.write("loop_state: expected `main_builds.py list --json` on stdin\n")
        sys.exit(2)
    text = sys.stdin.read()
    try:
        rows = json.loads(text, strict=False)
    except json.JSONDecodeError as err:
        sys.stderr.write(f"loop_state: stdin is not JSON: {err}\n")
        sys.exit(2)
    if not isinstance(rows, list):
        sys.stderr.write("loop_state: stdin JSON is not a list of builds\n")
        sys.exit(2)
    rows = [r for r in rows if isinstance(r, dict) and r.get("number") is not None and not r.get("daily")]
    return sorted(rows, key=lambda r: int(r["number"]))


def is_red(row: dict) -> bool:
    return bool(row.get("red_keys"))


def is_green(row: dict) -> bool:
    return row.get("terminal") and row.get("state") == "passed" and not is_red(row)


# --- commands --------------------------------------------------------------------

def cmd_init(args) -> None:
    path = state_path()
    if path.is_file() and not args.reset:
        state = load_state()
        print(f"state exists at {path} (watermark b{state.get('watermark')}); use --reset to move the watermark")
        return
    handled = {}
    if args.watermark is not None:
        watermark = args.watermark
    else:
        rows = read_rows()
        if not rows:
            sys.stderr.write("loop_state: no builds on stdin to derive a watermark from\n")
            sys.exit(2)
        watermark = int(rows[-1]["number"])
        for r in rows:
            if is_red(r):
                handled[str(r["number"])] = {
                    "status": "user-handled", "keys": r.get("red_keys", []), "issue": "", "pr": "",
                    "note": "red before the loop started", "updated": now(),
                }
    if path.is_file() and args.reset:
        old = load_state()
        handled = {**old.get("handled", {}), **handled}
    state = {"policy": POLICY, "started_at": now(), "watermark": watermark, "handled": handled}
    save_state(state)
    print(f"watermark b{watermark}; {len(handled)} pre-existing red build(s) recorded as user-handled; {path}")


def advisory_streak(rows: list[dict], row: dict, key: str) -> int:
    streak = 0
    for r in reversed([x for x in rows if int(x["number"]) <= int(row["number"]) and x.get("terminal")]):
        if key in (r.get("soft_keys") or []):
            streak += 1
        else:
            break
    return streak


def cmd_new(args) -> None:
    state = load_state()
    rows = read_rows()
    watermark = int(state.get("watermark", 0))
    handled = state.get("handled", {})
    new, advisory, running = [], [], []
    for r in rows:
        number = int(r["number"])
        if number <= watermark or str(number) in handled:
            continue
        if not r.get("terminal"):
            running.append(r)
            continue
        if is_red(r):
            new.append(r)
        elif r.get("soft_keys"):
            r = dict(r)
            r["advisory_streak"] = {k: advisory_streak(rows, r, k) for k in r["soft_keys"]}
            advisory.append(r)
    result = {"watermark": watermark, "new": new, "advisory": advisory, "running": running,
              "latest": rows[-1] if rows else None}
    if args.json:
        json.dump(result, sys.stdout, indent=1)
        sys.stdout.write("\n")
        return
    latest = result["latest"]
    if latest:
        print(f"latest standard main build: b{latest['number']} {latest['state']} {latest['commit'][:8]}")
    if not new:
        print(f"no new red builds above b{watermark}")
    for r in new:
        print(f"NEW  b{r['number']} {r['state']} {r['commit'][:8]} RED: {','.join(r['red_keys'])}  {r['web_url']}")
    for r in advisory:
        streaks = ",".join(f"{k}x{n}" for k, n in r["advisory_streak"].items())
        print(f"ADV  b{r['number']} soft-failed only: {streaks}")
    for r in running:
        print(f"RUN  b{r['number']} {r['state']} {r['commit'][:8]} ({len(r.get('running_keys') or [])} jobs running)")


def cmd_greens(args) -> None:
    rows = [r for r in read_rows() if int(r["number"]) > args.since and r.get("terminal")]
    greens = []
    for r in rows:
        if r.get("state") == "canceled":
            continue
        if is_green(r):
            greens.append(int(r["number"]))
        else:
            greens = []
    print(f"consecutive green standard main builds after b{args.since}: {len(greens)} {greens}")
    if args.key:
        again = [int(r["number"]) for r in rows if args.key in (r.get("red_keys") or [])]
        print(f"{args.key} red again in: {again or 'none'}")


def cmd_mark(args) -> None:
    if args.status not in STATUSES and not args.status.startswith("grouped-with:"):
        sys.stderr.write(f"loop_state: unknown status {args.status!r}; one of {', '.join(STATUSES)} or grouped-with:<build>\n")
        sys.exit(2)
    state = load_state()
    entry = state.setdefault("handled", {}).get(str(args.build), {})
    entry.update({
        "status": args.status,
        "keys": args.keys.split(",") if args.keys else entry.get("keys", []),
        "issue": args.issue or entry.get("issue", ""),
        "pr": args.pr or entry.get("pr", ""),
        "note": args.note if args.note is not None else entry.get("note", ""),
        "updated": now(),
    })
    state["handled"][str(args.build)] = entry
    save_state(state)
    print(f"b{args.build}: {entry['status']} {entry['issue'] or ''} {entry['pr'] or ''}".rstrip())


def cmd_show(args) -> None:
    state = load_state()
    print(f"watermark b{state.get('watermark')}  started {state.get('started_at')}  {state_path()}")
    handled = state.get("handled", {})
    for number in sorted(handled, key=int):
        e = handled[number]
        extras = " ".join(x for x in (e.get("issue"), e.get("pr")) if x)
        print(f"  b{number:<6} {e.get('status'):<22} {','.join(e.get('keys') or []) or '-':<40} {extras} {e.get('note') or ''}".rstrip())


def cmd_prune(args) -> None:
    state = load_state()
    handled = state.get("handled", {})
    keep = sorted(handled, key=int)[-args.keep:]
    before = len(handled)
    state["handled"] = {
        n: e for n, e in handled.items() if n in keep or e.get("status") not in TERMINAL_STATUSES
    }
    save_state(state)
    print(f"pruned {before - len(state['handled'])} entries; {len(state['handled'])} remain")


def cmd_log(args) -> None:
    path = state_dir() / "iterations.log"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"{now()} {args.text}\n")
    print(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="create the state file; watermark from stdin builds or --watermark")
    p.add_argument("--watermark", type=int)
    p.add_argument("--reset", action="store_true", help="move the watermark of an existing state file")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("new", help="unhandled red builds above the watermark (stdin: list --json)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_new)

    p = sub.add_parser("greens", help="consecutive green builds after a build (stdin: list --json)")
    p.add_argument("--since", type=int, required=True)
    p.add_argument("--key", help="also report whether this step key went red again")
    p.set_defaults(func=cmd_greens)

    p = sub.add_parser("mark", help="record a build's status")
    p.add_argument("build", type=int)
    p.add_argument("status")
    p.add_argument("--keys")
    p.add_argument("--issue")
    p.add_argument("--pr")
    p.add_argument("--note")
    p.set_defaults(func=cmd_mark)

    p = sub.add_parser("show", help="print the state")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("prune", help="drop old entries in terminal statuses")
    p.add_argument("--keep", type=int, default=40)
    p.set_defaults(func=cmd_prune)

    p = sub.add_parser("log", help="append one line to iterations.log")
    p.add_argument("text")
    p.set_defaults(func=cmd_log)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
