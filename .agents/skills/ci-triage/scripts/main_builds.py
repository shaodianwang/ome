#!/usr/bin/env python3
"""List and compare standard main-branch builds of the mslsrc/ome Buildkite pipeline.

Standard library only. The API token comes from $BUILDKITE_API_TOKEN or
~/.buildkite/token (mode 600); the tool refuses to run without one. It keeps
no state; `dump` writes raw build JSON under
~/.cache/ome-agent.noindex/scratch/ci-triage/ (override: OME_CITRIAGE_SCRATCH).

  main_builds.py list [--limit N] [--json] [--branch main]
  main_builds.py diff <build> [--json]
  main_builds.py dump <build>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ORG = "msl"
PIPELINE = "ome"
REPO = "mslsrc/ome"
API = f"https://api.buildkite.com/v2/organizations/{ORG}/pipelines/{PIPELINE}"

RED_STATES = {"failed", "timed_out"}
TERMINAL_BUILD_STATES = {"passed", "failed", "canceled", "blocked", "skipped", "not_run"}
_BAD_ESCAPE = re.compile(r"\\(?![\"\\/bfnrtu])")


def die(message: str, code: int = 1) -> None:
    sys.stderr.write(f"main_builds: {message}\n")
    sys.exit(code)


def load_token() -> str:
    token = os.environ.get("BUILDKITE_API_TOKEN", "").strip()
    if token:
        return token
    path = Path.home() / ".buildkite" / "token"
    if not path.is_file():
        die(
            "no Buildkite API token: export BUILDKITE_API_TOKEN or create "
            f"{path} (chmod 600) holding a user API token with read_builds and read_build_logs"
        )
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        sys.stderr.write(f"main_builds: {path} is group/world readable; run: chmod 600 {path}\n")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        die(f"{path} is empty")
    return token


_TOKEN: str | None = None


def token() -> str:
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = load_token()
    return _TOKEN


def parse_json(text: str):
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        return json.loads(_BAD_ESCAPE.sub(r"\\\\", text), strict=False)


def get_json(url: str):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token()}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return parse_json(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as err:
        if err.code == 401:
            die("Buildkite rejected the token (401): create a user API token with read_builds and store it in ~/.buildkite/token")
        die(f"GET {url} -> HTTP {err.code}")
    except urllib.error.URLError as err:
        die(f"GET {url} failed: {err.reason} (set HTTPS_PROXY if this host needs a proxy)")


def scratch_dir() -> Path:
    root = os.environ.get("OME_CITRIAGE_SCRATCH") or str(Path.home() / ".cache" / "ome-agent.noindex" / "scratch" / "ci-triage")
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


# --- build model -----------------------------------------------------------------

def fetch_builds(branch: str, limit: int) -> list[dict]:
    out: list[dict] = []
    page = 1
    while len(out) < limit:
        per_page = min(100, limit - len(out))
        query = urllib.parse.urlencode({"branch": branch, "per_page": per_page, "page": page, "include_retried_jobs": "true"})
        batch = get_json(f"{API}/builds?{query}")
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    return out[:limit]


def fetch_build(number: int) -> dict:
    return get_json(f"{API}/builds/{number}?include_retried_jobs=true")


def script_jobs(build: dict) -> list[dict]:
    return [j for j in build.get("jobs") or [] if j.get("type") == "script"]


def job_key(job: dict) -> str:
    return job.get("step_key") or job.get("name") or job.get("id", "?")[:8]


def latest_attempts(build: dict) -> tuple[list[dict], dict[str, int]]:
    """Newest attempt per (step key, parallel index); also earlier failed attempts per key."""
    newest: dict[tuple, dict] = {}
    earlier_red: dict[str, int] = {}
    for job in script_jobs(build):
        identity = (job.get("step_key") or job["id"], job.get("parallel_group_index"))
        if job.get("retried") or job.get("retried_in_job_id"):
            if job.get("state") in RED_STATES:
                earlier_red[job_key(job)] = earlier_red.get(job_key(job), 0) + 1
            continue
        rank = (job.get("retries_count") or 0, job.get("created_at") or "", job["id"])
        prev = newest.get(identity)
        if prev is None or rank > (prev.get("retries_count") or 0, prev.get("created_at") or "", prev["id"]):
            newest[identity] = job
    return list(newest.values()), earlier_red


def queue_of(job: dict) -> str:
    for rule in job.get("agent_query_rules") or []:
        if str(rule).startswith("queue="):
            return str(rule).split("=", 1)[1]
    return "?"


def summarize(build: dict) -> dict:
    jobs, earlier_red = latest_attempts(build)
    red = sorted({job_key(j) for j in jobs if j.get("state") in RED_STATES and not j.get("soft_failed")})
    soft = sorted({job_key(j) for j in jobs if j.get("state") in RED_STATES and j.get("soft_failed")})
    running = sorted({job_key(j) for j in jobs if j.get("state") in ("running", "scheduled", "assigned", "accepted", "limited", "limiting", "waiting")})
    message = (build.get("message") or "").split("\n")[0][:72]
    return {
        "number": build.get("number"),
        "state": build.get("state"),
        "terminal": build.get("state") in TERMINAL_BUILD_STATES,
        "commit": (build.get("commit") or "")[:40],
        "branch": build.get("branch"),
        "created_at": build.get("created_at"),
        "finished_at": build.get("finished_at"),
        "web_url": build.get("web_url") or f"https://buildkite.com/{ORG}/{PIPELINE}/builds/{build.get('number')}",
        "message": message,
        "red_keys": red,
        "soft_keys": soft,
        "retried_keys": dict(sorted(earlier_red.items())),
        "running_keys": running,
    }


def job_row(build_number: int, job: dict, attempts: int) -> dict:
    agent = job.get("agent") or {}
    return {
        "key": job_key(job),
        "job_id": job.get("id"),
        "state": job.get("state"),
        "exit_status": job.get("exit_status"),
        "signal_reason": job.get("signal_reason"),
        "soft_failed": bool(job.get("soft_failed")),
        "attempts": attempts,
        "retries_count": job.get("retries_count") or 0,
        "agent": agent.get("name"),
        "queue": queue_of(job),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "web_url": job.get("web_url"),
        "log_url": f"{API}/builds/{build_number}/jobs/{job.get('id')}/log.txt",
    }


# --- commands --------------------------------------------------------------------

def cmd_list(args) -> None:
    builds = fetch_builds(args.branch, args.limit)
    rows = [summarize(b) for b in builds]
    if args.json:
        json.dump(rows, sys.stdout, indent=1)
        sys.stdout.write("\n")
        return
    if not rows:
        print(f"no standard builds on {args.branch} in the last {args.limit}")
        return
    for r in rows:
        parts = [f"b{r['number']:<6}", f"{r['state']:<9}", r["commit"][:8], (r["created_at"] or "")[:16]]
        if r["red_keys"]:
            parts.append("RED: " + ",".join(r["red_keys"]))
        if r["soft_keys"]:
            parts.append("SOFT: " + ",".join(r["soft_keys"]))
        if r["retried_keys"]:
            parts.append("RETRIED: " + ",".join(f"{k}({n})" for k, n in r["retried_keys"].items()))
        if not r["terminal"] and r["running_keys"]:
            parts.append(f"RUNNING: {len(r['running_keys'])} jobs")
        parts.append(r["message"])
        print("  ".join(parts))
    print(f"\nhttps://buildkite.com/{ORG}/{PIPELINE}/builds/<n>")


def previous_standard_build(number: int, branch: str) -> dict | None:
    for build in fetch_builds(branch, 60):
        if (build.get("number") or 0) >= number:
            continue
        if build.get("state") not in TERMINAL_BUILD_STATES:
            continue
        return build
    return None


def cmd_diff(args) -> None:
    build = fetch_build(args.build)
    branch = build.get("branch") or "main"
    prev = previous_standard_build(build["number"], branch)
    jobs, earlier_red = latest_attempts(build)
    prev_state: dict[str, str] = {}
    if prev:
        prev_jobs, _ = latest_attempts(prev)
        for j in prev_jobs:
            prev_state[job_key(j)] = "red" if j.get("state") in RED_STATES and not j.get("soft_failed") else str(j.get("state"))
    attempts_by_key: dict[str, int] = {}
    for j in script_jobs(build):
        attempts_by_key[job_key(j)] = attempts_by_key.get(job_key(j), 0) + 1

    failed = []
    for j in sorted(jobs, key=job_key):
        if j.get("state") not in RED_STATES:
            continue
        row = job_row(build["number"], j, attempts_by_key.get(job_key(j), 1))
        if not prev:
            row["versus_previous"] = "no-previous-build"
        elif job_key(j) not in prev_state:
            row["versus_previous"] = "not-run"
        elif prev_state[job_key(j)] == "red":
            row["versus_previous"] = "persists"
        else:
            row["versus_previous"] = "new"
        failed.append(row)
    now_green = {job_key(j) for j in jobs if j.get("state") == "passed"}
    recovered = sorted(k for k, s in prev_state.items() if s == "red" and k in now_green)

    result = {
        "build": summarize(build),
        "previous": summarize(prev) if prev else None,
        "compare_url": (
            f"https://github.com/{REPO}/compare/{prev['commit'][:12]}...{build['commit'][:12]}" if prev else None
        ),
        "failed_jobs": failed,
        "passed_on_retry": earlier_red,
        "recovered_keys": recovered,
    }
    if args.json:
        json.dump(result, sys.stdout, indent=1)
        sys.stdout.write("\n")
        return

    b = result["build"]
    print(f"build b{b['number']} {b['state']} {b['commit'][:8]} {b['created_at']}  {b['web_url']}")
    print(f"  {b['message']}")
    if prev:
        p = result["previous"]
        print(f"previous standard build b{p['number']} {p['state']} {p['commit'][:8]}  red: {','.join(p['red_keys']) or '-'}")
        print(f"compare: {result['compare_url']}")
    else:
        print("previous standard build: none found in the last 60")
    if not failed:
        print("failed jobs (latest attempts): none")
    else:
        print("failed jobs (latest attempts):")
        for r in failed:
            flags = []
            if r["soft_failed"]:
                flags.append("soft-fail")
            if r["signal_reason"]:
                flags.append(f"signal={r['signal_reason']}")
            print(
                f"  {r['key']:<40} {r['state']:<9} exit={r['exit_status']!s:<4} attempts={r['attempts']} "
                f"queue={r['queue']} agent={r['agent'] or '-'} {r['versus_previous']} {' '.join(flags)}"
            )
            print(f"      {r['web_url']}")
    if earlier_red:
        print("passed after an earlier failed attempt: " + ", ".join(f"{k}({n})" for k, n in sorted(earlier_red.items())))
    if recovered:
        print("recovered since previous build: " + ", ".join(recovered))


def cmd_dump(args) -> None:
    build = fetch_build(args.build)
    path = scratch_dir() / f"b{args.build}.json"
    path.write_text(json.dumps(build, indent=1), encoding="utf-8")
    print(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list", help="recent standard builds on a branch with their red step keys")
    p.add_argument("--branch", default="main")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("diff", help="failed jobs of one build against the previous standard build")
    p.add_argument("build", type=int)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_diff)

    p = sub.add_parser("dump", help="save the raw build JSON to scratch")
    p.add_argument("build", type=int)
    p.set_defaults(func=cmd_dump)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
