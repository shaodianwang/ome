#!/usr/bin/env python3
"""Inspect, watch and summarize OME daily release-candidate builds on Buildkite.

Standard library only. The API token comes from $BUILDKITE_API_TOKEN or
~/.buildkite/token; the tool refuses to run without one. It writes nothing into
the repository: state, cleaned logs and downloads go under
~/.cache/ome-agent.noindex/scratch/daily-monitor/ (override: OME_DAILY_SCRATCH).

  find       recent daily builds (full and Kargo-triggered), newest first
  status     per-stage verdict for one build, decoded meta-data reports, PR link
  log        one job's log with ANSI and timestamp escapes stripped
  artifacts  list or download a job's artifacts
  classify   match every failed job's log against the known failure signatures
  release    publish stages of the standard main build for a commit or build
  watch      poll a build and exit 0 printing "WAKE: <reason>" on events
  retry      retry one failed job (explicit; prints the new job id)
  summary    evidence-linked markdown summary of one or more builds

A daily build is recognised by build env OME_PIPELINE_MODE=daily or, failing
that, by jobs whose step_key is one of the daily-* keys the daily generator
emits. A daily build without the discovery and GitOps-PR steps (or with a
truthy KARGO_BUILD env) is a Kargo-triggered, smoke-only build.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ORG = os.environ.get("OME_BK_ORG", "msl")
PIPELINE = os.environ.get("OME_BK_PIPELINE", "ome")
GITHUB_REPO = os.environ.get("OME_GITHUB_REPO", "mslsrc/ome")
SCRATCH = Path(
    os.environ.get(
        "OME_DAILY_SCRATCH",
        Path.home() / ".cache" / "ome-agent.noindex" / "scratch" / "daily-monitor",
    )
)

# (stage, step key) in pipeline order; keys come from generate_daily_pipeline.py.
DAILY_STAGES = (
    ("discovery", "daily-discovery"),
    ("cpu-smoke", "daily-rc-smoke"),
    ("tpu-default-scheduler", "daily-rc-tpu-smoke"),
    ("tpu-ome-scheduler", "daily-rc-tpu-ome-scheduler-smoke"),
    ("gitops-pr", "daily-gitops-pr"),
)
DAILY_KEYS = {key for _, key in DAILY_STAGES}
SMOKE_KEYS = {"daily-rc-smoke", "daily-rc-tpu-smoke", "daily-rc-tpu-ome-scheduler-smoke"}

# Build meta-data the daily scripts publish (buildkite-agent meta-data set).
META_KEYS = (
    ("daily-candidate-b64", "candidate", "json"),
    ("daily-rc-smoke-b64", "cpu-smoke report", "text"),
    ("daily-rc-tpu-smoke-b64", "tpu-default-scheduler report", "text"),
    ("daily-rc-tpu-ome-scheduler-smoke-b64", "tpu-ome-scheduler report", "text"),
    ("daily-gitops-pr-url", "gitops PR", "raw"),
)

# The standard main pipeline's publish stages that make a commit discoverable.
RELEASE_IMAGES = ("ome-manager", "ome-agent", "ome-scheduler", "ome-quota-manager")
RELEASE_CHARTS = ("ome-crd", "ome-resources", "ome-scheduler", "ome-quota-manager")
RELEASE_MARKER_KEY = "chart-ome-release"

FAIL_STATES = {"failed", "timed_out", "errored", "broken"}
BAD_STATES = FAIL_STATES | {"waiting_failed", "blocked_failed", "canceled"}
ACTIVE_STATES = {
    "running", "canceling", "timing_out", "scheduled", "waiting", "limited",
    "assigned", "accepted", "pending",
}
BUILD_TERMINAL = {"passed", "failed", "canceled", "blocked", "not_run"}
FAIL_VERDICTS = {"FAIL", "TIMEOUT", "ERRORED", "BROKEN", "CANCELED"}

# Ordered failure signatures: (regex, class, disposition). Specific before
# generic; the doc references/failure-classes.md explains each class.
SIGNATURES = (
    (r"failed to connect to the docker API", "infra/docker-daemon",
     "agent-side Docker daemon died; retry the job once"),
    (r"ecr_login_for: no ECR credential source produced a token", "infra/registry-auth",
     "no registry credential on the agent; retry once, twice in a row means the secret is broken"),
    (r"KARGO_BUILD(_COMMIT)? (requires|must)", "trigger/misconfigured",
     "Kargo trigger passed a bad commit; fix the trigger, do not retry"),
    (r"no complete OME release has all required charts and images", "release/incomplete-publish",
     "no main build has all four charts and four images; check `release` for the newest main commit"),
    (r"missing permission: ", "env/rbac",
     "the smoke ServiceAccount lost a grant in the RC cluster; not retryable"),
    (r"RC did not converge to the requested release within", "env/rc-not-on-candidate",
     "RC is not running the candidate; compare the expected/live lines, wait for RC to sync, then retry"),
    (r"new RC version detected while the smoke test was running", "env/rc-release-changed-midrun",
     "RC rolled to another release mid-run; retry once RC is stable"),
    (r"RC candidate rollout is no longer healthy", "needs-evidence/rc-rollout-unhealthy",
     "manager or scheduler rollout regressed during the smoke; inspect their pods before deciding"),
    (r"TPU smoke fixture does not exist", "product/fixture-missing",
     "repository fixture path changed; open an issue"),
    (r'(configmaps?|servingruntimes?\.ome\.io) "[^"]+" not found', "env/rc-prerequisites",
     "an RC-managed runtime or configmap is missing; not retryable"),
    (r"timed out waiting for production readiness", "needs-evidence/tpu-not-ready",
     "read final-state.log, events.log and pod logs: capacity is env, crash loops are product"),
    (r"unexpected (TPU gang shape|engine scheduler|router scheduler)", "product/gang-shape",
     "controller or scheduler behavior regressed; open an issue, do not retry"),
    (r"OME did not create the BaseModel metadata Job within", "product/basemodel-controller",
     "controller did not act on the BaseModel; open an issue"),
    (r"metadata Job image mismatch", "product/agent-image-mismatch",
     "controller rendered an agent image other than the configured one; open an issue"),
    (r"BaseModel did not become Ready within", "needs-evidence/basemodel-not-ready",
     "metadata Job ran but status did not converge; read the Job pod log in the same output"),
    (r"OME did not create an InferenceService pod within", "product/isvc-controller",
     "controller did not create the pod; open an issue"),
    (r"uses scheduler \S+, expected ome-scheduler", "product/scheduler-name",
     "runtime schedulerName not honoured; open an issue"),
    (r"GH_TOKEN is required", "infra/secret-missing",
     "GitOps PR step has no GitHub token; escalate CI infra"),
    (r"(GitHub pull (lookup|request operation)|OME comparison lookup) failed with HTTP (\d+)",
     "infra/github-api",
     "401/403 is a token scope problem (escalate); 5xx or timeout is transient (retry)"),
    (r"unexpected GitOps paths changed", "product/gitops-layout-drift",
     "the pin script and the GitOps layout disagree; open an issue"),
    (r"argo-gitops main already pins OME release", "pass/already-pinned",
     "no-op: production already pins this release"),
    (r"argo-gitops read access: unavailable", "env/gitops-read-access",
     "discovery could not reach the GitOps repo; the PR step will likely fail too"),
    (r"Insufficient google\.com/tpu|0/\d+ nodes are available", "env/tpu-capacity",
     "no schedulable TPU slice in RC; wait, do not retry into an empty pool"),
    (r"ImagePullBackOff|ErrImagePull|failed to pull image", "needs-evidence/image-pull",
     "check that the candidate image tags exist in the registry the RC pulls from"),
    (r"CrashLoopBackOff|OOMKilled", "needs-evidence/crashloop",
     "a smoke workload or control-plane pod is crash looping; read its log"),
    (r"timed out waiting for the condition", "needs-evidence/kubectl-wait-timeout",
     "a kubectl wait expired; the describe output below it says which resource and why"),
)

TOKEN = ""


# ---------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------


def die(message: str, code: int = 1) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(code)


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def load_token() -> str:
    token = os.environ.get("BUILDKITE_API_TOKEN", "").strip()
    if token:
        return token
    path = Path.home() / ".buildkite" / "token"
    if not path.is_file():
        die(
            "no Buildkite API token: export BUILDKITE_API_TOKEN or create "
            f"{path} (chmod 600) with a token that has read_builds "
            "(and write_builds for `retry`)"
        )
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        warn(f"{path} is mode {mode:o}; run: chmod 600 {path}")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        die(f"{path} is empty")
    return token


def api_root() -> str:
    return f"https://api.buildkite.com/v2/organizations/{ORG}/pipelines/{PIPELINE}"


def build_web_url(number: int | str) -> str:
    return f"https://buildkite.com/{ORG}/{PIPELINE}/builds/{number}"


def commit_url(sha: str) -> str:
    return f"https://github.com/{GITHUB_REPO}/commit/{sha}"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802
        return None


def http(
    method: str,
    url: str,
    accept: str = "application/json",
    body: dict | None = None,
    follow_redirects: bool = True,
) -> tuple[int, bytes, dict]:
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": accept,
        "User-Agent": "ome-daily-build/1",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    opener = urllib.request.build_opener() if follow_redirects else urllib.request.build_opener(_NoRedirect)
    for attempt in range(4):
        try:
            with opener.open(request, timeout=90) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as err:
            if err.code == 429 and attempt < 3:
                time.sleep(5 * (attempt + 1))
                continue
            return err.code, err.read(), dict(err.headers)
        except urllib.error.URLError as err:
            if attempt < 3:
                time.sleep(3 * (attempt + 1))
                continue
            die(f"{method} {url}: {err}")
    die(f"{method} {url}: gave up after retries")
    return 0, b"", {}


_BAD_ESCAPE = re.compile(r'\\(?!["\\/bfnrtu])')


def parse_json(text: str):
    """Buildkite job payloads may carry raw control characters; never use strict mode."""
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        return json.loads(_BAD_ESCAPE.sub(r"\\\\", text), strict=False)


def get_json(url: str):
    status, raw, _ = http("GET", url)
    if status >= 400:
        die(f"GET {url} -> HTTP {status}: {raw[:300].decode('utf-8', 'replace')}")
    return parse_json(raw.decode("utf-8", "replace"))


_ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_BK_TIMESTAMP = re.compile(r"\x1b_bk;t=\d+\x07")
_OTHER_ESCAPES = re.compile(r"\x1b[^\[_]")


def clean_log(text: str) -> str:
    text = _BK_TIMESTAMP.sub("", text)
    text = _ANSI.sub("", text)
    text = _OTHER_ESCAPES.sub("", text)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def fmt_duration(start: str | None, end: str | None) -> str:
    a, b = parse_time(start), parse_time(end)
    if not a:
        return "-"
    b = b or dt.datetime.now(dt.timezone.utc)
    seconds = int((b - a).total_seconds())
    if seconds < 0:
        return "-"
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def truthy(value) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def ensure_scratch() -> Path:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    return SCRATCH


def load_state(path: Path, default: dict) -> dict:
    if path.is_file():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            warn(f"{path} is not valid JSON; starting fresh")
    return dict(default)


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Build model
# ---------------------------------------------------------------------------


def fetch_build(number: int | str, retried: bool = True) -> dict:
    query = "?include_retried_jobs=true" if retried else ""
    return get_json(f"{api_root()}/builds/{number}{query}")


def script_jobs(build: dict) -> list[dict]:
    return [j for j in build.get("jobs") or [] if j.get("type") == "script"]


def step_keys(build: dict) -> set[str]:
    return {j.get("step_key") for j in build.get("jobs") or [] if j.get("step_key")}


def daily_mode(build: dict) -> str | None:
    """'full' (discovery + smokes + PR), 'kargo' (smokes only) or None."""
    env = build.get("env") or {}
    mode = str(env.get("OME_PIPELINE_MODE", "")).strip().lower()
    keys = step_keys(build)
    if mode != "daily" and not (keys & DAILY_KEYS):
        return None
    if truthy(env.get("KARGO_BUILD")):
        return "kargo"
    if keys & DAILY_KEYS and "daily-discovery" not in keys and "daily-gitops-pr" not in keys:
        return "kargo"
    return "full"


def attempts_for(build: dict, key: str) -> list[dict]:
    jobs = [j for j in script_jobs(build) if j.get("step_key") == key]
    return sorted(jobs, key=lambda j: j.get("created_at") or "")


def latest_attempt(attempts: list[dict]) -> dict | None:
    if not attempts:
        return None
    for job in reversed(attempts):
        if not job.get("retried"):
            return job
    return attempts[-1]


def verdict_of(job: dict | None) -> str:
    if job is None:
        return "absent"
    state = job.get("state")
    if state == "passed":
        return "PASS"
    if state in FAIL_STATES:
        if job.get("soft_failed"):
            return "SOFT-FAIL"
        return {"failed": "FAIL", "timed_out": "TIMEOUT", "errored": "ERRORED", "broken": "BROKEN"}[state]
    if state in ("waiting_failed", "blocked_failed"):
        return "SKIPPED"
    if state == "canceled":
        return "CANCELED"
    if state in ("running", "canceling", "timing_out"):
        return "RUNNING"
    if state == "limited":
        return "QUEUED(concurrency)"
    if state in ("scheduled", "waiting", "assigned", "accepted", "pending", "blocked"):
        return "WAITING"
    return state or "?"


def stage_rows(build: dict) -> list[dict]:
    rows = []
    for stage, key in DAILY_STAGES:
        attempts = attempts_for(build, key)
        job = latest_attempt(attempts)
        row = {"stage": stage, "key": key, "present": job is not None, "verdict": verdict_of(job)}
        if job is not None:
            agent = job.get("agent") or {}
            row.update(
                job_id=job.get("id"),
                state=job.get("state"),
                exit_status=job.get("exit_status"),
                soft_failed=bool(job.get("soft_failed")),
                attempts=len(attempts),
                started_at=job.get("started_at"),
                finished_at=job.get("finished_at"),
                duration=fmt_duration(job.get("started_at"), job.get("finished_at")),
                web_url=job.get("web_url"),
                agent=agent.get("name"),
                prior_attempts=[
                    {"job_id": a.get("id"), "state": a.get("state"), "exit_status": a.get("exit_status")}
                    for a in attempts
                    if a.get("id") != job.get("id")
                ],
            )
        rows.append(row)
    return rows


def build_verdict(build: dict, rows: list[dict]) -> str:
    present = [r for r in rows if r["present"]]
    if any(r["verdict"] in FAIL_VERDICTS for r in present):
        return "FAIL"
    if present and all(r["verdict"] == "PASS" for r in present):
        return "PASS"
    state = build.get("state")
    if state in BUILD_TERMINAL:
        return str(state).upper()
    return "IN-PROGRESS"


def decode_meta(build: dict) -> dict:
    meta = build.get("meta_data") or {}
    out = {}
    for key, label, kind in META_KEYS:
        if key not in meta:
            continue
        raw = meta[key]
        if kind == "raw":
            out[label] = raw
            continue
        try:
            text = base64.b64decode(raw).decode("utf-8", "replace")
        except (ValueError, TypeError):
            out[label] = f"<undecodable meta-data {key}>"
            continue
        if kind == "json":
            try:
                out[label] = json.loads(text)
            except json.JSONDecodeError:
                out[label] = text
        else:
            out[label] = text.rstrip("\n")
    return out


def fetch_annotations(number: int | str) -> list[dict]:
    status, raw, _ = http("GET", f"{api_root()}/builds/{number}/annotations")
    if status >= 400:
        return []
    return parse_json(raw.decode("utf-8", "replace"))


def compact_stages(rows: list[dict]) -> str:
    short = {
        "discovery": "disc", "cpu-smoke": "cpu", "tpu-default-scheduler": "tpu-def",
        "tpu-ome-scheduler": "tpu-ome", "gitops-pr": "pr",
    }
    return " ".join(f"{short[r['stage']]}:{r['verdict']}" for r in rows if r["present"])


def candidate_sha(build: dict) -> str:
    meta = decode_meta(build)
    cand = meta.get("candidate")
    if isinstance(cand, dict):
        return str(cand.get("short_sha") or "")
    env = build.get("env") or {}
    if truthy(env.get("KARGO_BUILD")):
        return str(build.get("commit") or "")[:8]
    return ""


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def iter_builds(days: float, branch: str | None, per_page: int = 50):
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).replace(microsecond=0)
    page = 1
    while True:
        params = {"per_page": per_page, "page": page, "created_from": since.isoformat()}
        if branch:
            params["branch"] = branch
        builds = get_json(f"{api_root()}/builds?{urllib.parse.urlencode(params)}")
        if not builds:
            return
        for build in builds:
            yield build
        if len(builds) < per_page:
            return
        page += 1


def cmd_find(args) -> None:
    found = []
    for build in iter_builds(args.days, args.branch):
        mode = daily_mode(build)
        if mode is None:
            continue
        rows = stage_rows(build)
        found.append(
            {
                "number": build.get("number"),
                "mode": mode,
                "state": build.get("state"),
                "verdict": build_verdict(build, rows),
                "source": build.get("source"),
                "branch": build.get("branch"),
                "commit": (build.get("commit") or "")[:8],
                "candidate": candidate_sha(build),
                "created_at": build.get("created_at"),
                "message": (build.get("message") or "").splitlines()[0][:80] if build.get("message") else "",
                "stages": compact_stages(rows),
                "pr_url": (build.get("meta_data") or {}).get("daily-gitops-pr-url"),
                "web_url": build.get("web_url") or build_web_url(build.get("number")),
            }
        )
        if len(found) >= args.limit:
            break
    if args.json:
        print(json.dumps(found, indent=2))
        return
    if not found:
        print(f"no daily builds in the last {args.days:g} day(s) on {ORG}/{PIPELINE}")
        return
    for b in found:
        print(
            f"b{b['number']:<6} {b['mode']:<5} {b['verdict']:<11} {b['state']:<9} "
            f"{(b['created_at'] or '')[:16]:<16} commit={b['commit']:<8} "
            f"candidate={b['candidate'] or '-':<8} {b['stages']}"
        )
        if b["pr_url"]:
            print(f"        promotion PR: {b['pr_url']}")
        print(f"        {b['web_url']}  {b['message']}")


def print_status(build: dict, rows: list[dict], annotations: list[dict]) -> None:
    number = build.get("number")
    mode = daily_mode(build)
    print(f"build b{number}  mode={mode or 'not-daily'}  state={build.get('state')}  verdict={build_verdict(build, rows)}")
    print(f"  {build.get('web_url') or build_web_url(number)}")
    print(
        f"  branch={build.get('branch')} commit={(build.get('commit') or '')[:8]} "
        f"source={build.get('source')} created={build.get('created_at')}"
    )
    env = build.get("env") or {}
    shown = {k: env[k] for k in ("OME_PIPELINE_MODE", "KARGO_BUILD", "KARGO_BUILD_COMMIT") if k in env}
    if shown:
        print(f"  env={shown}")
    print()
    print(f"  {'stage':<24}{'verdict':<20}{'exit':<6}{'dur':<9}{'att':<5}job")
    for r in rows:
        if not r["present"]:
            print(f"  {r['stage']:<24}{'absent':<20}{'-':<6}{'-':<9}{'-':<5}(step not in this build)")
            continue
        exit_status = "-" if r.get("exit_status") is None else str(r["exit_status"])
        print(
            f"  {r['stage']:<24}{r['verdict']:<20}{exit_status:<6}{r['duration']:<9}"
            f"{r['attempts']:<5}{r['web_url']}"
        )
        for prior in r.get("prior_attempts") or []:
            print(f"  {'':<24}earlier attempt {prior['job_id']} state={prior['state']} exit={prior['exit_status']}")
    meta = decode_meta(build)
    if meta:
        print()
        print("  meta-data:")
        cand = meta.get("candidate")
        if isinstance(cand, dict):
            print(f"    candidate short_sha={cand.get('short_sha')} registry={cand.get('registry')}")
            for chart, version in sorted((cand.get("charts") or {}).items()):
                print(f"      chart {chart}={version}")
            for image, ref in sorted((cand.get("images") or {}).items()):
                print(f"      image {image}={ref}")
        elif cand is not None:
            print(f"    candidate (raw): {cand}")
        for label in ("cpu-smoke report", "tpu-default-scheduler report", "tpu-ome-scheduler report"):
            if label in meta:
                print(f"    {label}:")
                for line in str(meta[label]).splitlines():
                    print(f"      {line}")
        if "gitops PR" in meta:
            print(f"    promotion PR: {meta['gitops PR']}")
    relevant = [a for a in annotations if a.get("context") in ("daily-gitops-pr",)]
    if relevant:
        print()
        for a in relevant:
            body = re.sub(r"<[^>]+>", "", a.get("body_html") or "").strip()
            print(f"  annotation[{a.get('context')}] ({a.get('style')}): {body}")


def cmd_status(args) -> None:
    build = fetch_build(args.build)
    rows = stage_rows(build)
    annotations = fetch_annotations(args.build)
    if args.json:
        print(
            json.dumps(
                {
                    "number": build.get("number"),
                    "mode": daily_mode(build),
                    "state": build.get("state"),
                    "verdict": build_verdict(build, rows),
                    "branch": build.get("branch"),
                    "commit": build.get("commit"),
                    "source": build.get("source"),
                    "created_at": build.get("created_at"),
                    "web_url": build.get("web_url") or build_web_url(build.get("number")),
                    "env": {k: v for k, v in (build.get("env") or {}).items() if k.startswith(("OME_", "KARGO_"))},
                    "stages": rows,
                    "meta": decode_meta(build),
                    "annotations": [
                        {"context": a.get("context"), "style": a.get("style"), "body_html": a.get("body_html")}
                        for a in annotations
                    ],
                },
                indent=2,
            )
        )
        return
    if daily_mode(build) is None:
        warn(f"b{args.build} does not look like a daily build (no OME_PIPELINE_MODE=daily env and no daily-* steps)")
    print_status(build, rows, annotations)


def resolve_job(build: dict, ref: str, attempt: int | None = None) -> dict:
    for job in build.get("jobs") or []:
        if job.get("id") == ref:
            return job
    attempts = attempts_for(build, ref)
    if not attempts:
        keys = sorted(step_keys(build))
        die(f"no job with id or step_key {ref!r} in b{build.get('number')}; step keys: {', '.join(keys)}")
    if attempt is not None:
        if attempt < 1 or attempt > len(attempts):
            die(f"{ref} has {len(attempts)} attempt(s); --attempt must be 1..{len(attempts)}")
        return attempts[attempt - 1]
    return latest_attempt(attempts)


def fetch_log(number: int | str, job: dict) -> str:
    url = job.get("raw_log_url") or f"{api_root()}/builds/{number}/jobs/{job['id']}/log.txt"
    status, raw, _ = http("GET", url, accept="text/plain")
    if status >= 400:
        die(f"GET {url} -> HTTP {status}")
    return clean_log(raw.decode("utf-8", "replace"))


def cmd_log(args) -> None:
    build = fetch_build(args.build)
    job = resolve_job(build, args.job, args.attempt)
    text = fetch_log(args.build, job)
    scratch = ensure_scratch()
    saved = scratch / f"b{args.build}-{job.get('step_key') or 'job'}-{job['id'][:8]}.log"
    saved.write_text(text, encoding="utf-8")
    lines = text.splitlines()
    print(f"# job {job['id']} step={job.get('step_key')} state={job.get('state')} exit={job.get('exit_status')}")
    print(f"# {job.get('web_url')}")
    print(f"# {len(lines)} lines saved to {saved}")
    if args.grep:
        pattern = re.compile(args.grep, re.IGNORECASE)
        for index, line in enumerate(lines, 1):
            if pattern.search(line):
                print(f"{index}: {line}")
        return
    for line in lines[-args.tail:] if args.tail else lines:
        print(line)


def fetch_artifacts(number: int | str, job_id: str) -> list[dict]:
    status, raw, _ = http("GET", f"{api_root()}/builds/{number}/jobs/{job_id}/artifacts")
    if status >= 400:
        return []
    return parse_json(raw.decode("utf-8", "replace"))


def download_artifact(artifact: dict, dest: Path) -> None:
    status, raw, headers = http("GET", artifact["download_url"], accept="*/*", follow_redirects=False)
    if status in (301, 302, 303, 307, 308):
        location = headers.get("Location") or headers.get("location")
        if not location:
            die(f"redirect without Location for {artifact.get('path')}")
        request = urllib.request.Request(location, headers={"User-Agent": "ome-daily-build/1"})
        with urllib.request.urlopen(request, timeout=300) as response:
            raw = response.read()
    elif status >= 400:
        die(f"download {artifact.get('path')} -> HTTP {status}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(raw)


def cmd_artifacts(args) -> None:
    build = fetch_build(args.build)
    jobs = [resolve_job(build, args.job)] if args.job else [
        latest_attempt(attempts_for(build, key)) for _, key in DAILY_STAGES if attempts_for(build, key)
    ]
    for job in jobs:
        if job is None:
            continue
        artifacts = fetch_artifacts(args.build, job["id"])
        print(f"{job.get('step_key')} ({job['id']}): {len(artifacts)} artifact(s)")
        for artifact in artifacts:
            print(f"  {artifact.get('path')}  {artifact.get('file_size')}B  {artifact.get('state')}")
            if args.download:
                target = Path(args.download) / f"b{args.build}" / (job.get("step_key") or job["id"]) / artifact.get("path", "artifact")
                download_artifact(artifact, target)
                print(f"    -> {target}")


def classify_text(text: str) -> list[dict]:
    hits = []
    lines = text.splitlines()
    for pattern, klass, action in SIGNATURES:
        regex = re.compile(pattern)
        for index, line in enumerate(lines, 1):
            match = regex.search(line)
            if match:
                hits.append({"line": index, "class": klass, "action": action, "text": line.strip()[:200]})
                break
    hits.sort(key=lambda h: h["line"])
    return hits


def job_level_signals(job: dict) -> list[dict]:
    signals = []
    if job.get("exit_status") == 125:
        signals.append({"class": "infra/docker-daemon", "action": "exit 125: agent-side Docker daemon failure; retry once"})
    if job.get("exit_status") == -1 or job.get("state") == "canceled":
        signals.append({"class": "infra/agent-lost-or-canceled", "action": "no exit code: agent lost, step timeout or manual cancel; retry once if not canceled by a person"})
    if job.get("state") == "timed_out":
        signals.append({"class": "timeout/step", "action": "hit the step's timeout_in_minutes; find what it was waiting on before retrying"})
    if job.get("state") in ("waiting_failed", "blocked_failed"):
        signals.append({"class": "skipped/upstream-failed", "action": "never ran: a dependency failed; nothing to retry here"})
    return signals


def cmd_classify(args) -> None:
    build = fetch_build(args.build)
    results = []
    for _, key in DAILY_STAGES:
        attempts = attempts_for(build, key)
        if not attempts:
            continue
        for job in attempts:
            if job.get("state") not in BAD_STATES:
                continue
            entry = {
                "step_key": key,
                "job_id": job["id"],
                "state": job.get("state"),
                "exit_status": job.get("exit_status"),
                "retried": bool(job.get("retried")),
                "web_url": job.get("web_url"),
                "signals": job_level_signals(job),
                "matches": [],
                "tail": [],
            }
            if job.get("state") not in ("waiting_failed", "blocked_failed"):
                text = fetch_log(args.build, job)
                entry["matches"] = classify_text(text)
                nonblank = [l for l in text.splitlines() if l.strip()]
                entry["tail"] = nonblank[-args.tail:]
            results.append(entry)
    if args.json:
        print(json.dumps(results, indent=2))
        return
    if not results:
        print(f"b{args.build}: no failed, canceled or skipped daily jobs")
        return
    for entry in results:
        flag = " (superseded by a retry)" if entry["retried"] else ""
        print(f"== {entry['step_key']} {entry['job_id']} state={entry['state']} exit={entry['exit_status']}{flag}")
        print(f"   {entry['web_url']}")
        for signal in entry["signals"]:
            print(f"   signal  {signal['class']}: {signal['action']}")
        for hit in entry["matches"]:
            print(f"   line {hit['line']:<6} {hit['class']}: {hit['text']}")
            print(f"   {'':<11} -> {hit['action']}")
        if entry["matches"]:
            print(f"   last signature hit: {entry['matches'][-1]['class']} (the fatal error is usually the last one)")
        elif entry["tail"]:
            print("   no known signature; classify by hand from the log tail:")
        for line in entry["tail"]:
            print(f"   | {line[:200]}")
        print()


def find_main_build(ref: str) -> dict:
    if ref.isdigit():
        return fetch_build(ref)
    ref = ref.lower()
    if not re.fullmatch(r"[0-9a-f]{7,40}", ref):
        die("release takes a build number or a commit SHA (7-40 hex chars)")
    if len(ref) == 40:
        builds = get_json(f"{api_root()}/builds?{urllib.parse.urlencode({'commit': ref, 'branch': 'main', 'per_page': 5})}")
        if builds:
            return fetch_build(builds[0]["number"])
    for build in iter_builds(days=45, branch="main"):
        if str(build.get("commit") or "").startswith(ref):
            return fetch_build(build["number"])
    die(f"no main build found for commit {ref} in the last 45 days")
    return {}


def cmd_release(args) -> None:
    build = find_main_build(args.ref)
    sha = str(build.get("commit") or "")
    short = sha[:8]
    keys = ["test"]
    keys += [f"build-{i}" for i in RELEASE_IMAGES] + [f"mirror-{i}" for i in RELEASE_IMAGES]
    keys += [f"chart-{c}" for c in RELEASE_CHARTS] + [RELEASE_MARKER_KEY]
    rows = []
    for key in keys:
        job = latest_attempt(attempts_for(build, key))
        rows.append({"key": key, "verdict": verdict_of(job), "web_url": job.get("web_url") if job else None, "exit_status": job.get("exit_status") if job else None})
    publish = [r for r in rows if r["key"] != "test"]
    complete = all(r["verdict"] == "PASS" for r in publish if r["key"] != RELEASE_MARKER_KEY)
    marker = next(r for r in rows if r["key"] == RELEASE_MARKER_KEY)
    if args.json:
        print(json.dumps({"number": build.get("number"), "commit": sha, "branch": build.get("branch"), "state": build.get("state"), "web_url": build.get("web_url"), "stages": rows, "charts_and_images_complete": complete, "marker": marker["verdict"]}, indent=2))
        return
    print(f"main build b{build.get('number')} state={build.get('state')} branch={build.get('branch')} commit={short}")
    print(f"  {build.get('web_url') or build_web_url(build.get('number'))}")
    print(f"  {commit_url(sha)}")
    print()
    for r in rows:
        print(f"  {r['key']:<28}{r['verdict']:<12}{r['web_url'] or ''}")
    print()
    print(f"  discoverable candidate (4 charts + 4 images published): {'yes' if complete else 'no'}")
    print(f"  release marker ({RELEASE_MARKER_KEY}, runs only after every hard step passed): {marker['verdict']}")
    print(f"  expected identities: image tag {short}; chart version 1.0.0-<UTC ts>.g{short}; marker version 1.0.0-<commit ts>.g{short}")
    if build.get("branch") != "main":
        print("  note: not a main build; publish steps skip pushes off main, so nothing here is promotable")


def cmd_watch(args) -> None:
    scratch = ensure_scratch()
    state_path = scratch / f"b{args.build}-watch-state.json"
    state = load_state(state_path, {"known_bad": None, "known_retried": None, "watch_ids": {}, "pr_seen": False})
    deadline = time.time() + args.max_hours * 3600
    iteration = 0
    while time.time() < deadline:
        iteration += 1
        build = fetch_build(args.build)
        jobs = script_jobs(build)
        rows = stage_rows(build)
        dirty = False
        if state.get("known_bad") is None:
            state["known_bad"] = sorted(j["id"] for j in jobs if j.get("state") in BAD_STATES)
            dirty = True
            if state["known_bad"]:
                print(f"baseline: {len(state['known_bad'])} already-bad job(s) recorded as known; triage them first", flush=True)
        if state.get("known_retried") is None:
            state["known_retried"] = sorted(j["id"] for j in jobs if j.get("retried"))
            dirty = True
        if dirty:
            save_state(state_path, state)
        known_bad = set(state["known_bad"])
        new_bad = [j for j in jobs if j.get("state") in BAD_STATES and j["id"] not in known_bad and not j.get("retried")]
        retried_now = {j["id"] for j in jobs if j.get("retried")}
        fresh_retries = sorted(retried_now - set(state["known_retried"]))
        watched = state.get("watch_ids") or {}
        watched_states = {jid: next((j.get("state") for j in jobs if j.get("id") == jid), "?") for jid in watched}
        watched_done = bool(watched) and all(s not in ACTIVE_STATES and s != "?" for s in watched_states.values())
        pr_url = (build.get("meta_data") or {}).get("daily-gitops-pr-url")
        print(
            f"iter {iteration} {dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%SZ')} build={build.get('state')} "
            f"{compact_stages(rows)} watched={watched_states or '-'}",
            flush=True,
        )
        if new_bad:
            detail = "\n".join(
                f"  [{j.get('state')}] {j.get('step_key')} id={j['id']} exit={j.get('exit_status')} {j.get('web_url')}"
                for j in new_bad
            )
            state["known_bad"] = sorted(known_bad | {j["id"] for j in new_bad})
            save_state(state_path, state)
            wake("new failed job(s)", detail)
        if fresh_retries:
            state["known_retried"] = sorted(retried_now)
            save_state(state_path, state)
            untriaged = [jid for jid in fresh_retries if jid not in known_bad]
            if untriaged:
                wake("retry fired on an untriaged job", "\n".join(f"  original job {jid}" for jid in untriaged))
        if watched_done:
            state["watch_ids"] = {}
            save_state(state_path, state)
            wake("watched retry attempt(s) finished", "\n".join(f"  {jid}: {s}" for jid, s in watched_states.items()))
        if pr_url and not state.get("pr_seen"):
            state["pr_seen"] = True
            save_state(state_path, state)
            wake("promotion PR opened; the pipeline never merges it and neither do you", f"  {pr_url}")
        if build.get("state") in BUILD_TERMINAL:
            wake(f"build settled: {build.get('state')} ({build_verdict(build, rows)})", f"  {build.get('web_url') or build_web_url(args.build)}")
        time.sleep(args.interval)
    wake(f"watch window of {args.max_hours:g}h elapsed", "build still running; relaunch to keep watching")


def wake(reason: str, detail: str = "") -> None:
    print(f"WAKE: {reason}")
    if detail:
        print(detail)
    raise SystemExit(0)


def cmd_retry(args) -> None:
    build = fetch_build(args.build)
    job = resolve_job(build, args.job)
    if job.get("retried"):
        die(f"job {job['id']} was already retried (into {job.get('retried_in_job_id')}); retry the newest attempt instead")
    if job.get("state") not in BAD_STATES and not args.force:
        die(f"job {job['id']} is {job.get('state')}, not failed; pass --force if you really mean it")
    if job.get("step_key") == "daily-discovery":
        ran = [k for k in SMOKE_KEYS if any(a.get("state") not in ("waiting_failed", "blocked_failed", None) for a in attempts_for(build, k))]
        if ran and not args.force:
            die("refusing to retry daily-discovery after a smoke already ran: a re-discovered, newer candidate would be promoted on smokes that tested the old one; start a fresh daily build instead")
    status, raw, _ = http("PUT", f"{api_root()}/builds/{args.build}/jobs/{job['id']}/retry")
    if status >= 400:
        die(f"retry -> HTTP {status}: {raw[:300].decode('utf-8', 'replace')}")
    new_job = parse_json(raw.decode("utf-8", "replace"))
    scratch = ensure_scratch()
    state_path = scratch / f"b{args.build}-watch-state.json"
    state = load_state(state_path, {"known_bad": None, "known_retried": None, "watch_ids": {}, "pr_seen": False})
    state.setdefault("watch_ids", {})[new_job.get("id")] = job.get("step_key")
    if state.get("known_bad") is not None and job["id"] not in state["known_bad"]:
        state["known_bad"].append(job["id"])
    save_state(state_path, state)
    print(f"retried {job.get('step_key')} {job['id']} -> new job {new_job.get('id')} state={new_job.get('state')}")
    print(f"  {new_job.get('web_url')}")
    print("  a retry cannot fix a product regression; if this is the second retry of the same failure, stop and classify")


def markdown_summary(builds: list[dict]) -> str:
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    out = [f"# OME daily RC triage — {today}", "", "**<one sentence: what was promoted, what is broken, what needs a person>**", ""]
    out += ["## Builds", "", "| Build | Mode | Commit | Candidate | Verdict | Stages | Promotion PR |", "|---|---|---|---|---|---|---|"]
    for build in builds:
        rows = stage_rows(build)
        sha = str(build.get("commit") or "")
        cand = candidate_sha(build)
        pr = (build.get("meta_data") or {}).get("daily-gitops-pr-url")
        out.append(
            f"| [b{build.get('number')}]({build.get('web_url') or build_web_url(build.get('number'))}) | {daily_mode(build) or 'n/a'} "
            f"| [{sha[:8]}]({commit_url(sha)}) | `{cand or '-'}` | {build_verdict(build, rows)} | {compact_stages(rows)} "
            f"| {'[PR](' + pr + ')' if pr else '-'} |"
        )
    out += ["", "## Stages", ""]
    out += ["| Build | Stage | Verdict | Exit | Duration | Attempts | Job |", "|---|---|---|---|---|---|---|"]
    for build in builds:
        for r in stage_rows(build):
            if not r["present"]:
                continue
            exit_status = "-" if r.get("exit_status") is None else r["exit_status"]
            out.append(f"| b{build.get('number')} | {r['stage']} | {r['verdict']} | {exit_status} | {r['duration']} | {r['attempts']} | [log]({r['web_url']}) |")
    out += ["", "## Failures and dispositions", "", "| Build / stage | Class | Evidence | Disposition |", "|---|---|---|---|", "| b<N> / <stage> | <infra or product class> | <log line + link, artifact> | <retried -> result / parked: why / issue link> |", ""]
    out += ["## Reports (from build meta-data)", ""]
    for build in builds:
        meta = decode_meta(build)
        for label in ("cpu-smoke report", "tpu-default-scheduler report", "tpu-ome-scheduler report"):
            if label in meta:
                out += [f"### b{build.get('number')} {label}", "", "```"] + str(meta[label]).splitlines() + ["```", ""]
        cand = meta.get("candidate")
        if isinstance(cand, dict):
            out += [f"### b{build.get('number')} candidate", "", "```"]
            out += [f"short_sha: {cand.get('short_sha')}"]
            out += [f"chart {k}: {v}" for k, v in sorted((cand.get("charts") or {}).items())]
            out += [f"image {k}: {v}" for k, v in sorted((cand.get("images") or {}).items())]
            out += ["```", ""]
    out += ["## Actions", "", "- <at most 5 one-liners; link retries, issues, PRs>", "", "## Waiting on", "", "- <owner>: <item>", "", "## Open questions", "", "- <anything the evidence could not settle>", ""]
    return "\n".join(out)


def cmd_summary(args) -> None:
    builds = [fetch_build(n) for n in args.builds]
    text = markdown_summary(builds)
    if args.out:
        path = Path(args.out).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
    else:
        print(text)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--org", default=ORG, help=f"Buildkite organization slug (default {ORG})")
    parser.add_argument("--pipeline", default=PIPELINE, help=f"Buildkite pipeline slug (default {PIPELINE})")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("find", help="recent daily builds")
    p.add_argument("--days", type=float, default=3)
    p.add_argument("--branch", default=None, help="restrict to a branch (default: any)")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_find)

    p = sub.add_parser("status", help="per-stage verdict for one build")
    p.add_argument("build")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("log", help="cleaned job log")
    p.add_argument("build")
    p.add_argument("job", help="job id or step key (latest attempt)")
    p.add_argument("--attempt", type=int, default=None, help="pick attempt N of a step key (1 = first)")
    p.add_argument("--tail", type=int, default=80, help="lines to print (0 = all)")
    p.add_argument("--grep", default=None, help="print only lines matching this regex")
    p.set_defaults(func=cmd_log)

    p = sub.add_parser("artifacts", help="list or download job artifacts")
    p.add_argument("build")
    p.add_argument("job", nargs="?", default=None, help="job id or step key (default: every daily stage)")
    p.add_argument("--download", default=None, help="directory to download into")
    p.set_defaults(func=cmd_artifacts)

    p = sub.add_parser("classify", help="match failed jobs against known signatures")
    p.add_argument("build")
    p.add_argument("--tail", type=int, default=15)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_classify)

    p = sub.add_parser("release", help="publish stages of the main build for a commit or build")
    p.add_argument("ref", help="main build number or commit SHA")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_release)

    p = sub.add_parser("watch", help="poll until an event; exits 0 with WAKE: <reason>")
    p.add_argument("build")
    p.add_argument("--interval", type=int, default=120, help="seconds between polls")
    p.add_argument("--max-hours", type=float, default=9)
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("retry", help="retry one failed job (needs write_builds)")
    p.add_argument("build")
    p.add_argument("job", help="job id or step key (latest attempt)")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("summary", help="markdown summary for one or more builds")
    p.add_argument("builds", nargs="+")
    p.add_argument("--out", default=None, help="write here instead of stdout")
    p.set_defaults(func=cmd_summary)
    return parser


def main(argv: list[str] | None = None) -> None:
    global ORG, PIPELINE, TOKEN
    args = build_parser().parse_args(argv)
    ORG, PIPELINE = args.org, args.pipeline
    TOKEN = load_token()
    args.func(args)


if __name__ == "__main__":
    main()
