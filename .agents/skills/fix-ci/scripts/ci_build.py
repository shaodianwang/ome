#!/usr/bin/env python3
"""Inspect, triage and retry standard mslsrc/ome CI builds on Buildkite.

Standard library only. The API token comes from $BUILDKITE_API_TOKEN or
~/.buildkite/token (mode 600); the tool refuses to run without one. It keeps no
state: cleaned logs and downloads go under
~/.cache/ome-agent.noindex/scratch/fix-ci/ (override: OME_FIXCI_SCRATCH).

  find       builds for a PR head commit, a commit, or a branch (newest first)
  status     every job of one build: step key, state, exit, attempts, class
  classify   failed jobs' logs matched against the known failure signatures
  log        one job's log with ANSI and timestamp escapes stripped
  artifacts  list or download a job's artifacts
  verdicts   the eval lanes' per-case verdicts of a build from their verdict files
  retry      retry one failed job (newest attempt only; needs write_builds)
  wait       poll until the build is terminal; prints DONE: <state>

A job is named by its id (any unique prefix) or by its step key; a step key
resolves to the newest attempt unless --attempt is given.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ORG = "msl"
PIPELINE = "ome"
GITHUB_REPO = "mslsrc/ome"
TERMINAL_BUILD_STATES = {"passed", "failed", "canceled", "blocked", "skipped", "not_run"}
BAD_JOB_STATES = {"failed", "timed_out", "canceled"}
DEPENDENCY_STATES = {"waiting_failed", "blocked_failed"}


def die(message: str, code: int = 1) -> None:
    print(f"error: {message}", file=sys.stderr)
    sys.exit(code)


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
            f"{path} (chmod 600) holding a user API token with read_builds, "
            "read_build_logs, read_artifacts (and write_builds for `retry`)"
        )
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        warn(f"{path} is mode {mode:o}; run: chmod 600 {path}")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        die(f"{path} is empty")
    return token


_TOKEN: str | None = None


def token() -> str:
    """Loaded on first use so --help and argument errors never need a token."""
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = load_token()
    return _TOKEN


def api_root() -> str:
    return f"https://api.buildkite.com/v2/organizations/{ORG}/pipelines/{PIPELINE}"


def build_web_url(number) -> str:
    return f"https://buildkite.com/{ORG}/{PIPELINE}/builds/{number}"


def scratch_dir() -> Path:
    root = os.environ.get("OME_FIXCI_SCRATCH") or str(Path.home() / ".cache" / "ome-agent.noindex" / "scratch" / "fix-ci")
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802
        return None


def http(method: str, url: str, accept: str = "application/json", body=None, follow_redirects: bool = True):
    headers = {"Authorization": f"Bearer {token()}", "Accept": accept, "User-Agent": "ome-fix-ci/1"}
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


_BAD_ESCAPE = re.compile(r'\\(?!["\\/bfnrtu])')


def parse_json(text: str):
    """Buildkite job payloads may carry raw control characters; never parse in strict mode."""
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        return json.loads(_BAD_ESCAPE.sub(r"\\\\", text), strict=False)


def get_json(url: str):
    status, raw, _ = http("GET", url)
    if status == 401:
        die("Buildkite API rejected the token (401): create a new user API token with the scopes above and store it in ~/.buildkite/token")
    if status >= 400:
        die(f"GET {url} -> HTTP {status}: {raw[:300].decode('utf-8', 'replace')}")
    return parse_json(raw.decode("utf-8", "replace"))


# --- builds and jobs ---------------------------------------------------------

def fetch_build(number) -> dict:
    return get_json(f"{api_root()}/builds/{number}?include_retried_jobs=true")


def builds_for(commit: str | None = None, branch: str | None = None, limit: int = 10) -> list[dict]:
    params = [f"per_page={limit}", "include_retried_jobs=true"]
    if commit:
        params.append(f"commit={commit}")
    if branch:
        params.append(f"branch={branch}")
    return get_json(f"{api_root()}/builds?" + "&".join(params))


def script_jobs(build: dict) -> list[dict]:
    return [j for j in build.get("jobs", []) if j.get("type") == "script"]


def job_name(job: dict) -> str:
    return job.get("step_key") or job.get("name") or job.get("id", "")[:8]


def latest_attempts(build: dict) -> list[dict]:
    return [j for j in script_jobs(build) if not j.get("retried")]


def attempts_for(build: dict, key: str) -> list[dict]:
    same = [j for j in script_jobs(build) if j.get("step_key") == key]
    return sorted(same, key=lambda j: j.get("created_at") or "")


def resolve_job(build: dict, ref: str, attempt: int | None = None) -> dict:
    by_key = attempts_for(build, ref)
    if by_key:
        if attempt is not None:
            if not 1 <= attempt <= len(by_key):
                die(f"{ref} has {len(by_key)} attempt(s); --attempt {attempt} is out of range")
            return by_key[attempt - 1]
        newest = [j for j in by_key if not j.get("retried")]
        return (newest or by_key)[-1]
    by_id = [j for j in script_jobs(build) if j.get("id", "").startswith(ref)]
    if len(by_id) == 1:
        return by_id[0]
    if len(by_id) > 1:
        die(f"job id prefix {ref!r} is ambiguous")
    die(f"no job with step key or id {ref!r} in build {build.get('number')}")


def quick_class(job: dict) -> str:
    """State-only guess; `classify` refines it from the log."""
    state = job.get("state")
    exit_status = job.get("exit_status")
    if state == "passed" or (state == "finished" and exit_status == 0):
        return "ok"
    if state in DEPENDENCY_STATES:
        return "dependency-failed"
    if job.get("soft_failed"):
        return "advisory (soft-fail)"
    if exit_status == 125:
        return "infra: docker daemon (exit 125)"
    if job.get("signal_reason") == "agent_stop" or exit_status == -1:
        return "infra: agent lost"
    if state == "timed_out":
        return "timeout"
    if state in BAD_JOB_STATES:
        return "code (read the log)"
    return state or "unknown"


def is_red(job: dict) -> bool:
    return job.get("state") in BAD_JOB_STATES


# --- logs ----------------------------------------------------------------------

_ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_OSC = re.compile(r"\x1b\][^\x07]*\x07")
_BK_TIMESTAMP = re.compile(r"\x1b_bk;t=\d+\x07|_bk;t=\d+")
_OTHER_ESCAPES = re.compile(r"\x1b[^\[\]_]")


def clean_log(text: str) -> str:
    text = _BK_TIMESTAMP.sub("", text)
    text = _OSC.sub("", text)
    text = _ANSI.sub("", text)
    text = _OTHER_ESCAPES.sub("", text)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def fetch_log(number, job: dict) -> str:
    url = job.get("raw_log_url") or f"{api_root()}/builds/{number}/jobs/{job['id']}/log.txt"
    status, raw, _ = http("GET", url, accept="text/plain")
    if status >= 400:
        die(f"log {job['id']} -> HTTP {status}")
    return clean_log(raw.decode("utf-8", "replace"))


def save_log(number, job: dict, text: str) -> Path:
    # Job labels carry "/", ":" and spaces; keep the file name one flat component.
    label = re.sub(r"[^A-Za-z0-9._-]+", "_", job_name(job)).strip("_")
    path = scratch_dir() / f"b{number}-{label}-{job['id'][:8]}.log"
    path.write_text(text, encoding="utf-8")
    return path


# Signature table: (regex, step-key suffix filter or None, class, action). First match wins.
SIGNATURES = [
    (r"failed to connect to the docker API|Cannot connect to the Docker daemon", None,
     "infra/docker-daemon", "retry once; the step's automatic retry usually covers this"),
    (r"FATAL: no reachable Docker daemon", None,
     "infra/dind-sidecar", "retry once; if it repeats, the GKE queue's pod spec lost its privileged dind container (.buildkite/README-gke-kind.md)"),
    (r"cluster create failed in BeforeSuite with no spec run", None,
     "infra/kind-node-boot", "exit 125, auto-retried; retry once more only if the limit was spent"),
    (r"no ECR credential source produced a token|authorization token has expired|no basic auth credentials", None,
     "infra/registry-auth", "retry once; a second hit means the cluster secret is broken: escalate CI infrastructure"),
    (r"proxy\.golang\.org.*(timeout|EOF|50[23])|TLS handshake timeout|dial tcp .*i/o timeout", None,
     "infra/network", "retry once; drift, build and mirror steps already retry on any exit"),
    (r"unclassified KIND specs: (.+)", None,
     "generator/unclassified-spec", "add the stem to KIND_SPEC_GROUPS in .buildkite/generate_pipeline.py; run python3 -m unittest discover -s .buildkite -p 'test_*.py'"),
    (r"KIND quarantine for (\S+) expired", None,
     "generator/quarantine-expired", "fix the spec and return it to its group, or extend the entry with a reason"),
    (r"mapped KIND specs do not exist|belongs to both|invalid or duplicate KIND spec group|quarantined KIND spec does not exist", None,
     "generator/spec-groups", "fix KIND_SPEC_GROUPS or KIND_QUARANTINE; run the generator unit tests"),
    (r"Git author email '.*' is not a GitHub noreply address", None,
     "hygiene/author-email", "set <id>+<login>@users.noreply.github.com as the author and rewrite the rejected commits"),
    (r"\d+ finding\(s\) in \d+ scanned file\(s\)", None,
     "hygiene/internal-token", "replace the identifier with a neutral one; the finding line names the group from hack/internal-tokens.txt"),
    (r"^diff --git ", "-drift",
     "drift", "run the step's make target locally (fmt, tidy, manifests, generate, dep-crds, oep-status) and commit the result"),
    (r"WARNING: DATA RACE", None,
     "code/data-race", "scheduler-test runs under -race; fix the race, never retry"),
    (r"panic: test timed out after|Test killed with quit: ran too long", None,
     "timeout/go-test", "a hung test past -test.timeout; read the goroutine dump for the blocked Eventually or channel"),
    (r"^\s*--- FAIL: (\S+)", None,
     "code/go-test", "go test ./<pkg>/... -run <Test> -v with KUBEBUILDER_ASSETS set; see references/local-reproduction.md"),
    (r"\[FAIL(ED)?\]|Summarizing \d+ Failures?", None,
     "code/ginkgo-spec", "reproduce the named spec (envtest: go test ./tests/integration/...; KIND: GINKGO_FOCUS='<It text>' make test-kind under a throwaway KUBECONFIG); never retry a red spec"),
    (r"NON-PASS:", None,
     "code/eval-case", "verdicts <build> --job <key> lists the cases from the lane's verdict file; the FAILED CASE LOGS section and the results archive artifact hold the per-case logs"),
    (r"average coverage|coverage threshold|below the .*threshold", "test",
     "code/coverage", "the merged average must stay at or above 48%; add tests (make coverage prints the number)"),
    (r"\S+\.go:\d+:\d+: .+ \([a-z0-9]+\)$", "lint",
     "code/lint", "make ci-lint; make lint-fix for auto-fixable findings"),
    (r"\[ERROR\]|Error: \d+ chart\(s\) linted, \d+ chart\(s\) failed", "helm-lint",
     "code/helm", "make helm-lint; render the chart with helm template to see the failing template"),
    (r"^error(\[E\d+\])?: |could not compile|clippy::", "muse-rust-checks",
     "code/rust", "make -C muse ci-rust"),
    (r"failed to solve:|ERROR: failed to build|Error response from daemon", None,
     "code/dockerfile", "build-* validates the Dockerfile on PR branches; read the failing RUN step"),
]


def classify_log(job: dict, text: str) -> tuple[str, str, str]:
    key = job.get("step_key") or ""
    lines = text.splitlines()
    for pattern, suffix, klass, action in SIGNATURES:
        if suffix and not key.endswith(suffix) and key != suffix:
            continue
        regex = re.compile(pattern)
        for line in lines:
            if regex.search(line):
                return klass, action, line.strip()[:200]
    guess = quick_class(job)
    if guess.startswith("infra"):
        return guess, "retry once", ""
    tail = next((l for l in reversed(lines) if l.strip()), "")
    return "needs-evidence", "no known signature; read the log from the last '--- stage ---' header", tail.strip()[:200]


# --- commands --------------------------------------------------------------------

def cmd_find(args) -> None:
    commit, branch = args.commit, args.branch
    if args.pr:
        try:
            out = subprocess.run(
                ["gh", "pr", "view", str(args.pr), "--repo", GITHUB_REPO, "--json", "headRefOid,headRefName,url,isDraft"],
                check=True, capture_output=True, text=True,
            ).stdout
        except FileNotFoundError:
            die("gh is not installed; pass --commit <head sha> instead")
        except subprocess.CalledProcessError as err:
            die(f"gh pr view failed: {err.stderr.strip()}")
        pr = json.loads(out)
        commit = pr["headRefOid"]
        print(f"# PR {args.pr} {pr['url']} head={commit[:12]} branch={pr['headRefName']} draft={pr['isDraft']}")
    if not commit and not branch:
        die("pass --pr, --commit or --branch")
    builds = builds_for(commit=commit, branch=branch, limit=args.limit)
    if not builds:
        print("no builds found")
        return
    for b in builds:
        reds = [job_name(j) for j in latest_attempts(b) if is_red(j)]
        print(
            f"build {b.get('number')} state={b.get('state')} branch={b.get('branch')} "
            f"commit={str(b.get('commit'))[:12]} created={b.get('created_at')} {b.get('web_url') or build_web_url(b.get('number'))}"
        )
        if reds:
            print(f"  red: {', '.join(reds)}")


def cmd_status(args) -> None:
    build = fetch_build(args.build)
    jobs = latest_attempts(build)
    print(f"build {build.get('number')} state={build.get('state')} branch={build.get('branch')} commit={str(build.get('commit'))[:12]}")
    print(f"  {build.get('web_url') or build_web_url(build.get('number'))}")
    counts: dict[str, int] = {}
    for j in jobs:
        counts[j.get("state") or "?"] = counts.get(j.get("state") or "?", 0) + 1
    print("  jobs: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    ordered = sorted(jobs, key=lambda j: (0 if is_red(j) else 1 if j.get("state") in DEPENDENCY_STATES else 2, job_name(j)))
    for j in ordered:
        if not args.all and not is_red(j) and j.get("state") not in DEPENDENCY_STATES:
            continue
        attempts = attempts_for(build, j.get("step_key") or "") if j.get("step_key") else [j]
        soft = " soft_fail" if j.get("soft_failed") else ""
        print(
            f"- {job_name(j)}: state={j.get('state')} exit={j.get('exit_status')}{soft} "
            f"attempts={len(attempts)} class={quick_class(j)}"
        )
        print(f"    id={j.get('id')} {j.get('web_url')}")
    if not any(is_red(j) for j in jobs):
        print("no failed jobs among the newest attempts")


def cmd_classify(args) -> None:
    build = fetch_build(args.build)
    reds = [j for j in latest_attempts(build) if is_red(j)]
    if not reds:
        print("no failed jobs among the newest attempts")
        return
    for j in reds:
        text = fetch_log(args.build, j)
        saved = save_log(args.build, j, text)
        klass, action, evidence = classify_log(j, text)
        soft = " (soft-fail)" if j.get("soft_failed") else ""
        print(f"- {job_name(j)}{soft}: {klass}")
        print(f"    action: {action}")
        if evidence:
            print(f"    evidence: {evidence}")
        print(f"    log: {saved}")


def cmd_log(args) -> None:
    build = fetch_build(args.build)
    job = resolve_job(build, args.job, args.attempt)
    text = fetch_log(args.build, job)
    saved = save_log(args.build, job, text)
    lines = text.splitlines()
    print(f"# job {job['id']} step={job.get('step_key')} state={job.get('state')} exit={job.get('exit_status')} retried={job.get('retried')}")
    print(f"# {job.get('web_url')}")
    print(f"# {len(lines)} lines saved to {saved}")
    if args.grep:
        pattern = re.compile(args.grep, re.IGNORECASE)
        hits = [i for i, line in enumerate(lines) if pattern.search(line)]
        if args.context:
            shown: set[int] = set()
            for i in hits:
                for k in range(max(0, i - args.context), min(len(lines), i + args.context + 1)):
                    shown.add(k)
            last = None
            for k in sorted(shown):
                if last is not None and k != last + 1:
                    print("--")
                print(f"{k + 1}: {lines[k]}")
                last = k
        else:
            for i in hits:
                print(f"{i + 1}: {lines[i]}")
        return
    for line in lines[-args.tail:] if args.tail else lines:
        print(line)


def download_artifact(artifact: dict) -> bytes:
    """The artifact's bytes: the API answers with a redirect to storage,
    which is fetched without the bearer header."""
    st, body, headers = http("GET", artifact["download_url"], accept="*/*", follow_redirects=False)
    if st in (301, 302, 303, 307, 308):
        location = headers.get("Location") or headers.get("location")
        request = urllib.request.Request(location, headers={"User-Agent": "ome-fix-ci/1"})
        with urllib.request.urlopen(request, timeout=300) as response:
            return response.read()
    if st >= 400:
        die(f"download {artifact.get('path')} -> HTTP {st}")
    return body


def cmd_artifacts(args) -> None:
    build = fetch_build(args.build)
    job = resolve_job(build, args.job)
    status, raw, _ = http("GET", f"{api_root()}/builds/{args.build}/jobs/{job['id']}/artifacts")
    if status >= 400:
        die(f"artifacts -> HTTP {status}")
    artifacts = parse_json(raw.decode("utf-8", "replace"))
    if not artifacts:
        print("no artifacts")
        return
    for a in artifacts:
        print(f"{a.get('path')}  {a.get('file_size')} bytes  state={a.get('state')}")
        if args.download:
            dest = Path(args.download) / a["path"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(download_artifact(a))
            print(f"  -> {dest}")


# --- eval verdict files ----------------------------------------------------------

# Every eval lane job uploads, beside its result archive, the harness's own
# record of the job: a JSON object with the lane ("step", "lane",
# "scheduler", "run", "seconds", "counts", ...) and "cases", one object per
# case with "case", "feature", "topology", "verdict", "reason", "exit",
# "seconds", "fault", "log" (the member <run>/<log> of the archive) and
# "result" (null when the harness never finished the case).
# .buildkite/eval_verdicts.py defines the format; this is a reader of it.
VERDICTS_SUFFIX = ".verdicts.json"
VERDICTS_FORMAT = "ome-eval-job-verdicts/1"
EVAL_STEP_PREFIX = "omenative-e2e-"
# Verdict words that need nobody's attention: a pass, parked debt, a case
# whose fault apparatus the cluster lacks.
GREEN_WORDS = {"PASS", "XFAIL", "SKIP"}


def build_artifacts(number) -> list[dict]:
    """Every artifact of the build, across pages."""
    out: list[dict] = []
    page = 1
    while True:
        batch = get_json(f"{api_root()}/builds/{number}/artifacts?per_page=100&page={page}")
        if not isinstance(batch, list) or not batch:
            return out
        out.extend(batch)
        if len(batch) < 100:
            return out
        page += 1


def verdict_word(verdict) -> str:
    """The verdict's first word, which every XFAIL form shares."""
    words = str(verdict or "").split()
    return words[0] if words else "?"


def parse_verdicts(text: str) -> dict:
    """One lane's verdict document; ValueError when the text is not one."""
    try:
        document = json.loads(text)
    except ValueError as err:
        raise ValueError(f"not JSON: {err}") from None
    if not isinstance(document, dict) or document.get("format") != VERDICTS_FORMAT:
        raise ValueError(f"not a {VERDICTS_FORMAT} document")
    if not isinstance(document.get("cases"), list):
        raise ValueError("cases is not a list")
    return document


def tally_verdicts(documents: list[dict]) -> dict:
    """Cases per verdict word over every document, and per lane, in the
    documents' order, its counts and the cases that are not green."""
    counts: dict[str, int] = {}
    lanes = []
    for document in documents:
        lane_counts: dict[str, int] = {}
        red = []
        for row in document["cases"]:
            row = row if isinstance(row, dict) else {"case": "?", "verdict": "?"}
            word = verdict_word(row.get("verdict"))
            counts[word] = counts.get(word, 0) + 1
            lane_counts[word] = lane_counts.get(word, 0) + 1
            if word not in GREEN_WORDS:
                red.append(row)
        lanes.append({
            "step": document.get("step"),
            "lane": document.get("lane"),
            "scheduler": document.get("scheduler"),
            "run": document.get("run"),
            "seconds": document.get("seconds"),
            "cases": len(document["cases"]),
            "counts": dict(sorted(lane_counts.items())),
            "red": red,
        })
    return {"cases": sum(counts.values()), "counts": dict(sorted(counts.items())), "lanes": lanes}


def _counts_text(counts: dict[str, int]) -> str:
    return ", ".join(f"{word} {n}" for word, n in counts.items()) or "no cases"


def format_tally(build, tally: dict, missing: list[dict], show_all: bool = False) -> list[str]:
    """The tally as lines: the build's totals; every lane with a case that is
    not green (every lane with show_all) and those cases; then the eval jobs
    that uploaded no verdict file."""
    lines = [
        f"build {build}: {tally['cases']} cases in {len(tally['lanes'])} lanes with a verdict file: "
        f"{_counts_text(tally['counts'])}"
    ]
    for lane in tally["lanes"]:
        if not lane["red"] and not show_all:
            continue
        name = lane["step"] or lane["lane"] or "?"
        lines.append(f"- {name}: {lane['cases']} cases, {_counts_text(lane['counts'])}  (case logs under {lane['run']}/ in the results archive)")
        for row in lane["red"]:
            seconds = row.get("seconds")
            fault = f"  fault={row['fault']}" if row.get("fault") else ""
            lines.append(
                f"    {str(row.get('case') or '?'):<6} {str(row.get('verdict') or '?'):<14} "
                f"{(f'{seconds:.0f}s' if isinstance(seconds, (int, float)) else '-'):>6}{fault}  {row.get('reason') or ''}".rstrip()
            )
            lines.append(f"           log {row.get('log') or '?'}")
    for job in missing:
        lines.append(
            f"- {job_name(job)}: no verdict file (state={job.get('state')} exit={job.get('exit_status')}): "
            "the lane stopped before its summary; read its log"
        )
    return lines


def cmd_verdicts(args) -> None:
    build = fetch_build(args.build)
    jobs = {j["id"]: j for j in latest_attempts(build)}
    if args.job:
        job = resolve_job(build, args.job)
        jobs = {job["id"]: job}
    artifacts = [
        a for a in build_artifacts(args.build)
        if str(a.get("filename") or "").endswith(VERDICTS_SUFFIX) and a.get("job_id") in jobs
    ]
    dest_dir = Path(args.download) if args.download else scratch_dir() / f"b{args.build}-verdicts"
    dest_dir.mkdir(parents=True, exist_ok=True)
    documents = []
    with_file = set()
    for a in sorted(artifacts, key=lambda a: a.get("path") or ""):
        body = download_artifact(a)
        (dest_dir / a["filename"]).write_bytes(body)
        try:
            document = parse_verdicts(body.decode("utf-8", "replace"))
        except ValueError as err:
            warn(f"{a.get('path')}: {err}")
            continue
        job = jobs[a["job_id"]]
        if not document.get("step"):
            document["step"] = job.get("step_key")
        document["job_web_url"] = job.get("web_url")
        documents.append(document)
        with_file.add(a["job_id"])
    # The "-skipped" steps only say which cases a scheduler twin left out.
    missing = sorted(
        (
            j for jid, j in jobs.items()
            if jid not in with_file
            and (j.get("step_key") or "").startswith(EVAL_STEP_PREFIX)
            and not (j.get("step_key") or "").endswith("-skipped")
        ),
        key=job_name,
    )
    tally = tally_verdicts(documents)
    if args.json:
        print(json.dumps({
            "build": build.get("number"),
            **tally,
            "without_file": [
                {"key": job_name(j), "id": j.get("id"), "state": j.get("state"), "exit_status": j.get("exit_status"), "web_url": j.get("web_url")}
                for j in missing
            ],
            "dir": str(dest_dir),
        }, indent=1))
        return
    for line in format_tally(build.get("number"), tally, missing, args.all):
        print(line)
    print(f"# {len(documents)} verdict file(s) saved under {dest_dir}")


def cmd_retry(args) -> None:
    build = fetch_build(args.build)
    job = resolve_job(build, args.job)
    if job.get("retried"):
        die(f"job {job['id']} was already retried into {job.get('retried_in_job_id')}; retry the newest attempt instead")
    if not is_red(job) and not args.force:
        die(f"job {job['id']} is {job.get('state')}, not failed; pass --force if you really mean it")
    status, raw, _ = http("PUT", f"{api_root()}/builds/{args.build}/jobs/{job['id']}/retry")
    if status >= 400:
        die(f"retry -> HTTP {status}: {raw[:300].decode('utf-8', 'replace')} (write_builds scope required)")
    new_job = parse_json(raw.decode("utf-8", "replace"))
    print(f"retried {job_name(job)} {job['id']} -> new job {new_job.get('id')} state={new_job.get('state')}")
    print(f"  {new_job.get('web_url')}")
    print("  a retry never fixes a code failure; if this is the second retry of the same signature, stop and classify")


def cmd_wait(args) -> None:
    deadline = time.time() + args.max_minutes * 60
    last_state = None
    while True:
        build = fetch_build(args.build)
        state = build.get("state")
        jobs = latest_attempts(build)
        reds = [job_name(j) for j in jobs if is_red(j)]
        running = sum(1 for j in jobs if j.get("state") in ("running", "scheduled", "assigned", "accepted"))
        if state != last_state:
            print(f"{time.strftime('%H:%M:%S')} build {args.build} state={state} running={running} red={', '.join(reds) or '-'}", flush=True)
            last_state = state
        if state in TERMINAL_BUILD_STATES:
            print(f"DONE: state={state} red={', '.join(reds) or '-'} {build.get('web_url') or build_web_url(args.build)}")
            return
        if time.time() > deadline:
            print(f"TIMEOUT: build {args.build} still {state} after {args.max_minutes} minutes")
            sys.exit(2)
        time.sleep(args.interval)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("find", help="builds for a PR, commit or branch")
    p.add_argument("--pr", type=int)
    p.add_argument("--commit")
    p.add_argument("--branch")
    p.add_argument("--limit", type=int, default=10)
    p.set_defaults(func=cmd_find)

    p = sub.add_parser("status", help="per-job state for one build")
    p.add_argument("build")
    p.add_argument("--all", action="store_true", help="list passing jobs too")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("classify", help="match failed jobs' logs against known signatures")
    p.add_argument("build")
    p.set_defaults(func=cmd_classify)

    p = sub.add_parser("log", help="cleaned job log")
    p.add_argument("build")
    p.add_argument("job", help="step key or job id prefix")
    p.add_argument("--attempt", type=int, help="1-based attempt for a step key (default newest)")
    p.add_argument("--grep", help="case-insensitive regex; print matching lines")
    p.add_argument("--context", type=int, default=0, help="lines of context around each --grep match")
    p.add_argument("--tail", type=int, default=80, help="lines to print without --grep (0 = all)")
    p.set_defaults(func=cmd_log)

    p = sub.add_parser("artifacts", help="list or download a job's artifacts")
    p.add_argument("build")
    p.add_argument("job")
    p.add_argument("--download", metavar="DIR")
    p.set_defaults(func=cmd_artifacts)

    p = sub.add_parser("verdicts", help="per-case verdicts of a build's eval lanes from their verdict files")
    p.add_argument("build")
    p.add_argument("--job", help="one lane: step key or job id prefix (default: every newest eval job)")
    p.add_argument("--download", metavar="DIR", help="where to save the files (default: scratch)")
    p.add_argument("--all", action="store_true", help="list every lane, not only those with a case that is not green")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_verdicts)

    p = sub.add_parser("retry", help="retry one failed job (needs write_builds)")
    p.add_argument("build")
    p.add_argument("job")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("wait", help="poll until the build is terminal")
    p.add_argument("build")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--max-minutes", type=int, default=90)
    p.set_defaults(func=cmd_wait)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
