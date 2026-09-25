#!/usr/bin/env python3
"""Per-queue pending analysis from the sweeps written by collect_sweeps.sh.

Standard library only; reads the data directory, prints a text report and
optionally writes summary.json. Queues default to every queue an ome job
referenced in the sweeps; pass --queue to narrow.

  queue_report.py --data-dir DIR [--queue Q ...] [--json PATH] [--top 8] [--long-factor 1.5]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

PIPELINE = "ome"
PENDING_STATES = {"scheduled", "assigned", "accepted"}
HELD_STATES = {"limited", "limiting"}
TERMINAL_JOB_STATES = {"passed", "failed", "timed_out", "canceled"}
PR_CHECK_KEYS = {
    "git-author-email", "internal-tokens", "lint", "vet", "test", "scheduler-test",
    "integration-test", "perf-test", "rollout-behavior-lock", "helm-lint", "muse-rust-checks",
}
FAMILIES = [
    "PR checks", "drift checks", "images", "charts", "KIND packaging", "KIND coordination",
    "KIND multi-cluster", "KIND quarantine", "KIND upgrade", "OMENative eval",
    "other ome step", "other pipelines",
]
CLASSES = ["ome main", "ome PR", "other pipelines"]
EMOJI = re.compile(r"^(:[a-z0-9_+-]+:\s*)+")


def parse_ts(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None


def load_json_dir(pattern):
    out = []
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.loads(fh.read(), strict=False)
        except Exception as err:  # a failed page is reported, not fatal
            sys.stderr.write(f"queue_report: skipping {path}: {err}\n")
            continue
        if isinstance(data, list):
            out.extend(data)
    return out


def dedupe(builds):
    seen, out = set(), []
    for b in builds:
        if b.get("id") not in seen:
            seen.add(b.get("id"))
            out.append(b)
    return out


def job_queue(job):
    for rule in job.get("agent_query_rules") or []:
        if str(rule).startswith("queue="):
            return str(rule).split("=", 1)[1]
    return None


def agent_queue(agent):
    for meta in agent.get("meta_data") or []:
        if str(meta).startswith("queue="):
            return str(meta).split("=", 1)[1]
    return None


def build_class(build):
    if (build.get("pipeline") or {}).get("slug") != PIPELINE:
        return "other pipelines"
    return "ome main" if build.get("branch") == "main" else "ome PR"


def family_of(job, build):
    if (build.get("pipeline") or {}).get("slug") != PIPELINE:
        return "other pipelines"
    key = str(job.get("step_key") or "").lower()
    if key.startswith("kind-gke-mc-"):
        return "KIND multi-cluster"
    if key == "kind-gke-quarantine":
        return "KIND quarantine"
    if key == "kind-gke-upgrade-identity":
        return "KIND upgrade"
    if key.startswith("kind-gke-"):
        return "KIND coordination"
    if key.startswith("omenative-e2e-"):
        return "OMENative eval"
    if key.startswith("package-kind-"):
        return "KIND packaging"
    if key.startswith(("build-", "mirror-")):
        return "images"
    if key.startswith("chart-"):
        return "charts"
    if key.endswith("-drift"):
        return "drift checks"
    if key in PR_CHECK_KEYS:
        return "PR checks"
    return "other ome step"


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))]


def minutes(a, b):
    return (b - a).total_seconds() / 60


class Sweep:
    def __init__(self, data_dir):
        with open(os.path.join(data_dir, "meta.json"), encoding="utf-8") as fh:
            self.meta = json.load(fh)
        self.now = parse_ts(self.meta["collected_at"])
        self.agents = load_json_dir(os.path.join(data_dir, "agents", "p*.json"))
        self.live = dedupe(load_json_dir(os.path.join(data_dir, "live", "p*.json")))
        org_hist = load_json_dir(os.path.join(data_dir, "hist", "org_*.json"))
        self.hist_scope = "org" if org_hist else PIPELINE
        self.hist = dedupe(org_hist or load_json_dir(os.path.join(data_dir, "hist", f"{PIPELINE}_*.json")))
        self.live_index = {((b.get("pipeline") or {}).get("slug"), b.get("number")): b for b in self.live}

    def queues(self):
        found = set()
        for b in self.live + self.hist:
            if (b.get("pipeline") or {}).get("slug") != PIPELINE:
                continue
            for j in b.get("jobs") or []:
                if j.get("type") == "script" and job_queue(j):
                    found.add(job_queue(j))
        return sorted(found)


def snapshot(sweep, queue, top):
    now = sweep.now
    agents = [a for a in sweep.agents if agent_queue(a) == queue]
    busy = [a for a in agents if a.get("job")]
    orphan = 0
    for a in busy:
        m = re.search(r"/pipelines/([^/]+)/builds/(\d+)", str((a.get("job") or {}).get("build_url") or ""))
        if not m or (m.group(1), int(m.group(2))) not in sweep.live_index:
            orphan += 1

    pend_class, pend_family, run_class, other_pipes = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    waits, held, owners, running_jobs = [], 0, [], []
    for b in sweep.live:
        cls = build_class(b)
        pend = run = 0
        for j in b.get("jobs") or []:
            if j.get("type") != "script" or job_queue(j) != queue:
                continue
            state = j.get("state")
            if state in PENDING_STATES:
                pend += 1
                pend_class[cls] += 1
                pend_family[family_of(j, b)] += 1
                if cls == "other pipelines":
                    other_pipes[(b.get("pipeline") or {}).get("slug")] += 1
                runnable = parse_ts(j.get("runnable_at") or j.get("scheduled_at") or j.get("created_at"))
                if runnable:
                    waits.append(minutes(runnable, now))
            elif state in HELD_STATES:
                held += 1
            elif state == "running":
                run += 1
                run_class[cls] += 1
                started = parse_ts(j.get("started_at"))
                if started:
                    running_jobs.append({
                        "key": j.get("step_key") or EMOJI.sub("", j.get("name") or "?"),
                        "family": family_of(j, b),
                        "pipeline": (b.get("pipeline") or {}).get("slug"),
                        "build": b.get("number"),
                        "minutes": round(minutes(started, now)),
                        "web_url": j.get("web_url"),
                    })
        if pend or run:
            owners.append({
                "pipeline": (b.get("pipeline") or {}).get("slug"),
                "number": b.get("number"),
                "branch": (b.get("branch") or "")[:40],
                "message": (b.get("message") or "").split("\n")[0][:60],
                "cls": cls,
                "pending": pend,
                "running": run,
                "web_url": b.get("web_url"),
            })
    owners.sort(key=lambda r: (-r["pending"], -r["running"]))
    return {
        "agents": len(agents),
        "busy": len(busy),
        "idle": len(agents) - len(busy),
        "orphan_holds": orphan,
        "pending": sum(pend_class.values()),
        "pending_by_class": {c: pend_class.get(c, 0) for c in CLASSES},
        "pending_by_family": {f: n for f, n in pend_family.items() if n},
        "pending_other_pipelines": dict(sorted(other_pipes.items(), key=lambda kv: -kv[1])[:5]),
        "held_by_concurrency": held,
        "running": sum(run_class.values()),
        "running_by_class": {c: run_class.get(c, 0) for c in CLASSES},
        "wait_p50": round(pct(waits, 0.5)),
        "wait_p90": round(pct(waits, 0.9)),
        "wait_max": round(max(waits)) if waits else 0,
        "demand_owners": owners[:top],
        "running_jobs": running_jobs,
    }


def history(sweep, queue):
    jobs = []
    for b in sweep.hist:
        cls = build_class(b)
        for j in b.get("jobs") or []:
            if j.get("type") != "script" or job_queue(j) != queue or not j.get("started_at"):
                continue
            start = parse_ts(j["started_at"])
            end = parse_ts(j.get("finished_at"))
            jobs.append((start, end, family_of(j, b), cls, j.get("state")))
    if not jobs:
        return None
    t1 = max([e for _, e, *_ in jobs if e] + [sweep.now])
    t0 = t1 - timedelta(hours=24)
    n_min = int((t1 - t0).total_seconds() // 60)
    delta = defaultdict(lambda: [0] * (n_min + 2))
    for start, end, fam, _, _ in jobs:
        a = max(0, int(minutes(t0, start)))
        z = min(n_min, int(minutes(t0, end or t1)) + 1)
        if a < z:
            delta[fam][a] += 1
            delta[fam][z] -= 1
    per_fam = {}
    for fam, d in delta.items():
        cur, arr = 0, []
        for m in range(n_min):
            cur += d[m]
            arr.append(cur)
        per_fam[fam] = arr
    total = [sum(arr[m] for arr in per_fam.values()) for m in range(n_min)]
    ceiling = pct(total, 0.995)
    mean = sum(total) / n_min if n_min else 0
    buckets = []
    for start in range(0, n_min, 10):
        mid = min(n_min - 1, start + 5)
        buckets.append({
            "at": (t0 + timedelta(minutes=mid)).strftime("%Y-%m-%dT%H:%M"),
            "total": total[mid],
            "by_family": {fam: arr[mid] for fam, arr in per_fam.items() if arr[mid]},
        })
    saturated = [b for b in buckets if ceiling and b["total"] >= 0.95 * ceiling]
    windows, cur = [], None
    for b in buckets:
        hot = ceiling and b["total"] >= 0.95 * ceiling
        if hot and cur is None:
            cur = [b["at"], b["at"]]
        elif hot:
            cur[1] = b["at"]
        elif cur is not None:
            windows.append(cur)
            cur = None
    if cur is not None:
        windows.append(cur)

    durs_by_fam, outcome_hours = defaultdict(list), defaultdict(float)
    for start, end, fam, _, state in jobs:
        if end and state in TERMINAL_JOB_STATES:
            d = minutes(start, end)
            if d > 0.2:
                durs_by_fam[fam].append(d)
            bucket = "failed / timed out" if state in ("failed", "timed_out") else ("canceled after start" if state == "canceled" else "passed")
            outcome_hours[bucket] += max(d, 0) / 60
    all_durs = [d for ds in durs_by_fam.values() for d in ds]
    return {
        "scope": sweep.hist_scope,
        "capped": bool(sweep.meta.get("org_history_capped")),
        "window": [t0.strftime("%Y-%m-%dT%H:%MZ"), t1.strftime("%Y-%m-%dT%H:%MZ")],
        "jobs_started": len(jobs),
        "builds": len(sweep.hist),
        "ceiling": ceiling,
        "mean_busy": round(mean, 1),
        "utilization": round(mean / ceiling, 2) if ceiling else 0,
        "saturated_buckets": len(saturated),
        "buckets_total": len(buckets),
        "saturation_windows": windows,
        "dur_p50": round(pct(all_durs, 0.5)),
        "dur_p90": round(pct(all_durs, 0.9)),
        "dur_by_family": {f: {"n": len(ds), "p50": round(pct(ds, 0.5)), "p90": round(pct(ds, 0.9)), "max": round(max(ds))} for f, ds in durs_by_fam.items()},
        "agent_hours_by_outcome": {k: round(v, 1) for k, v in outcome_hours.items()},
        "timeline": buckets,
    }


def long_running(snap, hist, factor):
    out = []
    for r in snap["running_jobs"]:
        p90 = (hist or {}).get("dur_by_family", {}).get(r["family"], {}).get("p90") if hist else None
        limit = p90 * factor if p90 else 90
        if r["minutes"] > limit:
            out.append({**r, "family_p90": p90, "limit": round(limit)})
    return sorted(out, key=lambda r: -r["minutes"])[:10]


def verdict(snap, hist):
    if snap["pending"] and snap["agents"] == 0:
        return f"stuck: 0 agents registered, {snap['pending']} jobs pending (p50 wait {snap['wait_p50']} min)"
    ceiling = (hist or {}).get("ceiling") or 0
    if snap["pending"] and ceiling and snap["busy"] >= 0.95 * ceiling:
        return f"saturated: {snap['pending']} pending + {snap['busy']} busy vs ceiling about {ceiling}, p50 wait {snap['wait_p50']} min"
    if snap["pending"] and snap["wait_p50"] >= 10:
        return f"backlog without saturation: {snap['pending']} pending, p50 wait {snap['wait_p50']} min, {snap['idle']} idle agents; check agent health"
    return f"clear: {snap['pending']} pending, p50 wait {snap['wait_p50']} min, {snap['busy']}/{snap['agents']} agents busy"


def fmt_counts(d):
    return " | ".join(f"{k} {v}" for k, v in d.items() if v) or "-"


def print_queue(queue, snap, hist, longs, when):
    print(f"== queue {queue}  (snapshot {when})")
    print(f"agents: {snap['agents']} total, {snap['busy']} busy, {snap['idle']} idle; orphan holds (busy, build not live): {snap['orphan_holds']}")
    print(f"pending: {snap['pending']} jobs  p50 wait {snap['wait_p50']} min  p90 {snap['wait_p90']}  max {snap['wait_max']}; held by concurrency groups: {snap['held_by_concurrency']}")
    print(f"  by class:  {fmt_counts(snap['pending_by_class'])}" + (f"  (other: {fmt_counts(snap['pending_other_pipelines'])})" if snap["pending_other_pipelines"] else ""))
    print(f"  by family: {fmt_counts(snap['pending_by_family'])}")
    print(f"running: {snap['running']} jobs  ({fmt_counts(snap['running_by_class'])})")
    if snap["demand_owners"]:
        print("demand owners:")
        for o in snap["demand_owners"]:
            print(f"  {o['pipeline']} #{o['number']} {o['branch']:<24} {o['pending']:>3} pending {o['running']:>3} running  {o['message']}  {o['web_url']}")
    if longs:
        print("long running (age beyond the family p90 x factor, or 90 min without history):")
        for r in longs:
            print(f"  {r['key']:<40} {r['pipeline']} #{r['build']}  {r['minutes']} min (limit {r['limit']}, family p90 {r['family_p90']})  {r['web_url']}")
    if hist:
        cap = " [page cap hit: ceiling is a lower bound]" if hist["capped"] else ""
        print(f"history ({hist['scope']} builds, {hist['window'][0]} to {hist['window'][1]}): {hist['jobs_started']} jobs started in {hist['builds']} builds; "
              f"ceiling about {hist['ceiling']} busy, mean {hist['mean_busy']} ({int(hist['utilization'] * 100)}%), "
              f"saturated {hist['saturated_buckets']}/{hist['buckets_total']} buckets{cap}")
        if hist["saturation_windows"]:
            print("  saturation windows: " + ", ".join(f"{a[11:]}-{b[11:]}" for a, b in hist["saturation_windows"]))
        print("  duration p50/p90/max min by family: " + ", ".join(f"{f} {d['p50']}/{d['p90']}/{d['max']} (n={d['n']})" for f, d in sorted(hist["dur_by_family"].items())))
        print(f"  agent-hours by outcome: {fmt_counts(hist['agent_hours_by_outcome'])}")
    else:
        print("history: no jobs for this queue in the sweep")
    print(f"verdict hint: {verdict(snap, hist)}")
    print()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--queue", action="append", help="queue name; repeatable (default: every queue ome jobs reference)")
    parser.add_argument("--json", metavar="PATH", help="write summary.json here")
    parser.add_argument("--top", type=int, default=8)
    parser.add_argument("--long-factor", type=float, default=1.5)
    args = parser.parse_args()

    sweep = Sweep(args.data_dir)
    queues = args.queue or sweep.queues()
    if not queues:
        sys.exit("queue_report: no queues found in the sweep; pass --queue")
    when = sweep.now.strftime("%Y-%m-%dT%H:%MZ")
    summary = {"collected_at": when, "queues": {}}
    for queue in queues:
        snap = snapshot(sweep, queue, args.top)
        hist = history(sweep, queue)
        longs = long_running(snap, hist, args.long_factor)
        print_queue(queue, snap, hist, longs, when)
        conclusion = verdict(snap, hist)
        snap = {k: v for k, v in snap.items() if k != "running_jobs"}
        summary["queues"][queue] = {"snapshot": snap, "long_running": longs, "history": hist, "verdict": conclusion}
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1, default=str)
        print(f"summary: {args.json}")


if __name__ == "__main__":
    main()
