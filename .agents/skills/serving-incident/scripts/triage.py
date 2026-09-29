#!/usr/bin/env python3
"""Probe every serving hop of one OME-served model in parallel and print one
hop x time-bucket grid of error rates, plus the notes that name a cause.

Hops: API proxy (per-request log), its envoy sidecar, the hub TrafficMap, and
per member cluster the ingress gateway, the router and the engine pods.
Read-only: Scuba queries through the meta CLI, kubectl reads, Prometheus
reads through the API-server service proxy. A probe that fails or times out
prints as unreadable, never as healthy.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

PROXY_LOG = "llm_service_responses_api"
SIDECAR_LOG = "envoy_access_logs"


class Probe:
    def __init__(self, hop):
        self.hop = hop
        self.series = {}  # bucket start -> (errors, total or None)
        self.notes = []
        self.error = None
        self.hosts = []  # (ip, errors, total) of engines that fail most


def run(cmd, timeout):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"timed out after {timeout}s")
    if p.returncode != 0:
        msg = (p.stderr or p.stdout).strip().splitlines()
        raise RuntimeError(msg[-1][:200] if msg else f"exit {p.returncode}")
    return p.stdout


def scuba(a, dataset, sql):
    out = run(["meta", "scuba.dataset", "query", f"--dataset={dataset}", f"--sql={sql}", "-o", "json"], a.timeout)
    start = out.find("[")
    return json.loads(out[start:]) if start >= 0 else []


def kube(a, cluster, args):
    kc = os.path.join(a.kube_dir, f"{cluster}.conf")
    return run(["kubectl", "--kubeconfig", kc, f"--request-timeout={a.timeout}s", *args], a.timeout + 10)


def prom(a, cluster, expr, start, end):
    ns, svc = a.prom_service.split("/", 1)
    q = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": a.bucket * 60})
    raw = kube(a, cluster, ["get", "--raw", f"/api/v1/namespaces/{ns}/services/{svc}/proxy/api/v1/query_range?{q}"])
    return json.loads(raw)["data"]["result"]


def window(a):
    b = a.bucket * 60
    end = int(a.until or time.time())
    start = (end - a.since * 60) // b * b
    return start, end, b


def where(a, extra=""):
    start, end, _ = window(a)
    return f"time >= {start} AND time < {end} {extra}"


def probe_proxy(a):
    p = Probe("proxy")
    _, _, b = window(a)
    w = where(a, f"AND model = '{a.model}'")
    err = "exception_type IS NOT NULL AND exception_type != ''"
    for r in scuba(a, PROXY_LOG, f"SELECT (time - time % {b}) AS b, COUNT(*) AS total, SUM(IF({err}, 1, 0)) AS err FROM {PROXY_LOG} WHERE {w} GROUP BY b"):
        p.series[int(r["b"])] = (int(r["err"]), int(r["total"]))
    for r in scuba(a, PROXY_LOG, f"SELECT exception_type AS e, model_manifold_path AS ck, COUNT(*) AS n FROM {PROXY_LOG} WHERE {w} AND {err} GROUP BY e, ck ORDER BY n DESC LIMIT 4"):
        p.notes.append(f"{r['n']:>5}  {r['e']}  on {os.path.basename(r['ck'] or '?')}")
    for r in scuba(a, PROXY_LOG, f"SELECT client_id AS c, COUNT(*) AS n FROM {PROXY_LOG} WHERE {w} AND {err} GROUP BY c ORDER BY n DESC LIMIT 2"):
        p.notes.append(f"{r['n']:>5}  errors from caller {r['c']}")
    for r in scuba(a, PROXY_LOG, f"SELECT decode_host AS h, COUNT(*) AS total, SUM(IF({err}, 1, 0)) AS err FROM {PROXY_LOG} WHERE {w} GROUP BY h ORDER BY err DESC LIMIT 5"):
        e, t = int(r["err"]), int(r["total"])
        if e >= 5:
            ip = urllib.parse.urlparse(r["h"]).hostname or r["h"]
            p.hosts.append((ip, e, t))
    return p


def probe_sidecar(a):
    p = Probe("sidecar")
    if not a.proxy_tier:
        p.notes.append("skipped: pass --proxy-tier (the alert's ODS entity without the smc. prefix)")
        return p
    _, _, b = window(a)
    w = where(a, f"AND source_tw_job LIKE '%{a.proxy_tier}%'")
    bad = "(response_code >= 500 OR response_code = 0)"
    for r in scuba(a, SIDECAR_LOG, f"SELECT (time - time % {b}) AS b, COUNT(*) AS total, SUM(IF({bad}, 1, 0)) AS err FROM {SIDECAR_LOG} WHERE {w} GROUP BY b"):
        p.series[int(r["b"])] = (int(r["err"]), int(r["total"]))
    for r in scuba(a, SIDECAR_LOG, f"SELECT response_code AS c, response_flags AS f, COUNT(*) AS n FROM {SIDECAR_LOG} WHERE {w} AND {bad} GROUP BY c, f ORDER BY n DESC LIMIT 3"):
        p.notes.append(f"{r['n']:>5}  code {r['c'] or '-'} flags {r['f'] or '-'}")
    for r in scuba(a, SIDECAR_LOG, f"SELECT response_flags AS f, COUNT(*) AS n FROM {SIDECAR_LOG} WHERE {w} AND response_flags IS NOT NULL GROUP BY f ORDER BY n DESC LIMIT 3"):
        p.notes.append(f"{r['n']:>5}  flags {r['f']}")
    p.notes.append("sampled: counts show presence, not rates")
    return p


def probe_trafficmap(a):
    p = Probe("trafficmap")
    try:
        tm = json.loads(kube(a, a.hub, ["get", "trafficmap", a.isvc, "-n", a.namespace, "-o", "json"]))
    except RuntimeError as e:
        if "NotFound" in str(e) or "not found" in str(e):
            p.notes.append("no TrafficMap on the hub: single-cluster service, skip this hop")
            return p
        raise
    for e in tm.get("spec", {}).get("entries", []):
        cap = e.get("capacity", {})
        p.notes.append(f"{e.get('cluster')}: weight {e.get('weight')} healthy {e.get('healthy')} ready {cap.get('ready')}/{cap.get('allocated')}")
    for c in tm.get("status", {}).get("conditions", []):
        if c.get("status") != "True":
            p.notes.append(f"condition {c.get('type')}={c.get('status')}: {c.get('reason')}")
    return p


def prom_probe(a, hop, cluster, expr, is_err, note_retention=True):
    p = Probe(hop)
    start, end, b = window(a)
    first = max(start + b, end - a.prom_retention * 60 + b)
    for r in prom(a, cluster, expr, first, end):
        bad = is_err(r["metric"])
        for t, v in r["values"]:
            k = (int(float(t)) - b) // b * b
            e, tot = p.series.get(k, (0, 0))
            p.series[k] = (e + (float(v) if bad else 0), tot + float(v))
    if note_retention and first > start + b:
        p.notes.append(f"Prometheus keeps {a.prom_retention}m: buckets before {hhmm(first - b)} are unknown")
    return p


def probe_gateway(a, cluster):
    expr = f'sum by (envoy_response_code_class) (increase(envoy_cluster_upstream_rq_xx{{envoy_cluster_name="httproute/{a.namespace}/{a.isvc}/rule/0"}}[{a.bucket}m]))'
    return prom_probe(a, f"gateway {cluster}", cluster, expr, lambda m: m.get("envoy_response_code_class") == "5")


def probe_router(a, cluster):
    expr = f'sum by (status) (increase(router_requests_total{{namespace="{a.namespace}",inferenceservice="{a.isvc}"}}[{a.bucket}m]))'
    return prom_probe(a, f"router {cluster}", cluster, expr, lambda m: m.get("status", "").startswith("5"), note_retention=False)


def probe_engine(a, cluster):
    p = Probe(f"engine {cluster}")
    start, end, b = window(a)
    pods = json.loads(kube(a, cluster, ["get", "pods", "-n", a.namespace, "-l", f"ome.io/inferenceservice={a.isvc}", "-o", "json"]))["items"]
    notready, restarted = [], []
    for pod in pods:
        name = pod["metadata"]["name"]
        for cs in pod.get("status", {}).get("containerStatuses", []):
            if not cs.get("ready"):
                notready.append(name)
            term = cs.get("lastState", {}).get("terminated") or {}
            if term.get("finishedAt"):
                t = int(datetime.strptime(term["finishedAt"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
                if start <= t < end:
                    k = (t - start) // b * b + start
                    p.series[k] = (p.series.get(k, (0, None))[0] + 1, None)
                    restarted.append(f"{name}/{cs['name']} {term.get('reason')} exit {term.get('exitCode')}")
    notready = sorted(set(notready))
    p.notes.append(f"{len(pods)} pods, {len(notready)} not ready" + (f": {', '.join(notready[:3])}" + (" ..." if len(notready) > 3 else "") if notready else ""))
    p.notes += [f"restart in window: {r}" for r in restarted[:3]]
    return p


def locate(a, ip):
    for c in a.clusters:
        try:
            out = kube(a, c, ["get", "pods", "-A", "--field-selector", f"status.podIP={ip}", "-o", "jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{end}"])
        except RuntimeError:
            continue
        if out.strip():
            return f"{c} {out.strip()}"
    return "not found on the listed clusters (pod replaced, or another cluster)"


def safe(fn, *args):
    try:
        return fn(*args)
    except Exception as e:  # a failed probe is reported, not hidden
        p = Probe(fn.__name__.replace("probe_", "") + (f" {args[1]}" if len(args) > 1 else ""))
        p.error = str(e)
        return p


def hhmm(t):
    return datetime.fromtimestamp(t).strftime("%H:%M")


def cell(v):
    e, t = v
    if t is None:
        return f"{int(e)}r"
    if t == 0:
        return "0"
    return f"{100 * e / t:.1f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--isvc", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--clusters", required=True, help="comma-separated member clusters; kubeconfig <kube-dir>/<cluster>.conf")
    ap.add_argument("--hub", help="hub cluster holding TrafficMaps; omit for single-cluster services")
    ap.add_argument("--model", help="model column in the proxy log (default: --isvc)")
    ap.add_argument("--proxy-tier", help="proxy TW job name fragment, e.g. the alert's ODS entity without smc.")
    ap.add_argument("--since", type=int, default=90, help="minutes back")
    ap.add_argument("--until", type=int, help="window end, epoch seconds (default now)")
    ap.add_argument("--bucket", type=int, default=10, help="bucket minutes")
    ap.add_argument("--kube-dir", default=os.path.expanduser("~/.kube"))
    ap.add_argument("--prom-service", default="ome/ome-prometheus:9090")
    ap.add_argument("--prom-retention", type=int, default=60, help="minutes the in-cluster Prometheus keeps")
    ap.add_argument("--timeout", type=int, default=90, help="seconds per probe call")
    a = ap.parse_args()
    a.model = a.model or a.isvc
    a.clusters = [c for c in a.clusters.split(",") if c]

    jobs = [(probe_proxy, a), (probe_sidecar, a)] + ([(probe_trafficmap, a)] if a.hub else [])
    for c in a.clusters:
        jobs += [(probe_gateway, a, c), (probe_router, a, c), (probe_engine, a, c)]
    with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
        probes = list(ex.map(lambda j: safe(*j), jobs))
        hot = [(h, ex.submit(locate, a, h[0])) for p in probes for h in p.hosts]
        hot = [(h, f.result()) for h, f in hot]

    start, end, b = window(a)
    buckets = list(range(start, end, b))
    width = max(len(p.hop) for p in probes) + 2
    print(f"{a.isvc} in {a.namespace}: {hhmm(start)}-{hhmm(end)} local, {a.bucket}m buckets; cells are error % (Nr = restarts)")
    print("HOP".ljust(width) + "".join(hhmm(t).rjust(7) for t in buckets))
    for p in probes:
        if p.error or not p.series:
            continue
        print(p.hop.ljust(width) + "".join((cell(p.series[t]) if t in p.series else "·").rjust(7) for t in buckets))
    print()
    for p in probes:
        if p.error:
            print(f"{p.hop}: UNREADABLE ({p.error})")
        for n in p.notes:
            print(f"{p.hop}: {n}")
    for (ip, e, t), where_ in hot:
        print(f"hot engine {ip}: {e}/{t} errors -> {where_}")
    return 1 if any(p.error for p in probes) else 0


if __name__ == "__main__":
    sys.exit(main())
