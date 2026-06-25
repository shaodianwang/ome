#!/usr/bin/env python3
"""hung_prom.py [N] [--timeout S] — hung-Prometheus isolation test.

The headline guarantee of the async sampler: a slow/hung Prometheus must never
stall the single reconcile worker. This deploys a delay-proxy in front of the
bundled prometheus (every query waits 30s >> the 5s QueryTimeout), N analysis
canaries whose source points at it (so each churns inconclusive forever,
generating sustained background query pressure), and one CONTROL canary on a
normal source. It bumps them all together and asserts the control still reaches
Stable within a normal budget — i.e. the hung source didn't block the worker.

Real cluster; env like run.py. Needs the runtime fixtures + bundled
ome-prometheus, and assumes OME in ns "ome" (see perf/prom-delay-proxy.yaml).

  python3 hung_prom.py 5
  python3 hung_prom.py 10 --timeout 400
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(os.path.dirname(HERE), "runtime")
PROXY = os.path.join(HERE, "prom-delay-proxy.yaml")
KUBECONFIG = os.environ.get("OME_TEST_KUBECONFIG") or os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
NS = os.environ.get("OME_TEST_NS", "sandbox")
ENV = {**os.environ, "KUBECONFIG": KUBECONFIG}

# Control: a fast plain auto-canary (no analysis) — its time-to-Stable is the
# no-stall signal. Real cluster, nginx CSR.
CONTROL = """apiVersion: ome.io/v1beta1
kind: InferenceService
metadata: { name: __NAME__ }
spec:
  model:   { apiGroup: ome.io, kind: ClusterBaseModel,      name: ome-coordtest-noop }
  runtime: { apiGroup: ome.io, kind: ClusterServingRuntime, name: ome-coordtest-cpu, autoSync: true }
  engine:
    minReplicas: 2
    maxReplicas: 2
    annotations: { ome.io/deploymentMode: OMENative }
    lifecycle: { updateStrategy: { type: "SurgeThenDrain", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } } }
  rollout:
    groups:
        - components: [engine]
          canary:
            steps:
              - { capacity: "50%", traffic: 50, pause: { duration: "5s" } }
              - { capacity: "100%", traffic: 100 }
"""

# Hung: analysis source = the delay-proxy. failureLimit huge + onInconclusive Hold
# so it never resolves — it just keeps issuing (timing-out) background queries.
HUNG = """apiVersion: ome.io/v1beta1
kind: InferenceService
metadata: { name: __NAME__ }
spec:
  model:   { apiGroup: ome.io, kind: ClusterBaseModel,      name: ome-coordtest-noop }
  runtime: { apiGroup: ome.io, kind: ClusterServingRuntime, name: ome-coordtest-metrics, autoSync: true }
  engine:
    minReplicas: 2
    maxReplicas: 2
    annotations: { ome.io/deploymentMode: OMENative, perm.test/error-rate: "0.0", perm.test/emit: "true", prometheus.io/scrape: "true", prometheus.io/port: "8080", prometheus.io/path: "/metrics" }
    lifecycle: { updateStrategy: { type: "SurgeThenDrain", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } } }
  rollout:
    groups:
        - components: [engine]
          canary:
            prometheus: { serverAddress: "http://prom-delay-proxy.__NS__.svc:9090" }
            steps:
              - capacity: "50%"
                traffic: 50
                analysis:
                  interval: "15s"
                  failureLimit: 100
                  onInconclusive: Hold
                  metrics:
                    - name: error_rate
                      query: 'max(canary_error_rate{inferenceservice="{{.ISVCName}}",component="{{.Component}}"})'
                      operator: LTE
                      threshold: "0.05"
              - { capacity: "100%", traffic: 100 }
"""


def kubectl(*args, **kw):
    return subprocess.run(["kubectl", *args], env=ENV, capture_output=True, text=True, **kw)


def phase(name):
    r = kubectl("-n", NS, "get", "isvc", name, "-o", "json")
    if r.returncode:
        return None
    st = json.loads(r.stdout or "{}").get("status", {}) or {}
    return ((st.get("components", {}) or {}).get("engine", {}) or {}).get("rolloutPhase", "")


def ready(name):
    r = kubectl("-n", NS, "get", "pods", "-l",
                f"ome.io/inferenceservice={name},ome.io/component=engine", "-o", "json")
    if r.returncode:
        return 0
    return sum(1 for p in json.loads(r.stdout or "{}").get("items", [])
               if {c["type"]: c["status"] for c in (p.get("status", {}).get("conditions") or [])}.get("Ready") == "True")


def main():
    argv = sys.argv[1:]
    n = 5
    timeout = 300
    i = 0
    while i < len(argv):
        if argv[i] == "--timeout":
            timeout = int(argv[i + 1]); i += 2
        else:
            n = int(argv[i]); i += 1
    control = "hung-control"
    hung = [f"hung-{j:03d}" for j in range(n)]
    print(f"=== hung-prom isolation: {n} hung canaries + 1 control, ns={NS}, timeout={timeout}s ===")

    kubectl("apply", "-f", RUNTIME)
    kubectl("apply", "-n", NS, "-f", PROXY)
    docs = [CONTROL.replace("__NAME__", control)]
    docs += [HUNG.replace("__NAME__", h).replace("__NS__", NS) for h in hung]
    kubectl("apply", "-n", NS, "-f", "-", input="\n---\n".join(docs))

    print("waiting for initial Ready …")
    t0 = time.time()
    everyone = [control] + hung
    while time.time() - t0 < timeout and any(ready(x) < 2 for x in everyone):
        time.sleep(5)

    # Bump all at once: control rolls; the hung ones start churning queries.
    rev = json.dumps({"spec": {"engine": {"annotations": {"perm.test/rev": "v2", "ome.io/deploymentMode": "OMENative"}}}})
    bump_ts = time.time()
    for x in everyone:
        kubectl("-n", NS, "patch", "isvc", x, "--type=merge", "-p", rev)
    print(f"bumped; waiting for the CONTROL to reach Stable (hung canaries query a 30s-delayed source) …")

    control_secs = None
    while time.time() - bump_ts < timeout:
        p = phase(control)
        if p == "Stable":
            control_secs = time.time() - bump_ts
            break
        print(f"  [t={int(time.time() - bump_ts):4d}s] control={p}  hung[0]={phase(hung[0]) if hung else '-'}")
        time.sleep(5)

    print("\n" + "=" * 60)
    ok = control_secs is not None
    if ok:
        print(f"PASS — control reached Stable in {control_secs:.0f}s despite {n} hung-prom canaries")
        print("       (a slow analysis source did NOT stall the reconcile worker)")
    else:
        print(f"FAIL — control did NOT reach Stable within {timeout}s with {n} hung-prom canaries")
        print("       (the slow source may be stalling the single reconcile worker)")
    print(f"  hung canary phases: {{ {', '.join(f'{h}={phase(h)}' for h in hung[:5])} }}")
    print("=" * 60)

    kubectl("-n", NS, "delete", "isvc", *everyone, "--wait=false")
    kubectl("-n", NS, "delete", "-f", PROXY, "--wait=false")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
