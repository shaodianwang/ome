#!/usr/bin/env python3
"""scale.py [K] [--timeout S] — fleet-scale canary-analysis soak.

Launches K identical PASS analysis canaries at once, bumps them together, and
measures how many converge (-> Stable) within the timeout plus p50/p95/max
convergence. This quantifies how many concurrent analysis canaries the controller
(MaxConcurrentReconciles=1) services before it falls behind — i.e. whether the
async sampler keeps analysis canaries at parity with plain rollouts under load.
Run escalating K (10, 20, 30, …) to find the ceiling.

Real cluster; env like run.py: OME_TEST_KUBECONFIG (else KUBECONFIG, else
~/.kube/config), OME_TEST_NS (default sandbox). Needs the runtime fixtures
(`kubectl apply -f ../runtime`) and the bundled ome-prometheus enabled.

  python3 scale.py 10
  python3 scale.py 30 --timeout 900
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(os.path.dirname(HERE), "runtime")
KUBECONFIG = os.environ.get("OME_TEST_KUBECONFIG") or os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
NS = os.environ.get("OME_TEST_NS", "sandbox")
ENV = {**os.environ, "KUBECONFIG": KUBECONFIG}

# __NAME__ placeholder keeps the PromQL {…} + {{.Tmpl}} braces literal (no
# f-string escaping). A small 2-replica PASS analysis canary.
TEMPLATE = """apiVersion: ome.io/v1beta1
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
            steps:
              - capacity: "50%"
                traffic: 50
                analysis:
                  interval: "20s"
                  failureLimit: 1
                  initialDelay: "20s"
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
    # OME stamps the plain `component` label (constants.OMEComponentLabel), not an
    # ome.io/-prefixed one; only inferenceservice is ome.io/-prefixed.
    r = kubectl("-n", NS, "get", "pods", "-l",
                f"ome.io/inferenceservice={name},component=engine", "-o", "json")
    if r.returncode:
        return 0
    n = 0
    for p in json.loads(r.stdout or "{}").get("items", []):
        conds = {c["type"]: c["status"] for c in (p.get("status", {}).get("conditions") or [])}
        if conds.get("Ready") == "True":
            n += 1
    return n


def pctl(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    i = min(len(xs) - 1, int(q * len(xs)))
    return xs[i]


def main():
    argv = sys.argv[1:]
    k = 10
    timeout = 900
    i = 0
    while i < len(argv):
        if argv[i] == "--timeout":
            timeout = int(argv[i + 1]); i += 2
        else:
            k = int(argv[i]); i += 1
    names = [f"scale-{n:03d}" for n in range(k)]
    print(f"=== scale: {k} concurrent analysis canaries, ns={NS}, timeout={timeout}s ===")

    kubectl("apply", "-f", RUNTIME)
    kubectl("apply", "-n", NS, "-f", "-", input="\n---\n".join(TEMPLATE.replace("__NAME__", n) for n in names))

    # Wait for every canary's 2 engine pods Ready (bounded).
    print("waiting for initial Ready …")
    t0 = time.time()
    while time.time() - t0 < timeout:
        notready = [n for n in names if ready(n) < 2]
        if not notready:
            break
        time.sleep(5)
    print(f"  ready after {int(time.time() - t0)}s ({k - len(notready)}/{k})" if 'notready' in dir() else "")

    # Bump all together (rev + healthy error-rate) → all canaries start at once.
    patch = json.dumps({"spec": {"engine": {"annotations": {
        "perm.test/rev": "v2", "perm.test/error-rate": "0.01", "ome.io/deploymentMode": "OMENative"}}}})
    bump_ts = time.time()
    for n in names:
        kubectl("-n", NS, "patch", "isvc", n, "--type=merge", "-p", patch)
    print(f"bumped {k} canaries; polling for Stable …")

    conv = {}
    while time.time() - bump_ts < timeout and len(conv) < k:
        for n in names:
            if n not in conv and phase(n) == "Stable":
                conv[n] = time.time() - bump_ts
        done = len(conv)
        print(f"  [t={int(time.time() - bump_ts):4d}s] converged {done}/{k}")
        if done == k:
            break
        time.sleep(5)

    times = list(conv.values())
    print("\n" + "=" * 60)
    print(f"SCALE RESULT  K={k}")
    print(f"  converged : {len(conv)}/{k}")
    print(f"  timed out : {k - len(conv)}  {sorted(set(names) - set(conv))[:10]}")
    if times:
        print(f"  conv p50  : {pctl(times, 0.50):.0f}s")
        print(f"  conv p95  : {pctl(times, 0.95):.0f}s")
        print(f"  conv max  : {max(times):.0f}s")
    print("=" * 60)

    kubectl("-n", NS, "delete", "isvc", *names, "--wait=false")
    sys.exit(0 if len(conv) == k else 1)


if __name__ == "__main__":
    main()
