# Track A — the GKE Buildkite agent queue for `integration-test-kind-gke`

This is the setup to stand up the **`gke-kind-x86`** Buildkite queue on the MSL
GCP cluster so the sharded OMENative KIND suite runs in CI. Until it exists the
`integration-test-kind-gke` step is **not emitted** — `generate_pipeline.py`
adds it only when `GKE_KIND_ENABLED` is truthy (a step targeting a queue with no
agents would hang every build; `soft_fail` doesn't help because the job never
runs to fail).

## Why GKE (recap)

The KIND suite needs real `kind` node containers, which need bridge networking
and writable net sysctls (`net.ipv4.ip_forward=1`, …). The TW dind forbids that
(read-only `/proc/sys/net`), so its step is parked. **We proved** on
`msl-gcp-usc1-rc-1` that a **privileged** pod there *can* boot kind offline and
its pod networking works — so GKE is the path.

## The hard constraint that shapes everything: no pod egress

The RC cluster has **no public egress from pods** (Docker Hub, `dl.k8s.io`,
`proxy.golang.org`, `crates.io`, `gcr.io` all time out — measured). This splits
the run into two egress domains:

| Needs egress? | How it's satisfied |
|---|---|
| kind node image, Go modules, cargo crates, fake-serving bases, tools | **None** — all baked into the `kind-ci` image (built on TW, pulled from prod ECR, `docker load`ed into dind). |
| Buildkite agent ↔ Buildkite API (`agent.buildkite.com`) | **Corp proxy** (the `with-proxy` / `HTTPS_PROXY` mechanism). The controller + agent must be proxy-configured. |
| Git checkout of `mslsrc/ome` | **Corp proxy** (or an in-cluster git mirror). |

So: the **test workload is fully offline**; only the **agent plumbing**
(job polling + checkout) needs egress, via the corp proxy. This is the single
most important thing to confirm with Cloud Foundation — see the checklist.

## Architecture

```
agent-stack-k8s controller (Deployment on the GKE cluster)
  • polls Buildkite API via the corp proxy for `queue=gke-kind-x86` jobs
  • for each job, creates a Job pod from the queue's pod-spec-patch
        │
        ▼
  Job pod (one per shard)
   ├─ checkout (agent-stack-k8s init): git clone mslsrc/ome via proxy
   ├─ container "container-0": image = kind-ci (prod ECR); runs
   │     sh .buildkite/run_kind_tests_gke.sh  (offline; DOCKER_HOST→dind)
   └─ sidecar "dind": docker:28-dind, privileged, shares pod netns
```

## Prerequisites to confirm with Cloud Foundation (the open questions)

1. **Privileged pods** allowed in the agent namespace — PodSecurity `privileged`
   (not `baseline`/`restricted`) and no Gatekeeper/Policy-Controller block on
   `privileged` / `hostPath`. (We ran a privileged Job in `def-sandbox` for the
   validation, so this is likely already OK there.)
2. **A node pool** sized for the fan-out. Each shard pod ≈ dind + kind (2 nodes)
   + Go test compile → budget ~8–12 Gi RAM, ~4 vCPU, ~20 Gi ephemeral **per
   pod**. For `parallelism: 8`, size for 8 concurrent pods (e.g. 8×
   `e2-standard-8`, or fewer larger nodes; leave headroom).
3. **Corp proxy egress** for the controller + agent to reach
   `agent.buildkite.com` (job polling) and the git remote for `mslsrc/ome`
   (checkout). Confirm the proxy endpoint/env (`HTTPS_PROXY`, `NO_PROXY`) or the
   in-cluster git mirror to use. **Without this the agents cannot run.**
4. **ECR pull for job pods**: the node already pulls prod ECR (proven — the
   validation pod pulled `168653207203…`). So the `kind-ci` image pulls with the
   node's existing creds; no imagePullSecret expected. Confirm this holds in the
   agent namespace.
5. **Buildkite agent token** available as a cluster Secret (org/cluster the
   `ome` pipeline lives in).

## agent-stack-k8s install (sketch — adapt to your chart version)

Deploy a **dedicated controller** for this queue so its pod-spec-patch only
affects kind jobs. Helm values:

```yaml
# values-gke-kind.yaml  (helm install agent-stack-k8s buildkite/agent-stack-k8s -f …)
config:
  org: <buildkite-org>
  tags:
    - queue=gke-kind-x86
  # Controller needs the Buildkite API via the corp proxy.
  # (set on the controller Deployment env too, depending on chart version)
  agent-token-secret: buildkite-agent-token      # k8s Secret with the token

  # Applied to every job pod on this queue: workload image + privileged dind.
  pod-spec-patch:
    containers:
      # agent-stack-k8s injects the checkout + your step command into the
      # first ("container-0") container; point it at the offline kind-ci image.
      - name: container-0
        image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest
        env:
          - { name: DOCKER_HOST, value: "tcp://localhost:2375" }
          # proxy so the in-container git ops / any agent calls can egress
          - { name: HTTPS_PROXY, value: "<corp-proxy>" }
          - { name: HTTP_PROXY,  value: "<corp-proxy>" }
          - { name: NO_PROXY,    value: "localhost,127.0.0.1,.svc,.cluster.local,169.254.169.254" }
        resources:
          requests: { cpu: "3", memory: 8Gi, ephemeral-storage: 8Gi }
          limits:   { cpu: "6", memory: 12Gi, ephemeral-storage: 16Gi }
      - name: dind
        image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/library/docker:28-dind
        securityContext: { privileged: true }
        env:
          - { name: DOCKER_TLS_CERTDIR, value: "" }   # listen on tcp://…:2375, no TLS
        volumeMounts:
          - { name: docker-graph, mountPath: /var/lib/docker }
        resources:
          requests: { cpu: "2", memory: 6Gi, ephemeral-storage: 16Gi }
          limits:   { cpu: "4", memory: 8Gi, ephemeral-storage: 24Gi }
    volumes:
      - { name: docker-graph, emptyDir: {} }

  # The checkout/agent image itself must reach Buildkite + git via the proxy —
  # set HTTPS_PROXY/HTTP_PROXY/NO_PROXY on the controller + the agent sidecar
  # (chart-version-specific; see agent-stack-k8s docs for `agent` env).
```

Notes:
- `container-0` image = `kind-ci` means the step's command (`sh
  .buildkite/run_kind_tests_gke.sh`) runs with the full offline toolchain and
  `DOCKER_HOST` pointed at the sidecar — exactly what the by-hand shard Job proves.
- The dind + kind-ci images are referenced on **prod ECR** so the node pulls
  them with its existing creds (no public pull).
- `docker:28-dind` via the `pt_dockerio` pull-through (adjust region/path if your
  cluster's pull-through differs — ours resolved `pt_dockerio/library/...`).

## Secrets to create

```bash
kubectl -n <agent-ns> create secret generic buildkite-agent-token \
  --from-literal=token=<BUILDKITE_AGENT_TOKEN>
```
(ECR pull is via node creds; no image-pull secret expected — confirm per #4.)

## Going live

1. Confirm the 5 prerequisites (esp. #3 proxy egress) with Cloud Foundation.
2. `helm install` the controller with the values above; verify agents register
   under `queue=gke-kind-x86` in Buildkite.
3. Set **`GKE_KIND_ENABLED=1`** in the `ome` pipeline env (optionally
   `GKE_KIND_PARALLELISM` / `GKE_KIND_QUEUE`) so the step is emitted.
4. Open a PR touching `.buildkite/`; confirm the step fans into 8 jobs, each
   runs a disjoint 9-file shard, all green. It ships `soft_fail: true` so this
   first run doesn't gate merges.
5. Once reliably green: flip `soft_fail: false` in `_gke_kind_step`
   (`generate_pipeline.py`) and **delete the parked TW `integration-test-kind`
   step**.

## How this was de-risked before the queue existed

- `gke-kind-validation-job.yaml` / the v2 Job: proved privileged kind boots +
  networks offline on the cluster (`PASS`).
- `gke-kind-shard-job.yaml` (by-hand): proves the real suite compiles + runs a
  spec offline in the `kind-ci` image. Run it while this queue is being stood up.

## Open follow-ups

- **Repo checkout source**: confirm whether the agent clones `mslsrc/ome` from
  github via proxy or an in-cluster mirror (#3). If neither is available, bake
  the repo into a CI image variant (heavier; only if unavoidable).
- **Image freshness**: `kind-ci` bakes the Go module + cargo caches at build
  time; it rebuilds every main build, so caches track `go.mod`/`Cargo.lock`. A
  PR that changes deps but doesn't rebuild `kind-ci` first could miss modules
  (`GOPROXY=off` fails fast, so it's visible, not silent).
- **Node-image pin**: `KIND_NODE_TAG` in `dockerfiles/kind-ci.Dockerfile` and
  `KIND_NODE_IMAGE` in `run_kind_tests_gke.sh` must stay in lockstep with the
  pinned `kind` release's bundled default.
