# Track A — the GKE Buildkite agent queue for `integration-test-kind-gke`

This is the setup to stand up a dedicated **`gke-kind-x86`** Buildkite queue on
the MSL GCP cluster for the grouped OMENative KIND suite. The suite already runs
in CI: `generate_pipeline.py` emits `integration-test-kind-gke` **by default**,
targeting the live `gke-tpu-v7x` queue (via the per-step `kubernetes` plugin,
which lands the job on that pool's CPU nodes). The package step and every named
test job gate the build. Disable the group only as an emergency measure with
`GKE_KIND_ENABLED=0`; retarget it with `GKE_KIND_QUEUE=<queue>` once this
dedicated CPU queue exists. Keep any queue you point it at live—a step targeting
a queue with no agents waits indefinitely.

## Why GKE (recap)

The KIND suite needs real `kind` node containers, which need bridge networking
and writable net sysctls (`net.ipv4.ip_forward=1`, …). The TW dind forbids that
(read-only `/proc/sys/net`), so the suite runs on GKE. **We proved** on
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
  Job pod (one per named semantic group)
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
2. **A node pool** sized for the fan-out. Each job pod runs one three-node KIND
   cluster and requests 5 vCPU, 14 GiB RAM, and 24 GiB ephemeral storage across
   its workload and dind containers. `KIND_SPEC_GROUPS` controls the number of
   concurrent jobs; leave room for jobs to queue when the full fan-out exceeds
   available capacity.
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
3. Point the step at the new queue with **`GKE_KIND_QUEUE=gke-kind-x86`** in the
   `ome` pipeline env (the step is already emitted by default on `gke-tpu-v7x`).
4. Open a PR touching `.buildkite/`; confirm every named semantic job owns a
   disjoint file set and all jobs pass. The package and test jobs are hard
   gates, so any failure blocks the merge.

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
## Version pins

`.kind-node-image` at the repo root is the single source of truth for the node
image, the way `.envtest-k8s-version` is for the envtest control plane. The kind
binary comes from the `sigs.k8s.io/kind` require in `hack/internal/tools/go.mod`
— the same pin `make kind` installs. Two constraints when bumping: the node
image's minor must match `.envtest-k8s-version`, so the KIND and envtest lanes
exercise the same Kubernetes API; and the pinned `kind` release must publish a
node image for that tag (its release notes list them).

`package_kind_test_image.sh` reads both files and appends the binary and the
node tarball to the runner image, shadowing the copies `kind-ci` bakes. That
indirection is load-bearing, not tidiness: `kind-ci` only builds and mirrors
from `main`, and a PR branch's runner is assembled on top of `kind-ci:latest`.
A pin baked only into the Dockerfile therefore cannot reach the branch that
raises it — the runner asks for a node image it has no egress to fetch and dies
in `kind create cluster`. The `ARG`s in `dockerfiles/kind-ci.Dockerfile` are
fallbacks for standalone use of the image; keep them tracking the two files.

## Alfred lane

`integration-test-kind-alfred-gke` runs `tests/integration/singlecluster/controller/alfred_kind`
from `/kind-test/alfred-kind-test.tgz`, one four-node cluster (three workers)
per group in `ALFRED_KIND_SPEC_GROUPS`. The suite deploys the alfred image this
pipeline built from `.alfred-oss-ref`: `build-alfred` pushes its commit tag on
every branch (`latest` stays main-only), `package_kind_test_image.sh` pulls that
tag into `/images/alfred.tar`, and `run_kind_tests_gke.sh` exports
`OME_ALFRED_IMAGE` after loading it. Kill switch: `GKE_ALFRED_ENABLED=0` drops
the lane, the package step's wait on and requirement for the alfred image, and
the every-branch push, so the image publishes on main only like every other.
Soft-fail until it has soaked; promote by removing `soft_fail` in
`_alfred_kind_group`.
