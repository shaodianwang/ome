---
name: cks-devbox
description: Start, use and remove an interactive devbox pod on a CKS (CoreWeave) cluster for OME development - build the operator, run the unit and envtest suites, run the KIND suites inside the pod through a docker-in-docker sidecar, optionally with GPUs attached, next to the clusters the OMENative harness targets. Use when the user says "devbox on CKS", "dev pod on CoreWeave", "CKS shell with GPUs", "run test-kind in a pod on the dev cluster".
---

# CKS devbox for OME development

A devbox is one bare `Pod` you own, in a namespace you can write to, running
the repository's own CI toolchain image `kind-ci`
(`dockerfiles/kind-ci.Dockerfile`: Go 1.26, Rust, docker CLI, kind, kubectl,
Helm, crane, jq, and the kind node image as a tarball under `/images`). Add a
privileged `docker:dind` sidecar when you need Docker or KIND inside the pod -
the two-container shape the GKE CI jobs use (`.buildkite/README-gke-kind.md`,
`.buildkite/run_kind_tests_gke.sh`) - and `nvidia.com/gpu` only when you will
run accelerator code in the pod.

| Reference | Load when |
|---|---|
| `references/pod-shape.md` | you need the exact pod spec the renderer emits, which flags map to which fields, and the CKS-specific bits (in-cluster proxy, GPU taints, NVLink domains) |
| `references/in-pod-workflows.md` | you are inside the pod: syncing the tree, building, envtest, KIND-in-pod, running the manager against the in-pod KIND cluster |

## Prerequisites

- `cloud` CLI (`feature install cloud`) on a devserver, and read access to at
  least one CKS cluster (`cloud k8s list cw`).
- A writable namespace on that cluster. On the OME dev clusters the OMENative
  harness works in `sandbox` (`OME_TEST_NS` default,
  `hack/omenative-testing/perm/run.py`); `kubectl auth can-i create pods` is
  the test, `forbidden` means request access.
- Python 3 on the devserver (the renderer is standard library only) and
  `kubectl`.

## Cluster-first rule

Do not deploy on the first cluster you remember. List, pick, verify, then
create - and prefer the cluster whose OME install you are actually testing
against, so the devbox and the workload share a control plane:

```sh
cloud k8s list cw
```

## Contract (edit, then run the steps in one shell)

```sh
set -euo pipefail
export CKS_CLUSTER=msl-cw-use2-dev-2                       # from `cloud k8s list cw`
export KC="$HOME/.kube/$CKS_CLUSTER.conf"                  # one file per cluster; never ~/.kube/config
export NS=sandbox
export DEVBOX="devbox-$(id -un | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')"
export IMAGE=168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest
export PROXY=http://mtls-proxy.mtls-proxy.svc.cluster.local:8080   # FQDN: short forms break non-glibc resolvers
export RUN_DIR="$HOME/.cache/ome-agent.noindex/scratch/cks-devbox/$DEVBOX"
mkdir -p "$RUN_DIR" "$(dirname "$KC")"
[ -s "$KC" ] || cloud k8s generate-kubeconfig "cw/$CKS_CLUSTER" --path "$KC"
export CTX="$(kubectl --kubeconfig "$KC" config get-contexts -o name | head -1)"
k() { kubectl --kubeconfig "$KC" --context "$CTX" "$@"; }
```

Every command selects the kubeconfig explicitly. Never `kubectl config
use-context` on `$KC`, and never export `KUBECONFIG=$KC` in a shell that will
run `kind` - the in-pod KIND workflow below keeps its own temp kubeconfig.

## Step 0 - preflight

```sh
k get nodes -o wide | head -5
k -n "$NS" auth can-i create pods                          # must be yes
k get ns "$NS" -o jsonpath='{.metadata.labels}{"\n"}'      # pod-security.kubernetes.io/enforce decides whether --dind can run
k -n "$NS" get pods -l app=devbox -o wide                  # an existing devbox of yours? reuse it
# only when requesting GPUs: is there a node with that many free?
k get nodes -o json | jq -r '.items[] | select(.status.allocatable["nvidia.com/gpu"] != null) | "\(.metadata.name) \(.status.allocatable["nvidia.com/gpu"])"' | head
```

## Step 1 - render and create the pod

The renderer prints a Pod manifest (JSON) to stdout and writes nothing:

```sh
python3 .agents/skills/cks-devbox/scripts/render_devbox_pod.py \
  --name "$DEVBOX" --namespace "$NS" --image "$IMAGE" \
  --proxy "$PROXY" \
  --dind \
  > "$RUN_DIR/pod.json"                    # add: --gpus 4  when you need accelerators
k -n "$NS" create --dry-run=server -f "$RUN_DIR/pod.json" -o name
k -n "$NS" create -f "$RUN_DIR/pod.json"
k -n "$NS" wait --for=condition=Ready "pod/$DEVBOX" --timeout=10m
```

Flags: `--gpus N` requests `nvidia.com/gpu`; `--dind` adds the privileged
docker sidecar and sets `DOCKER_HOST` in the dev container; `--proxy URL` sets
`HTTP(S)_PROXY` for Go module, git and package fetches (the CKS in-cluster
proxy above; the MKS `fwdproxy` name does not resolve here);
`--annotation k=v` / `--label k=v` / `--node-selector k=v` /
`--toleration key[=value]:Effect` / `--image-pull-secret NAME` pass through.

## Step 2 - connect and bring the tree in

```sh
k -n "$NS" exec -it "$DEVBOX" -c dev -- bash
```

From the devserver, stream the checkout in (faster and more reliable than
`kubectl cp` for a large tree; build outputs are excluded):

```sh
REPO=/path/to/your/ome/checkout
tar -C "$REPO" --exclude=./bin --exclude=./pkg/xet/target --exclude=./muse/target -cf - . \
  | k -n "$NS" exec -i "$DEVBOX" -c dev -- sh -c 'mkdir -p /work/ome && tar -xf - -C /work/ome'
```

Re-run to sync edits. Alternatively `git clone` inside the pod through the
proxy with your own credentials; never bake a token into the pod spec.

## Step 3 - build and test inside the pod

```sh
cd /work/ome
make ome-manager                 # builds the Rust xet lib first; toolchains are baked in
make test-no-xet                 # unit suite without the Rust link
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
make integration-test            # envtest suites (needs the proxy for the first asset download)
```

KIND inside the pod (requires `--dind`): docker talks to the sidecar over
`DOCKER_HOST`, the node image comes from the baked tarball, and the cluster's
kubeconfig is a temp file - the ambient one is never touched:

```sh
until docker info >/dev/null 2>&1; do sleep 2; done
docker load -i /images/kindest-node.tar
export KIND_NODE_IMAGE="$(tr -d '[:space:]' < .kind-node-image | cut -d@ -f1)"
docker run --rm --privileged busybox sh -c 'sysctl -w fs.inotify.max_user_watches=1048576 fs.inotify.max_user_instances=8192' || true
export KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)"
GINKGO_FOCUS='rollout converges' make test-kind
make test-kind-clean; rm -f "$KUBECONFIG"
```

To run the manager from source against an in-pod cluster, use the perm harness
target that brings up KIND plus CRDs and then expects an external manager
(`make test-kind-perm-up`, Makefile). The real-cluster harness
(`hack/omenative-testing/perm/run.py`, `OME_TEST_KUBECONFIG`) runs from the
devserver with your kubeconfig - do not copy a personal kubeconfig into a pod.

## Step 4 - GPUs (optional)

With `--gpus N` the dev container sees the devices (`nvidia-smi`). OME itself
needs none; request them only for runtime images or kernels you will exercise
in the pod, and release the pod when done. Multi-node GPU fabrics (one NVLink
domain per group, `ds.coreweave.com/nvlink.domain`) are out of scope for a
single devbox.

## Cleanup

A devbox is a bare pod with no controller: deleting it is permanent.

```sh
k -n "$NS" delete pod "$DEVBOX" --wait=true
k -n "$NS" get pods -l app=devbox,user="${DEVBOX#devbox-}"
```

## Troubleshooting

- **`Pending` with `Insufficient nvidia.com/gpu`** - drop `--gpus`, or pick
  a node pool with free devices (Step 0 listing); the toolchain needs no GPU.
- **`ImagePullBackOff` on `kind-ci`** - the cluster cannot pull prod ECR
  directly. Use an image it mirrors (`pt_dockerio/library/golang:1.26` on the
  same registry, see `.buildkite/eval_image_mirror.sh` for the mirror layout)
  or `--image-pull-secret` with a secret the namespace already has.
- **pod rejected: privileged not allowed** - the namespace enforces a
  restricted PodSecurity level. Recreate without `--dind`; KIND then has to run
  on a host with Docker (`make test-kind` locally, or the GKE CI lane).
- **`go mod download` / `git` time out** - the proxy variables are exported in
  the dev container; check `NO_PROXY` covers `localhost`, `.svc`,
  `.svc.cluster.local`, and use the proxy FQDN, not the short form.
- **`forbidden` creating pods** - namespace ACL missing; request it.
- **a `kind-*` context in `$KC`** - a kind run inherited the real kubeconfig.
  Remove those entries from that file only and give kind a temp file.

## Not covered

- Multi-pod fabrics (MPI hostfiles, NCCL, NVLink domain grouping), node IPv6
  health probing, checkpoint PVC mounts and model downloads into the pod: not
  part of an OME dev loop.
- Launching a serving engine on the devbox's GPUs; deploy an
  `InferenceService` through OME instead.
