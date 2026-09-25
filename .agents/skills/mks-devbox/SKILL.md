---
name: mks-devbox
description: Start, use and remove an interactive devbox pod on an MKS cluster for OME development - build the operator, run the unit and envtest suites, run the KIND suites inside the pod through a docker-in-docker sidecar, optionally with GPUs attached. Use when the user says "devbox on MKS", "dev pod on MKS", "MKS shell with GPUs", "run test-kind in a pod on MKS", or needs a Linux box with the OME toolchain next to the MKS GPU nodes.
---

# MKS devbox for OME development

A devbox is one bare `Pod` you own, in a namespace you can write to, running
the repository's own CI toolchain image `kind-ci`
(`dockerfiles/kind-ci.Dockerfile`: Go 1.26, Rust, docker CLI, kind, kubectl,
Helm, crane, jq, and the kind node image as a tarball under `/images`). Add a
privileged `docker:dind` sidecar when you need Docker or KIND inside the pod -
the same two-container shape the GKE CI jobs use
(`.buildkite/README-gke-kind.md`, `.buildkite/run_kind_tests_gke.sh`) - and
`nvidia.com/gpu` only when you will run accelerator code in the pod.

| Reference | Load when |
|---|---|
| `references/pod-shape.md` | you need the exact pod spec the renderer emits, which flags map to which fields, and the MKS-specific bits (identity annotations, proxy, GPU taints) |
| `references/in-pod-workflows.md` | you are inside the pod: syncing the tree, building, envtest, KIND-in-pod, running the manager against the in-pod KIND cluster |

## Prerequisites

- **A devserver.** `cloud k8s generate-kubeconfig` does not issue MKS
  kubeconfigs to laptops. `cloud` itself: `feature install cloud`.
- Membership in the namespace's ACL. `playground` is the conventional devbox
  namespace on the engineering dev cluster; `kubectl auth can-i create pods`
  is the test, `forbidden` means request access, not a workaround.
- Python 3 on the devserver (the renderer is standard library only) and
  `kubectl`.
- Registry access from the node to prod ECR for `kind-ci` - see
  Troubleshooting if the pod lands in `ImagePullBackOff`.

## Cluster choice

| Cluster (`cloud` target) | Purpose |
|---|---|
| `mks/prod-msl-capi-lco-0` | infrastructure dev (also the GitOps management plane) |
| `mks/prod-msl-capi-lco-1` | capacity fabric |
| `mks/prod-msl-capi-lco-2` | mkube |
| `mks/prod-msl-capi-lco-4` | engineering dev - default here, `playground` namespace |
| `mks/prod-msl-capi-lco-5` | engineering prod |

## Contract (edit, then run the steps in one shell)

```sh
set -euo pipefail
export MKS_CLUSTER=prod-msl-capi-lco-4
export KC="$HOME/.kube/$MKS_CLUSTER.conf"            # one file per cluster; never ~/.kube/config
export NS=playground
export DEVBOX="devbox-$(id -un | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')"
export IMAGE=168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest
export RUN_DIR="$HOME/.cache/ome-agent.noindex/scratch/mks-devbox/$DEVBOX"
mkdir -p "$RUN_DIR" "$(dirname "$KC")"
[ -s "$KC" ] || cloud k8s generate-kubeconfig "mks/$MKS_CLUSTER" --path "$KC"
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
```

## Step 1 - render and create the pod

The renderer prints a Pod manifest (JSON) to stdout and writes nothing:

```sh
python3 .agents/skills/mks-devbox/scripts/render_devbox_pod.py \
  --name "$DEVBOX" --namespace "$NS" --image "$IMAGE" \
  --proxy http://fwdproxy:8080 \
  --dind \
  > "$RUN_DIR/pod.json"                    # add: --gpus 4  when you need accelerators
k -n "$NS" create --dry-run=server -f "$RUN_DIR/pod.json" -o name
k -n "$NS" create -f "$RUN_DIR/pod.json"
k -n "$NS" wait --for=condition=Ready "pod/$DEVBOX" --timeout=10m
```

Flags: `--gpus N` requests `nvidia.com/gpu`; `--dind` adds the privileged
docker sidecar and sets `DOCKER_HOST` in the dev container; `--proxy URL` sets
`HTTP(S)_PROXY` for Go module, git and package fetches (`fwdproxy:8080`
resolves on MKS); `--annotation k=v` / `--label k=v` / `--node-selector k=v` /
`--toleration key[=value]:Effect` pass through. Pods that need cloud
credentials inside (ECR pulls into dind, `crane`) carry the cluster's
pod-identity annotations; copy them from a pod that already has them in the
namespace rather than guessing:
`k -n "$NS" get pod <existing> -o jsonpath='{.metadata.annotations}'`.

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
(`make test-kind-perm-up`, Makefile), not a copy of a real-cluster kubeconfig.

## Step 4 - GPUs (optional)

With `--gpus N` the dev container sees the devices (`nvidia-smi`). OME itself
needs none; request them only for runtime images or kernels you will exercise
in the pod, and release the pod when done - a Pending devbox holding a GPU
request blocks other users.

## Cleanup

A devbox is a bare pod with no controller: deleting it is permanent.

```sh
k -n "$NS" delete pod "$DEVBOX" --wait=true
k -n "$NS" get pods -l app=devbox,user="${DEVBOX#devbox-}"
```

## Troubleshooting

- **`Pending` with `Insufficient nvidia.com/gpu`** - drop `--gpus` or choose
  another cluster; the toolchain needs no GPU.
- **`ImagePullBackOff` on `kind-ci`** - the node cannot pull prod ECR. Point
  `--image` at an image the cluster mirrors (`golang:1.26` through
  `pt_dockerio/library/`) and install tools by hand, or add an image pull
  secret the namespace already has (`--image-pull-secret NAME`).
- **pod rejected: privileged not allowed** - the namespace enforces a
  restricted PodSecurity level. Recreate without `--dind`; KIND then has to run
  on a host with Docker (`make test-kind` locally, or the GKE CI lane).
- **`go mod download` / `git` time out** - set `--proxy`; inside the pod the
  variables are already exported, so re-check `NO_PROXY` covers `.svc` and
  `localhost`.
- **`forbidden` creating pods** - namespace ACL missing; request it.
- **a `kind-*` context in `$KC`** - a kind run inherited the real kubeconfig.
  Remove those entries from that file only and give kind a temp file.

## Not covered

- Multi-pod fabrics (MPI hostfiles, NCCL/NVLink domain grouping), checkpoint
  PVC mounts and model downloads into the pod: not part of an OME dev loop.
- Launching a serving engine on the devbox's GPUs; deploy an
  `InferenceService` through OME instead.
