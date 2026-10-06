#!/usr/bin/env bash
# Stand up a kind cluster running OME against kwok-simulated TPU nodes.
#
# Usage:
#   hack/kwok-sim/up.sh [--skip-build] [--cluster NAME] [--cubes 4x4x4,2x2x2]
#                       [--component NAME]... [--no-scheduler] [--with-isvcs]
#                       [--with-kueue] [--with-routing]
#
#   --component NAME
#                   build and deploy only this OME component; repeat the flag
#                   for more than one. Omitted, all of them are built and
#                   deployed. One of:
#                     manager        ome-manager + its webhooks, the model and
#                                    runtime fixtures
#                     scheduler      ome-scheduler and the PodGroup CRD
#                     quota-manager  ome-quota-manager, the Kueue CRDs it reads,
#                                    and the ResourceFlavor the fleet maps to.
#                                    No AcceleratorQuota is created; the
#                                    controller makes its own reserved root.
#                                    Add --with-kueue for Kueue itself.
#                   The simulator itself — kind, kwok, the fake nodes, the OME
#                   CRDs — is ensured on every run whatever is selected, so one
#                   component can be re-rolled against a cluster already up.
#   --no-scheduler  skip ome-scheduler and use the default scheduler; implies
#                   --topology strict, the full production contract.
#   --no-nodes      skip kwok and the simulated nodes entirely. For a cluster
#                   that carries no silicon of its own — a management plane
#                   measures nothing locally, it reads what each member derived.
#   --multicluster-role R
#                   run ome-manager in a multi-cluster role; "control-plane" is
#                   the only one. It turns ON the WorkloadCluster registry, whose
#                   reconciler is the single writer of the Ready condition every
#                   remote client waits for, and turns OFF the local
#                   InferenceService, BaseModel and ServingRuntime controllers,
#                   which would otherwise churn against an empty local catalog.
#                   Needs the manager component.
#   --with-routing enable TrafficMap and HTTPRoute publication using
#                   routing-values.yaml. Needs --multicluster-role control-plane.
#                   Installs the HTTPRoute CRD before the manager starts; no
#                   Gateway controller or HTTP serving is installed.
#   --topology M    podgroup (default) | plugin | strict — where slice
#                   co-location comes from. See render-runtimes.sh, and the
#                   README on why strict + ome-scheduler wedges.
#   --with-isvcs    also apply fixtures/2*-isvc-*.yaml and wait for them.
#                   Needs the manager component.
#   --with-kueue    also install Kueue itself, not just its CRDs, so a rendered
#                   ClusterQueue is judged by the thing that judges it in
#                   production: it goes Active, gates pods, admits them, and
#                   reports usage the quota plane can read back. Needs the
#                   quota-manager component, and needs to pull the Kueue image,
#                   so it is opt-in rather than the default.
#
#                   The version comes from .kueue-crd-version -- the version the
#                   FLEET runs -- not from the sigs.k8s.io/kueue pin in go.mod,
#                   which is held back by the Kubernetes and controller-runtime
#                   versions this repo is on. Testing against the version we
#                   compile with would exercise the one combination production
#                   never runs.
#
# Environment:
#   OME_KWOK_CLUSTER   kind cluster name        (default: ome-kwok)
#   OME_IMAGE_TAG      tag for the built images (default: kind-local)
#   KIND_NODE_IMAGE    kind node image          (default below)
#   CERT_MANAGER_VERSION
#   KWOK_CHART_VERSION
#   KUEUE_IMAGE        Kueue controller image   (default: the release image at
#                      the version in .kueue-crd-version)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${OME_KWOK_CLUSTER:-ome-kwok}"
BLOCK=""
IP_PREFIX=""
TAG="${OME_IMAGE_TAG:-kind-local}"
# 1.35, not the 1.34 a generic kind recipe would pick: charts/ome-scheduler
# guards kubeVersion ">=1.35.0-0 <1.36.0-0" and refuses to render otherwise. The
# scheduler is the upstream kube-scheduler built as a library, so its plugin API
# is pinned to one minor (scheduler/go.mod tracks k8s 1.35.4) and the node has to
# match. --no-scheduler frees you to run any version.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-$(tr -d '[:space:]' <.kind-node-image)}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
KWOK_CHART_VERSION="${KWOK_CHART_VERSION:-0.3.0}"
CUBES="4x4x4,2x2x2"
MODEL="demo-noop"
SIM_NS="kwok-sim"
OME_NS="ome"
# The node label that identifies one TPU slice. In the reference TPU cluster a
# node pool is exactly one slice, and unlike
# cloud.google.com/gke-tpu-partition-<topo>-id the key is the same for every
# carve — so it works as one fleet-wide setting.
TOPOLOGY_KEY="cloud.google.com/gke-nodepool"
SKIP_BUILD=0
NO_SCHEDULER=0
NO_NODES=0
MC_ROLE=""
WITH_ISVCS=0
WITH_KUEUE=0
WITH_ROUTING=0
TOPOLOGY_MODE=""
ALL_COMPONENTS="manager scheduler quota-manager"
REQUESTED=""

while [ $# -gt 0 ]; do
  # A value-taking flag left last on the line leaves $2 unset, which `set -u`
  # reports as "$2: unbound variable" — a bash internal, from a line number, for
  # what is an ordinary usage mistake.
  case "$1" in
    --cluster|--cubes|--component|--topology|--block|--ip-prefix|--multicluster-role)
      [ $# -ge 2 ] || { echo "$1 needs a value" >&2; exit 2; } ;;
  esac
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    --cubes) CUBES="$2"; shift 2 ;;
    # Passed through to the node generator. Both are per-cluster salts, and they
    # only matter once more than one simulated cluster shares a docker bridge:
    # the generator derives node names from a hash of the block and hands out
    # addresses from the prefix, so two clusters left at the defaults mint
    # identically named nodes on identically numbered addresses.
    --block) BLOCK="$2"; shift 2 ;;
    --ip-prefix) IP_PREFIX="$2"; shift 2 ;;
    --component) REQUESTED="${REQUESTED} $2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --no-scheduler) NO_SCHEDULER=1; shift ;;
    --no-nodes) NO_NODES=1; shift ;;
    --multicluster-role) MC_ROLE="$2"; shift 2 ;;
    --topology) TOPOLOGY_MODE="$2"; shift 2 ;;
    --with-isvcs) WITH_ISVCS=1; shift ;;
    --with-kueue) WITH_KUEUE=1; shift ;;
    --with-routing) WITH_ROUTING=1; shift ;;
    -h|--help) sed -n '2,/^set -/ { /^set -/d; p; }' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Membership over a space-separated list rather than an array: bash 3.2 is still
# the default on macOS, and expanding an empty array under `set -u` is an error
# there.
in_list() { case " $2 " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

for c in $REQUESTED; do
  in_list "$c" "$ALL_COMPONENTS" || {
    echo "unknown component: $c (want one of: $ALL_COMPONENTS)" >&2; exit 2; }
done

if [ "$NO_SCHEDULER" -eq 1 ]; then
  in_list scheduler "$REQUESTED" && {
    echo "--component scheduler contradicts --no-scheduler" >&2; exit 2; }
fi

# --no-scheduler subtracts from the default set rather than being a fourth
# spelling of --component, so the two stay composable.
COMPONENTS=""
for c in ${REQUESTED:-$ALL_COMPONENTS}; do
  if [ "$c" = scheduler ] && [ "$NO_SCHEDULER" -eq 1 ]; then
    continue
  fi
  COMPONENTS="${COMPONENTS:+$COMPONENTS }${c}"
done

WITH_MANAGER=0
WITH_SCHEDULER=0
# Kueue is installed for the quota plane's benefit; without it nothing here
# reads a ClusterQueue and the install is pure cost.
if [ "$WITH_KUEUE" -eq 1 ]; then
  in_list quota-manager "$COMPONENTS" || {
    echo "FATAL: --with-kueue needs the quota-manager component" >&2; exit 1; }
fi

WITH_QUOTA=0
in_list manager "$COMPONENTS" && WITH_MANAGER=1
in_list scheduler "$COMPONENTS" && WITH_SCHEDULER=1
in_list quota-manager "$COMPONENTS" && WITH_QUOTA=1

if [ "$WITH_ISVCS" -eq 1 ] && [ "$WITH_MANAGER" -eq 0 ]; then
  echo "--with-isvcs needs the manager component; nothing else reconciles an ISVC" >&2
  exit 2
fi

if [ -n "$MC_ROLE" ]; then
  # The same value main.go accepts, checked here so a typo costs a second rather
  # than a cluster: the manager exits on a bad role, but not until it boots.
  [ "$MC_ROLE" = control-plane ] || {
    echo "--multicluster-role must be control-plane, got: $MC_ROLE" >&2; exit 2; }
  [ "$WITH_MANAGER" -eq 1 ] || {
    echo "--multicluster-role needs the manager component; nothing else reads it" >&2; exit 2; }
fi

if [ "$WITH_ROUTING" -eq 1 ] && [ "$MC_ROLE" != control-plane ]; then
  echo "--with-routing needs --multicluster-role control-plane" >&2
  exit 2
fi

if [ "$NO_NODES" -eq 1 ] && [ "$WITH_ISVCS" -eq 1 ]; then
  echo "--with-isvcs has nowhere to place a pod with --no-nodes" >&2
  exit 2
fi

# plugin mode has no meaning without the plugin, and podgroup's override only
# exists to work around it — so the default scheduler always runs strict.
if [ -z "$TOPOLOGY_MODE" ]; then
  [ "$WITH_SCHEDULER" -eq 1 ] && TOPOLOGY_MODE=podgroup || TOPOLOGY_MODE=strict
fi

# kind is not in this list: resolve_kind below accepts bin/kind, which is where
# `make kind` puts it, and requiring it on PATH as well would reject the setup
# this repo actually asks for.
for tool in docker helm kubectl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool not on PATH" >&2; exit 1; }
done

. "$(dirname "${BASH_SOURCE[0]}")/lib/kind.sh"
. "$(dirname "${BASH_SOURCE[0]}")/lib/fixtures.sh"
resolve_kind

KCTX="kind-${CLUSTER}"
IMAGE="ome-manager:${TAG}"
SCHED_IMAGE="ome-scheduler:${TAG}"
QUOTA_IMAGE="ome-quota-manager:${TAG}"
k() { kubectl --context "$KCTX" "$@"; }

# Client-side merging preserves cert-manager's webhook caBundle. Helm 4 needs
# an explicit flag; Helm 3 already uses that apply mode.
HELM_APPLY_ARGS=()
helm version --short 2>/dev/null | grep -q '^v3' || HELM_APPLY_ARGS=(--server-side=false)

echo "==> components: ${COMPONENTS}"

echo "==> kind cluster ${CLUSTER}"
# kind copies the shell's proxy variables into the node container. A proxy on
# localhost is the host's loopback, not the node's, so every in-cluster image
# pull dies with "proxyconnect ... connection refused". Clear them for the
# create; the node reaches registries directly. Leave them set for everything
# after this, which runs on the host and may need the proxy to reach the
# internet at all. Set KWOK_SIM_KEEP_PROXY=1 where the node genuinely needs one.
kind_create() {
  if [ -n "${KWOK_SIM_KEEP_PROXY:-}" ]; then
    "$KIND" create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE"
  else
    env -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u http_proxy -u https_proxy -u no_proxy \
      "$KIND" create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE"
  fi
}
"$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind_create

if [ "$NO_NODES" -eq 1 ]; then
  echo "==> no kwok, no simulated nodes"
else
echo "==> kwok"
helm repo add kwok https://kwok.sigs.k8s.io/charts/ >/dev/null 2>&1 || true
helm repo update kwok >/dev/null
# hostNetwork is what the chart recommends on kind. It also means the controller
# reports the kind node's IP as its own, which is why every fake node must ship
# an explicit InternalIP — see gen-tpu-nodes.sh.
helm upgrade --install kwok kwok/kwok --version "$KWOK_CHART_VERSION" \
  --kube-context "$KCTX" --namespace kube-system --set hostNetwork=true --wait --timeout 180s >/dev/null
# Distinct release name: the upstream recipe installs both charts as "kwok",
# which collides in one namespace. Without the Stages nothing ever goes Ready.
helm upgrade --install kwok-stages kwok/stage-fast --version "$KWOK_CHART_VERSION" \
  --kube-context "$KCTX" --namespace kube-system --wait --timeout 120s >/dev/null
# stage-fast ships pod-complete with no delay at all, so a Job's pod succeeds
# the moment it is Running and gives back whatever it held. Nothing that holds
# a resource for a measurable interval can be simulated that way -- a quota
# tenant's usage appears and is gone between two reads. Teaching the stage to
# read a per-pod delay annotation makes the duration the pod's business, with
# 30s for one that asks for nothing. The stage selects Job-owned pods only, so
# this reaches no ISVC. Patched rather than applied: the chart owns the rest of
# the object, and a partial apply over it would depend on merge semantics that
# change once kubectl has written a last-applied annotation.
kubectl --context "$KCTX" patch stage.kwok.x-k8s.io pod-complete --type=merge -p \
  '{"spec":{"delay":{"durationMilliseconds":30000,"durationFrom":{"expressionFrom":".metadata.annotations[\"pod-complete.stage.kwok.x-k8s.io/delay\"]"}}}}' >/dev/null
# stage-fast never recomputes Ready after start, so a pod OME drains through its
# ome.io/serving gate would stay a ready endpoint and the drain would stall.
kubectl --context "$KCTX" apply -f hack/kwok-sim/readiness-gate-stages.yaml >/dev/null

echo "==> simulated TPU nodes (${CUBES})"
KWOK_SIM_CONTEXT="$KCTX" hack/kwok-sim/apply-tpu-nodes.sh --context "$KCTX" \
  -- --cubes "$CUBES" --model "$MODEL" \
  ${BLOCK:+--block "$BLOCK"} ${IP_PREFIX:+--ip-prefix "$IP_PREFIX"}
fi

# Before OME, so the manager registers the scheme at startup rather than needing
# a restart: it probes for optional CRDs once, on boot. Without PodGroup, OME
# still runs but stamps GangSchedulingUnavailable=True on every multi-pod
# Component, and ome-scheduler has nothing to gang on.
#
# There is no committed copy of this CRD: it is vendored out of the
# sigs.k8s.io/scheduler-plugins module already in go.mod, into a gitignored
# directory. `make scheduler-plugins-crds` is that copy, and it reads the module
# cache rather than the network, so regenerating is cheap and offline.
if [ "$WITH_MANAGER" -eq 1 ] || [ "$WITH_SCHEDULER" -eq 1 ]; then
  echo "==> scheduler-plugins PodGroup CRD"
  PODGROUP_CRD=tests/integration/dep-crds/scheduler-plugins/scheduling.x-k8s.io_podgroups.yaml
  [ -f "$PODGROUP_CRD" ] || make scheduler-plugins-crds >/dev/null
  if [ -f "$PODGROUP_CRD" ]; then
    k apply -f "$PODGROUP_CRD" >/dev/null
  elif [ "$WITH_SCHEDULER" -eq 1 ]; then
    echo "FATAL: no PodGroup CRD and ome-scheduler requires it; run 'go mod download' then retry" >&2
    exit 1
  else
    echo "    unavailable — OME will report GangSchedulingUnavailable"
  fi
fi

# The routing publisher watches HTTPRoutes from startup. Vendor the CRD from
# the pinned Gateway API module before enabling that controller.
if [ "$WITH_ROUTING" -eq 1 ]; then
  echo "==> Gateway API HTTPRoute CRD"
  make gateway-api-crds >/dev/null
  k apply --server-side -f tests/integration/dep-crds/gateway-api/gateway.networking.k8s.io_httproutes.yaml >/dev/null
  k wait --for=condition=Established crd/httproutes.gateway.networking.k8s.io --timeout=60s >/dev/null
fi

# ome-quota-manager reads Kueue's ResourceFlavors to decide which hardware class
# a node's accelerators belong to; without them every chip comes back
# unattributed and the reserved root reports no capacity at all. Only the CRDs
# are installed, not Kueue itself: nothing here admits a workload. Vendored by
# `make kueue-crds` from .kueue-crd-version, into the same gitignored directory
# as PodGroup.
KUEUE_VERSION="$(tr -d '[:space:]' < .kueue-crd-version)"

if [ "$WITH_QUOTA" -eq 1 ] && [ "$WITH_KUEUE" -eq 1 ]; then
  echo "==> Kueue ${KUEUE_VERSION} (controller, webhooks and CRDs)"

  # The chart ships inside the module, so this needs no chart repo and works
  # offline once the module is in the cache. Sourced at KUEUE_VERSION rather
  # than at the go.mod pin: the pin is what OME COMPILES against and lags the
  # fleet deliberately, and installing it here would test the one pairing
  # production never runs.
  KUEUE_CHART="$(go list -m -mod=mod -f '{{.Dir}}' "sigs.k8s.io/kueue@${KUEUE_VERSION}" 2>/dev/null)/charts/kueue"
  [ -d "$KUEUE_CHART" ] || {
    echo "FATAL: no Kueue chart for ${KUEUE_VERSION}; run 'go mod download sigs.k8s.io/kueue@${KUEUE_VERSION}' then retry" >&2
    exit 1; }

  # The node has no egress, so the image is pulled on the host and side-loaded.
  KUEUE_IMAGE="${KUEUE_IMAGE:-registry.k8s.io/kueue/kueue:${KUEUE_VERSION}}"
  KUEUE_TAR="$(mktemp -t kueue-image-XXXXXX.tar)"

  # Taken from the daemon rather than assumed. The kind node runs the host's
  # architecture, so an amd64 image is useless on an arm64 laptop -- and asking
  # to export a platform the local image does not have fails with "no suitable
  # export target found", which reads like a registry problem rather than an
  # architecture one.
  KUEUE_ARCH="$(docker version --format '{{.Server.Arch}}')"
  [ -n "$KUEUE_ARCH" ] || { echo "FATAL: cannot determine the docker daemon's architecture" >&2; exit 1; }

  # Matched on architecture, not merely on presence: a ref already pulled for
  # the wrong platform would otherwise skip the pull and fail at the export.
  if [ "$(docker image inspect -f '{{.Architecture}}' "$KUEUE_IMAGE" 2>/dev/null)" != "$KUEUE_ARCH" ]; then
    echo "    pulling ${KUEUE_IMAGE} (${KUEUE_ARCH})"
    docker pull --platform "linux/${KUEUE_ARCH}" "$KUEUE_IMAGE" >/dev/null || {
      echo "FATAL: cannot pull ${KUEUE_IMAGE} for linux/${KUEUE_ARCH}. Set KUEUE_IMAGE to a" >&2
      echo "       reachable ref, or pull it by hand and re-run; the kind node has no egress" >&2
      echo "       of its own." >&2
      exit 1; }
  fi

  # Saved platform-scoped and loaded as an archive, unlike the images this
  # script builds. A pulled multi-arch ref carries attestation manifests, and
  # `kind load docker-image` fails importing them with "content digest not
  # found" -- the digest it wants is an attestation, not a layer.
  docker save --platform "linux/${KUEUE_ARCH}" "$KUEUE_IMAGE" -o "$KUEUE_TAR"
  "$KIND" load image-archive "$KUEUE_TAR" --name "$CLUSTER" >/dev/null
  rm -f "$KUEUE_TAR"

  # Installed with `helm upgrade --install`, the same as every other component
  # here. Helm keeps its state in a release secret rather than in a
  # last-applied-configuration annotation, so Kueue's Workload CRD -- far past
  # the 256KiB a client-side kubectl apply has to fit into -- installs without
  # any special handling.
  helm upgrade --install kueue "$KUEUE_CHART" \
    --kube-context "$KCTX" \
    --namespace kueue-system --create-namespace \
    --set "controllerManager.manager.image.repository=${KUEUE_IMAGE%:*}" \
    --set "controllerManager.manager.image.tag=${KUEUE_IMAGE##*:}" \
    --set controllerManager.manager.image.pullPolicy=Never \
    --set enableCertManager=false \
    "${HELM_APPLY_ARGS[@]+"${HELM_APPLY_ARGS[@]}"}" --wait --timeout 300s >/dev/null

  # A ready pod is not a serving webhook, and --wait only knows about the pod.
  # Kueue generates its own certificate in process and its webhooks are
  # failurePolicy=Fail, so until the listener accepts TLS every ResourceFlavor
  # and ClusterQueue write -- the quota plane's included -- is rejected with
  # "connection refused".
  #
  # Probed by attempting a write rather than by watching endpoints: the pod
  # passes its readiness probe before the webhook server has bound, so a ready
  # EndpointSlice returns while the very next apply still fails. A server-side
  # dry-run runs the admission chain without creating anything, which is the
  # same question the next step asks.
  echo "==> wait for the Kueue webhook to admit"
  KUEUE_PROBE='{"apiVersion":"kueue.x-k8s.io/v1beta2","kind":"ResourceFlavor","metadata":{"name":"kwok-sim-webhook-probe"}}'
  for _ in $(seq 90); do
    printf '%s' "$KUEUE_PROBE" | k apply --dry-run=server -f - >/dev/null 2>&1 && break
    sleep 2
  done
  printf '%s' "$KUEUE_PROBE" | k apply --dry-run=server -f - >/dev/null 2>&1 || {
    echo "FATAL: Kueue's webhook never began admitting; check" >&2
    echo "       'kubectl -n kueue-system logs deploy/kueue-controller-manager'" >&2
    exit 1; }

elif [ "$WITH_QUOTA" -eq 1 ]; then
  echo "==> Kueue CRDs"
  KUEUE_CRDS=tests/integration/dep-crds/kueue
  # Probed by file, not by directory: `make kueue-crds` mkdir -p's before it
  # looks the module up, so an empty directory is what a missing module leaves
  # behind. ResourceFlavor is the one the quota plane cannot work without.
  [ -f "$KUEUE_CRDS/kueue.x-k8s.io_resourceflavors.yaml" ] || make kueue-crds >/dev/null
  [ -f "$KUEUE_CRDS/kueue.x-k8s.io_resourceflavors.yaml" ] || {
    echo "FATAL: no Kueue CRDs and ome-quota-manager requires them; run 'go mod download' then retry" >&2
    exit 1; }
  # Server-side: Kueue's Workload schema is far past the 256KiB a client-side
  # apply has to fit into the last-applied-configuration annotation.
  k apply --server-side --force-conflicts -f "$KUEUE_CRDS" >/dev/null

fi

# Outside the branches above, because the flavor belongs to the quota plane
# rather than to whichever of them installed the Kueue types: with --with-kueue
# it comes from the chart, without it from dep-crds, and either way the flavor
# is the same requirement.
#
# Before the manager starts, not after it. The controller reconciles on
# AcceleratorQuota and Node events, and creating a ResourceFlavor is neither —
# so a flavor applied afterwards stays invisible until the next node event or
# the resync tick, leaving a fresh cluster reporting no capacity for minutes.
# Applying it here means it is already there for the first reconcile.
#
# No AcceleratorQuota is applied, here or anywhere: a tree is what an operator
# is here to write, so it is left alone exactly as the ISVCs are, and the
# controller creates the reserved root itself. The flavor is not a tree — it
# describes the hardware, which this simulator owns, and without it every fake
# chip is unattributed and the root reports nothing, which reads as a broken
# component rather than an empty fleet.
if [ "$WITH_QUOTA" -eq 1 ]; then
  echo "==> ResourceFlavor (no AcceleratorQuota; the controller makes its own root)"
  k apply -f hack/kwok-sim/fixtures/30-resourceflavor.yaml >/dev/null
  # The namespaces this install enrolls, so any quota fixture applied by hand
  # fully materializes. None is created by the tree: a leaf is a tenant, and a
  # tenant is not a namespace.
  ensure_namespaces "$KCTX"
fi

if [ "$WITH_MANAGER" -eq 1 ]; then
  echo "==> cert-manager ${CERT_MANAGER_VERSION}"
  k apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" >/dev/null
  k -n cert-manager wait --for=condition=Available deployment --all --timeout=300s >/dev/null
fi

if [ "$SKIP_BUILD" -eq 0 ]; then
  if [ "$WITH_MANAGER" -eq 1 ]; then
    echo "==> build ${IMAGE}"
    docker build -f dockerfiles/manager.Dockerfile -t "$IMAGE" .
  fi
  if [ "$WITH_SCHEDULER" -eq 1 ]; then
    echo "==> build ${SCHED_IMAGE}"
    docker build -f dockerfiles/ome-scheduler.Dockerfile -t "$SCHED_IMAGE" .
  fi
  if [ "$WITH_QUOTA" -eq 1 ]; then
    echo "==> build ${QUOTA_IMAGE}"
    docker build -f dockerfiles/ome-quota-manager.Dockerfile -t "$QUOTA_IMAGE" .
  fi
fi
if [ "$WITH_MANAGER" -eq 1 ]; then
  echo "==> load ${IMAGE}"
  "$KIND" load docker-image "$IMAGE" --name "$CLUSTER"
fi
if [ "$WITH_SCHEDULER" -eq 1 ]; then
  echo "==> load ${SCHED_IMAGE}"
  "$KIND" load docker-image "$SCHED_IMAGE" --name "$CLUSTER"
fi
if [ "$WITH_QUOTA" -eq 1 ]; then
  echo "==> load ${QUOTA_IMAGE}"
  "$KIND" load docker-image "$QUOTA_IMAGE" --name "$CLUSTER"
fi

# A rebuild reuses the tag, so the Deployment's image ref is byte-identical and
# nothing rolls: helm reports success, `kind load` reports success, and the pod
# keeps serving the previous binary. Stamping the image ID into a pod annotation
# is what makes a rebuild visible to the Deployment — and only a real one, so an
# unchanged image still rolls nothing.
image_id() { docker image inspect -f '{{.Id}}' "$1" | cut -d: -f2 | cut -c1-16; }

# Unconditional, whatever --component selected. ome-crd is the single owner of
# every OME CustomResourceDefinition here, including AcceleratorQuota, which
# charts/ome-quota-manager can also install: two charts claiming one
# cluster-scoped object fails whichever release claims it second, and which one
# that is would depend on the order of past runs.
echo "==> OME CRDs"
helm upgrade --install ome-crd charts/ome-crd/ --kube-context "$KCTX" \
  --namespace "$OME_NS" --create-namespace --wait --timeout 180s >/dev/null

if [ "$WITH_MANAGER" -eq 1 ]; then
  # global.hub="" renders a bare ome-manager:<tag> so the kind-loaded image is the
  # one that runs. The release namespace must be `ome`: the ConfigMap name, the
  # webhooks' inject-ca-from, and the manager's leader-election namespace all
  # assume it. Chart defaults (3 replicas x 2 CPU / 4Gi) do not fit one kind node.
  HELM_ARGS=(
    --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace
    --set global.hub="" --set ome.controller.tag="$TAG"
    --set modelAgent.enabled=false
    --set prometheus.enabled=false
    --set ome.multiclusterAccess.enabled=false
    --set ome.controller.replicaCount=1
    --set ome.controller.podAnnotations.kwok-sim/image-id="$(image_id "$IMAGE")"
    --set ome.controller.resources.requests.cpu=200m
    --set ome.controller.resources.requests.memory=512Mi
    --set ome.controller.resources.limits.cpu=2
    --set ome.controller.resources.limits.memory=2Gi
    # The simulated silicon. OME counts accelerators only by the names listed
    # here and the chart lists nvidia.com/gpu alone, so without this a TPU gang
    # measures zero accelerator demand and capacity placement cannot size it.
    --set 'ome.controller.acceleratorResources={google.com/tpu}'
  )
  if [ -n "$MC_ROLE" ]; then
    # Every member is installed by this script into $OME_NS, which is therefore
    # where placement finds each member's configuration and runtime pins.
    # Capacity reads the fleet report the management-mode quota manager keeps on
    # the reserved root; members refresh theirs every minute, so maxAge
    # tolerates a few missed refreshes.
    HELM_ARGS+=(
      --set ome.multicluster.enabled=true
      --set ome.multicluster.role="$MC_ROLE"
      --set ome.multicluster.config.placement.memberOperatorNamespace="$OME_NS"
      --set ome.multicluster.config.placement.capacity.rootName=root
      --set ome.multicluster.config.placement.capacity.maxAge=5m
      --set ome.multicluster.config.placement.capacity.stabilityWindow=1m
      --set ome.multicluster.config.placement.capacity.refreshInterval=10s
    )
  fi
  if [ "$WITH_ROUTING" -eq 1 ]; then
    HELM_ARGS+=(-f hack/kwok-sim/routing-values.yaml)
  fi

  # Two passes. The chart registers failurePolicy=Fail webhooks and, in the same
  # apply, a ClusterServingRuntime those webhooks intercept — which cannot succeed
  # before the controller serves and cainjector has replaced the placeholder
  # caBundle. Pass 1 is expected to report that rejection.
  echo "==> OME controller (pass 1 of 2; the runtime rejection here is expected)"
  helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" "${HELM_APPLY_ARGS[@]+"${HELM_APPLY_ARGS[@]}"}" >/dev/null 2>&1 || true
  k -n "$OME_NS" rollout status deployment/ome-controller-manager --timeout=300s

  echo "==> wait for webhook cert and endpoints"
  for _ in $(seq 90); do
    b="$(k get validatingwebhookconfiguration inferenceservice.ome.io \
          -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
    case "$b" in ""|"Cg==") sleep 2 ;; *) break ;; esac
  done
  # Ready endpoints, not merely present ones: the manager's readiness probe
  # answers on its health port before the webhook server has bound 9443, so a
  # Deployment that reports Available can still refuse admission connections.
  for _ in $(seq 90); do
    k -n "$OME_NS" get endpointslices -l kubernetes.io/service-name=ome-webhook-server-service \
      -o jsonpath='{.items[*].endpoints[?(@.conditions.ready==true)].addresses[*]}' 2>/dev/null | grep -q . && break
    sleep 2
  done

  echo "==> OME controller (pass 2 of 2)"
  # Still retried: cainjector's caBundle write and the webhook's first successful
  # TLS accept are independent events, and pass 2 needs both.
  for attempt in 1 2 3 4 5 6; do
    if helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" \
         "${HELM_APPLY_ARGS[@]+"${HELM_APPLY_ARGS[@]}"}" --wait --timeout 300s >/dev/null 2>&1; then
      break
    fi
    [ "$attempt" -lt 6 ] || { echo "FATAL: ome-resources did not converge" >&2; exit 1; }
    echo "    webhook not answering yet; retry ${attempt}"
    sleep 10
  done
fi

if [ "$WITH_SCHEDULER" -eq 1 ]; then
  # Same namespace as the manager so one `kubectl -n ome get pods` shows the
  # whole control plane. The chart is written for a real fleet: 2 replicas with
  # a required hostname anti-affinity and a PDB (unschedulable on one node), and
  # 4Gi/12Gi of memory. percentageOfNodesToScore is forced to 100 because
  # OMEGangPack builds its per-domain free-node counts from whatever upstream
  # sampled, and a partial view can rank a filled slice above an empty one.
  echo "==> ome-scheduler"
  helm upgrade --install ome-scheduler charts/ome-scheduler/ \
    --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace \
    --set global.hub="" \
    --set scheduler.image.tag="$TAG" \
    --set scheduler.replicaCount=1 \
    --set scheduler.leaderElect=false \
    --set scheduler.podDisruptionBudget.enabled=false \
    --set scheduler.podAnnotations.kwok-sim/image-id="$(image_id "$SCHED_IMAGE")" \
    --set scheduler.percentageOfNodesToScore=100 \
    --set scheduler.plugin.topologyKey="$TOPOLOGY_KEY" \
    --set scheduler.resources.requests.cpu=100m \
    --set scheduler.resources.requests.memory=256Mi \
    --set scheduler.resources.limits.cpu=1 \
    --set scheduler.resources.limits.memory=1Gi \
    "${HELM_APPLY_ARGS[@]+"${HELM_APPLY_ARGS[@]}"}" --wait --timeout 300s >/dev/null
fi

if [ "$WITH_QUOTA" -eq 1 ]; then
  # workload mode: this cluster is the one with the silicon, so the quota plane
  # derives the reserved root's capacity from the fake nodes. Management mode
  # projects per-cluster shares onto members, and there are none here.
  # crd.install stays off — ome-crd above owns the CRD.
  #
  # capacity.resources is narrowed to google.com/tpu, the only accelerator the
  # fake nodes carry. The chart also lists nvidia.com/gpu, and every listed
  # resource gets a row per flavor, so the root would report an explicit zero
  # GPU pool under the tpu7x flavor. One replica and no leader election, for the
  # same reason as ome-scheduler: it is one kind node.
  echo "==> ome-quota-manager"
  helm upgrade --install ome-quota-manager charts/ome-quota-manager/ \
    --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace \
    --set global.hub="" \
    --set quotaManager.image.tag="$TAG" \
    --set quotaManager.mode=workload \
    --set quotaManager.crd.install=false \
    --set quotaManager.replicaCount=1 \
    --set quotaManager.leaderElect=false \
    --set quotaManager.podDisruptionBudget.enabled=false \
    --set quotaManager.podAnnotations.kwok-sim/image-id="$(image_id "$QUOTA_IMAGE")" \
    --set "quotaManager.materialize.enrolledNamespaces={$(enrolled_namespaces_csv)}" \
    --set 'quotaManager.capacity.resources={google.com/tpu}' \
    "${HELM_APPLY_ARGS[@]+"${HELM_APPLY_ARGS[@]}"}" --wait --timeout 300s >/dev/null

  # The AcceleratorQuota webhook is failurePolicy=Fail, and it gates the
  # controller's own creation of the reserved root as much as any hand-written
  # tree — so until it answers, the cluster has no root and `up.sh` would hand
  # back a component that looks broken. Unlike ome-manager's there is no
  # cainjector involved: the component generates its own cert and writes its own
  # caBundle, and both happen after the pod starts.
  echo "==> wait for quota webhook cert and endpoints"
  for _ in $(seq 90); do
    b="$(k get validatingwebhookconfiguration ome-quota-manager.ome.io \
          -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
    case "$b" in ""|"Cg==") sleep 2 ;; *) break ;; esac
  done
  for _ in $(seq 90); do
    k -n "$OME_NS" get endpointslices -l kubernetes.io/service-name=ome-quota-manager-webhook \
      -o jsonpath='{.items[*].endpoints[?(@.conditions.ready==true)].addresses[*]}' 2>/dev/null | grep -q . && break
    sleep 2
  done
fi

if [ "$WITH_MANAGER" -eq 1 ]; then
  echo "==> fixtures"
  k create namespace "$SIM_NS" --dry-run=client -o yaml | k apply -f - >/dev/null
  k apply -f hack/kwok-sim/fixtures/00-model.yaml >/dev/null
  echo "    runtimes: topology=${TOPOLOGY_MODE}$([ "$WITH_SCHEDULER" -eq 1 ] || echo ' (default scheduler)')"
  RENDER_ARGS=(--key "$TOPOLOGY_KEY")
  [ "$WITH_SCHEDULER" -eq 1 ] || RENDER_ARGS+=(--no-scheduler)
  hack/kwok-sim/render-runtimes.sh "$TOPOLOGY_MODE" "${RENDER_ARGS[@]}" | k apply -f - >/dev/null
  if [ "$TOPOLOGY_MODE" = "strict" ] && [ "$WITH_SCHEDULER" -eq 1 ]; then
    echo "    NOTE: strict + ome-scheduler is the known wedge; gangs will stay Pending"
  fi
fi

if [ "$WITH_ISVCS" -eq 1 ]; then
  echo "==> inference services"
  k apply -n "$SIM_NS" -f hack/kwok-sim/fixtures/20-isvc-2x2x2.yaml \
                       -f hack/kwok-sim/fixtures/21-isvc-4x4x4.yaml >/dev/null
  # Wait on pods, not on the ISVC: top-level Ready flips True immediately even
  # with the whole gang Pending, so waiting on it proves nothing.
  echo "    waiting for engine pods to be Running"
  for _ in $(seq 60); do
    pending="$(k -n "$SIM_NS" get pods --no-headers 2>/dev/null | grep -cv ' Running ' || true)"
    total="$(k -n "$SIM_NS" get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    [ "$total" -gt 0 ] && [ "$pending" -eq 0 ] && break
    sleep 3
  done
  echo
  hack/kwok-sim/status.sh
  exit 0
fi

echo
echo "Ready. Context ${KCTX}, fixtures namespace ${SIM_NS}."
echo
if [ "$WITH_MANAGER" -eq 1 ]; then
  cat <<EOF
  kubectl apply -n ${SIM_NS} -f hack/kwok-sim/fixtures/20-isvc-2x2x2.yaml
  kubectl apply -n ${SIM_NS} -f hack/kwok-sim/fixtures/21-isvc-4x4x4.yaml

Or re-run with --with-isvcs to do that automatically.

EOF
fi
if [ "$WITH_QUOTA" -eq 1 ]; then
  cat <<EOF
  kubectl get acceleratorquota root -o jsonpath='{.status.capacity}' | jq .
  kubectl apply -f hack/kwok-sim/fixtures/31-acceleratorquota.yaml

The root is created by the controller and carries this cluster's capacity. No
tree is applied; that fixture is one if you want a starting point.

EOF
fi
cat <<EOF
  hack/kwok-sim/status.sh

EOF
