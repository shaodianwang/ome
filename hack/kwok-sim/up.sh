#!/usr/bin/env bash
# Stand up a kind cluster running OME against kwok-simulated TPU nodes.
#
# Usage:
#   hack/kwok-sim/up.sh [--skip-build] [--cluster NAME] [--cubes 4x4x4,2x2x2]
#                       [--no-scheduler] [--with-isvcs]
#
#   --no-scheduler  skip ome-scheduler and use the default scheduler; implies
#                   --topology strict, the full production contract.
#   --topology M    podgroup (default) | plugin | strict — where slice
#                   co-location comes from. See render-runtimes.sh, and the
#                   README on why strict + ome-scheduler wedges.
#   --with-isvcs    also apply fixtures/2*-isvc-*.yaml and wait for them.
#
# Environment:
#   OME_KWOK_CLUSTER   kind cluster name        (default: ome-kwok)
#   OME_IMAGE_TAG      tag for the built images (default: kind-local)
#   KIND_NODE_IMAGE    kind node image          (default below)
#   CERT_MANAGER_VERSION
#   KWOK_CHART_VERSION
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${OME_KWOK_CLUSTER:-ome-kwok}"
TAG="${OME_IMAGE_TAG:-kind-local}"
# 1.35, not the 1.34 a generic kind recipe would pick: charts/ome-scheduler
# guards kubeVersion ">=1.35.0-0 <1.36.0-0" and refuses to render otherwise. The
# scheduler is the upstream kube-scheduler built as a library, so its plugin API
# is pinned to one minor (scheduler/go.mod tracks k8s 1.35.4) and the node has to
# match. --no-scheduler frees you to run any version.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.35.1}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
KWOK_CHART_VERSION="${KWOK_CHART_VERSION:-0.3.0}"
CUBES="4x4x4,2x2x2"
MODEL="demo-noop"
SIM_NS="kwok-sim"
OME_NS="ome"
# The node label that identifies one TPU slice. In rc-1 a node pool is exactly
# one slice, and unlike cloud.google.com/gke-tpu-partition-<topo>-id the key is
# the same for every carve — so it works as one fleet-wide setting.
TOPOLOGY_KEY="cloud.google.com/gke-nodepool"
SKIP_BUILD=0
WITH_SCHEDULER=1
WITH_ISVCS=0
TOPOLOGY_MODE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    --cubes) CUBES="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --no-scheduler) WITH_SCHEDULER=0; shift ;;
    --topology) TOPOLOGY_MODE="$2"; shift 2 ;;
    --with-isvcs) WITH_ISVCS=1; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# plugin mode has no meaning without the plugin, and podgroup's override only
# exists to work around it — so the default scheduler always runs strict.
if [ -z "$TOPOLOGY_MODE" ]; then
  [ "$WITH_SCHEDULER" -eq 1 ] && TOPOLOGY_MODE=podgroup || TOPOLOGY_MODE=strict
fi

for tool in docker kind helm kubectl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool not on PATH" >&2; exit 1; }
done

KCTX="kind-${CLUSTER}"
IMAGE="ome-manager:${TAG}"
SCHED_IMAGE="ome-scheduler:${TAG}"
k() { kubectl --context "$KCTX" "$@"; }

# Helm 4 applies server-side, so its placeholder caBundle collides with the one
# cert-manager's cainjector owns. Helm 3 has no such flag and needs none.
FORCE_CONFLICTS=()
helm version --short 2>/dev/null | grep -q '^v3' || FORCE_CONFLICTS=(--force-conflicts)

echo "==> kind cluster ${CLUSTER}"
# kind copies the shell's proxy variables into the node container. A proxy on
# localhost is the host's loopback, not the node's, so every in-cluster image
# pull dies with "proxyconnect ... connection refused". Clear them for the
# create; the node reaches registries directly. Leave them set for everything
# after this, which runs on the host and may need the proxy to reach the
# internet at all. Set KWOK_SIM_KEEP_PROXY=1 where the node genuinely needs one.
kind_create() {
  if [ -n "${KWOK_SIM_KEEP_PROXY:-}" ]; then
    kind create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE"
  else
    env -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u http_proxy -u https_proxy -u no_proxy \
      kind create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE"
  fi
}
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind_create

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

echo "==> simulated TPU nodes (${CUBES})"
KWOK_SIM_CONTEXT="$KCTX" hack/kwok-sim/apply-tpu-nodes.sh --context "$KCTX" \
  -- --cubes "$CUBES" --model "$MODEL"

# Before OME, so the manager registers the scheme at startup rather than needing
# a restart: it probes for optional CRDs once, on boot. Without PodGroup, OME
# still runs but stamps GangSchedulingUnavailable=True on every multi-pod
# Component, and ome-scheduler has nothing to gang on.
#
# There is no committed copy of this CRD: it is vendored out of the
# sigs.k8s.io/scheduler-plugins module already in go.mod, into a gitignored
# directory. `make scheduler-plugins-crds` is that copy, and it reads the module
# cache rather than the network, so regenerating is cheap and offline.
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

echo "==> cert-manager ${CERT_MANAGER_VERSION}"
k apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" >/dev/null
k -n cert-manager wait --for=condition=Available deployment --all --timeout=300s >/dev/null

if [ "$SKIP_BUILD" -eq 0 ]; then
  echo "==> build ${IMAGE}"
  docker build -f dockerfiles/manager.Dockerfile -t "$IMAGE" .
  if [ "$WITH_SCHEDULER" -eq 1 ]; then
    echo "==> build ${SCHED_IMAGE}"
    docker build -f dockerfiles/ome-scheduler.Dockerfile -t "$SCHED_IMAGE" .
  fi
fi
echo "==> load ${IMAGE}"
kind load docker-image "$IMAGE" --name "$CLUSTER"
if [ "$WITH_SCHEDULER" -eq 1 ]; then
  echo "==> load ${SCHED_IMAGE}"
  kind load docker-image "$SCHED_IMAGE" --name "$CLUSTER"
fi

echo "==> OME CRDs"
helm upgrade --install ome-crd charts/ome-crd/ --kube-context "$KCTX" \
  --namespace "$OME_NS" --create-namespace --wait --timeout 180s >/dev/null

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
  --set ome.controller.resources.requests.cpu=200m
  --set ome.controller.resources.requests.memory=512Mi
  --set ome.controller.resources.limits.cpu=2
  --set ome.controller.resources.limits.memory=2Gi
)

# Two passes. The chart registers failurePolicy=Fail webhooks and, in the same
# apply, a ClusterServingRuntime those webhooks intercept — which cannot succeed
# before the controller serves and cainjector has replaced the placeholder
# caBundle. Pass 1 is expected to report that rejection.
echo "==> OME controller (pass 1 of 2; the runtime rejection here is expected)"
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" "${FORCE_CONFLICTS[@]+"${FORCE_CONFLICTS[@]}"}" >/dev/null 2>&1 || true
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
       "${FORCE_CONFLICTS[@]+"${FORCE_CONFLICTS[@]}"}" --wait --timeout 300s >/dev/null 2>&1; then
    break
  fi
  [ "$attempt" -lt 6 ] || { echo "FATAL: ome-resources did not converge" >&2; exit 1; }
  echo "    webhook not answering yet; retry ${attempt}"
  sleep 10
done

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
    --set scheduler.percentageOfNodesToScore=100 \
    --set scheduler.plugin.topologyKey="$TOPOLOGY_KEY" \
    --set scheduler.resources.requests.cpu=100m \
    --set scheduler.resources.requests.memory=256Mi \
    --set scheduler.resources.limits.cpu=1 \
    --set scheduler.resources.limits.memory=1Gi \
    "${FORCE_CONFLICTS[@]+"${FORCE_CONFLICTS[@]}"}" --wait --timeout 300s >/dev/null
fi

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

cat <<EOF

Ready. Context ${KCTX}, fixtures namespace ${SIM_NS}.

  kubectl apply -n ${SIM_NS} -f hack/kwok-sim/fixtures/20-isvc-2x2x2.yaml
  kubectl apply -n ${SIM_NS} -f hack/kwok-sim/fixtures/21-isvc-4x4x4.yaml
  hack/kwok-sim/status.sh

Or re-run with --with-isvcs to do that automatically.

EOF
