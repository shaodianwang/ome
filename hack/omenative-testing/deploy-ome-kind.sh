#!/usr/bin/env bash
# Install OME into a KIND cluster from the current working tree, then apply the
# eval's runtime fixtures.
#
# The eval drives a DEPLOYED operator, not an in-process manager: its
# expect=reject cases assert that the validating webhook denies an apply, which
# only a real install with wired certs can exercise. That is the difference
# between this and the Go KIND suites, which run the manager in-process.
#
# Usage:
#   hack/omenative-testing/deploy-ome-kind.sh [--cluster NAME] [--skip-build]
#
# Environment:
#   OME_KIND_CLUSTER   KIND cluster name           (default: ome-eval-kind)
#   OME_TEST_NS        namespace for the fixtures  (default: sandbox)
#   OME_NAMESPACE      namespace OME is installed into (default: ome)
#   OME_IMAGE_TAG      tag for the built image     (default: kind-local)
#   CERT_MANAGER_VERSION  cert-manager release to install (default below)
#
# The image is built locally and loaded into the cluster's nodes, never pushed.
# global.hub is set empty so the chart renders a bare `ome-manager:<tag>` that
# resolves to the loaded image; the tag must not be `latest`, or the kubelet's
# default pull policy would try to fetch it from a registry.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${OME_KIND_CLUSTER:-ome-eval-kind}"
NS="${OME_TEST_NS:-sandbox}"
OME_NS="${OME_NAMESPACE:-ome}"
TAG="${OME_IMAGE_TAG:-kind-local}"
# The controller chart renders a cert-manager Certificate + Issuer for the
# webhook serving cert and offers no alternative, so cert-manager is a hard
# prerequisite rather than an option. Pinned, and overridable.
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
SKIP_BUILD=0

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for tool in docker kind helm kubectl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool not on PATH" >&2; exit 1; }
done
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || {
  echo "FATAL: no KIND cluster named $CLUSTER (create it first)" >&2; exit 1; }

KCTX="kind-${CLUSTER}"
IMAGE="ome-manager:${TAG}"

if [ "$SKIP_BUILD" -eq 0 ]; then
  echo "--- build ${IMAGE} ---"
  docker build -f dockerfiles/manager.Dockerfile -t "$IMAGE" .
fi

echo "--- load ${IMAGE} into ${CLUSTER} ---"
kind load docker-image "$IMAGE" --name "$CLUSTER"

# Create both namespaces up front so nothing downstream depends on ordering.
# The eval's cases land in $NS, but runtime/pvc-and-seed.yaml pins its own
# namespace, so that one is created too rather than silently failing later.
FIXTURE_NS="$(grep -m1 -E '^\s+namespace:' hack/omenative-testing/perm/runtime/pvc-and-seed.yaml | awk '{print $2}')"
echo "--- create namespaces (${NS}, ${FIXTURE_NS}) ---"
for n in "$NS" "$FIXTURE_NS"; do
  [ -n "$n" ] || continue
  kubectl --context "$KCTX" create namespace "$n" --dry-run=client -o yaml \
    | kubectl --context "$KCTX" apply -f -
done

echo "--- install cert-manager ${CERT_MANAGER_VERSION} ---"
kubectl --context "$KCTX" apply -f \
  "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
kubectl --context "$KCTX" -n cert-manager wait --for=condition=Available \
  deployment --all --timeout=300s

echo "--- install CRDs ---"
helm upgrade --install ome-crd charts/ome-crd/ \
  --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace \
  --wait --timeout 180s

echo "--- install controller ---"
# hub empty => `ome-manager:<tag>`, matching the loaded image exactly.
# model-agent is an externally-published image this cluster cannot pull, and the
# eval does not exercise it: the fixtures back their ClusterBaseModel with a PVC.
# The chart defaults to 3 controller replicas at 2 CPU / 4Gi requests each,
# which cannot schedule on a single-node KIND cluster: the rollout stalls part
# way and never reports complete. One right-sized replica is what the eval
# needs anyway — the harness drives one reconcile loop at a time.
HELM_ARGS=(
  --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace
  --set global.hub=""
  --set ome.controller.tag="$TAG"
  --set modelAgent.enabled=false
  --set ome.controller.replicaCount=1
  --set ome.controller.resources.requests.cpu=500m
  --set ome.controller.resources.requests.memory=512Mi
  --set ome.controller.resources.limits.cpu=2
  --set ome.controller.resources.limits.memory=2Gi
)

# Two passes, because the chart both registers webhooks with failurePolicy=Fail
# and ships a ClusterServingRuntime those webhooks intercept. Helm submits the
# Deployment and that resource in a single pass, so on a fresh cluster the
# resource is rejected while the controller is not yet serving and the caBundle
# is still the chart's placeholder. The first pass lands the Deployment, webhook
# configuration and Certificate; the second converges once the webhook answers.
echo "    pass 1: land the deployment and webhook plumbing"
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" --wait=false || true

echo "--- wait for the controller to become available ---"
kubectl --context "$KCTX" -n "$OME_NS" rollout status \
  deployment/ome-controller-manager --timeout=300s

# cert-manager's cainjector rewrites the placeholder caBundle once the
# Certificate is issued. Until it does, the apiserver cannot parse the bundle
# and every intercepted create fails.
echo "--- wait for the webhook caBundle to be injected ---"
for _ in $(seq 60); do
  bundle="$(kubectl --context "$KCTX" get validatingwebhookconfiguration \
    inferenceservice.ome.io \
    -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
  case "$bundle" in
    ""|"Cg==") sleep 2 ;;
    *) echo "caBundle injected"; break ;;
  esac
done

# A Deployment reporting Available only means its pods pass readiness; the
# apiserver can still fail an admission call until the webhook Service has
# endpoints. The eval's very first action is an apply, and expect=reject cases
# depend on the webhook actually answering, so gate on endpoints rather than
# racing the first case.
echo "--- wait for webhook endpoints ---"
for _ in $(seq 60); do
  if kubectl --context "$KCTX" -n "$OME_NS" get endpointslices \
       -l kubernetes.io/service-name=ome-webhook-server-service \
       -o jsonpath='{.items[*].endpoints[*].addresses[*]}' 2>/dev/null | grep -q .; then
    echo "webhook endpoints ready"
    break
  fi
  sleep 2
done

echo "--- install controller (pass 2: converge) ---"
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" --wait --timeout 300s

echo "--- apply eval runtime fixtures into ${NS} ---"
kubectl --context "$KCTX" -n "$NS" apply -f hack/omenative-testing/perm/runtime/

echo "--- OME deployed to ${CLUSTER}; run the eval with: ---"
echo "    OME_TEST_KUBECONFIG=\$(kind get kubeconfig --name ${CLUSTER} > /tmp/kc && echo /tmp/kc) \\"
echo "      python3 hack/omenative-testing/perm/batch.py --conc 4 p01-p05"
