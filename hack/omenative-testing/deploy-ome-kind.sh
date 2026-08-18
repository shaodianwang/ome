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
#   OME_IMAGE_TAG      local build/lookup tag      (default: kind-local)
#   CERT_MANAGER_VERSION  cert-manager release to install (default below)
#
# Images are built locally and loaded into the cluster's nodes, never pushed.
# global.hub is set empty so the chart renders bare `ome-manager:<tag>` /
# `ome-agent:<tag>` references that resolve to the loaded images. The deploy tag
# is derived from both image IDs: a reused cluster therefore gets a new
# controller pod template when either local image changes. The tag is never
# `latest`, so the kubelet does not try to fetch it from a registry.
#
# ome-agent is required, not optional: the ClusterBaseModel is PVC-backed, and
# the PVC path always runs a metadata-extraction Job built from the omeAgent
# image to validate the model. There is no annotation or flag that skips it, so
# the model never leaves In_Transit without a pullable agent image.
sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    echo "FATAL: sha256sum or shasum is required to derive the deploy tag" >&2
    return 1
  fi
}

deploy_tag_from_image_ids() {
  local manager_id="$1" agent_id="$2" digest
  digest="$(printf '%s\n%s\n' "$manager_id" "$agent_id" | sha256_stdin)"
  printf 'kind-%s\n' "$digest"
}

# Keep the content-derived tag helper directly testable without executing a
# cluster deployment when this file is sourced.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${OME_KIND_CLUSTER:-ome-eval-kind}"
NS="${OME_TEST_NS:-sandbox}"
OME_NS="${OME_NAMESPACE:-ome}"
BUILD_TAG="${OME_IMAGE_TAG:-kind-local}"
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
MANAGER_BUILD_IMAGE="ome-manager:${BUILD_TAG}"
AGENT_BUILD_IMAGE="ome-agent:${BUILD_TAG}"
RENDER="hack/omenative-testing/perm/runtime_fixtures.py"

# `kind load docker-image` reads the image through `docker save`, which under
# Docker's containerd image store emits a multi-platform index whose other
# platforms' blobs are absent — containerd then rejects the import with
# "content digest ... not found". Saving one explicit platform avoids it.
NODE_PLATFORM="linux/$(docker info --format '{{.Architecture}}' 2>/dev/null \
  | sed 's/^x86_64$/amd64/; s/^aarch64$/arm64/')"

kind_load() {
  local image="$1" archive
  archive="$(mktemp "${TMPDIR:-/tmp}/kindload.XXXXXX")"
  docker save --platform "$NODE_PLATFORM" "$image" -o "$archive"
  kind load image-archive "$archive" --name "$CLUSTER"
  rm -f "$archive"
}

if [ "$SKIP_BUILD" -eq 0 ]; then
  echo "--- build ${MANAGER_BUILD_IMAGE} ---"
  docker build -f dockerfiles/manager.Dockerfile -t "$MANAGER_BUILD_IMAGE" .
  # Compiles the Rust xet staticlib before the Go binary; slow on a cold cache.
  echo "--- build ${AGENT_BUILD_IMAGE} ---"
  docker build -f dockerfiles/ome-agent.Dockerfile -t "$AGENT_BUILD_IMAGE" .
fi

if ! MANAGER_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$MANAGER_BUILD_IMAGE" 2>/dev/null)"; then
  echo "FATAL: local image ${MANAGER_BUILD_IMAGE} not found" >&2
  echo "       omit --skip-build or set OME_IMAGE_TAG to an existing image tag" >&2
  exit 1
fi
if ! AGENT_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$AGENT_BUILD_IMAGE" 2>/dev/null)"; then
  echo "FATAL: local image ${AGENT_BUILD_IMAGE} not found" >&2
  echo "       omit --skip-build or set OME_IMAGE_TAG to an existing image tag" >&2
  exit 1
fi

DEPLOY_TAG="$(deploy_tag_from_image_ids "$MANAGER_IMAGE_ID" "$AGENT_IMAGE_ID")"
MANAGER_IMAGE="ome-manager:${DEPLOY_TAG}"
AGENT_IMAGE="ome-agent:${DEPLOY_TAG}"
docker image tag "$MANAGER_BUILD_IMAGE" "$MANAGER_IMAGE"
docker image tag "$AGENT_BUILD_IMAGE" "$AGENT_IMAGE"
echo "--- deploy image set ${DEPLOY_TAG} ---"
echo "    manager: ${MANAGER_IMAGE_ID}"
echo "    agent:   ${AGENT_IMAGE_ID}"

echo "--- load images into ${CLUSTER} ---"
kind_load "$MANAGER_IMAGE"
kind_load "$AGENT_IMAGE"

echo "--- create namespace ${NS} ---"
kubectl --context "$KCTX" create namespace "$NS" --dry-run=client -o yaml \
  | kubectl --context "$KCTX" apply -f -

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
# hub empty => `ome-manager:<tag>`, matching the loaded image exactly. Both
# chart image tags use the common content-derived deploy tag so an agent-only
# rebuild also changes the controller Deployment template on a reused cluster.
# model-agent is an externally-published image this cluster cannot pull, and the
# eval does not exercise it: the fixtures back their ClusterBaseModel with a PVC.
# The chart defaults to 3 controller replicas at 2 CPU / 4Gi requests each,
# which cannot schedule on a single-node KIND cluster: the rollout stalls part
# way and never reports complete. One right-sized replica is what the eval
# needs anyway — the harness drives one reconcile loop at a time.
HELM_ARGS=(
  --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace
  --set global.hub=""
  --set ome.controller.tag="$DEPLOY_TAG"
  --set ome.omeAgent.tag="$DEPLOY_TAG"
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

# cert-manager's cainjector owns .webhooks[*].clientConfig.caBundle by then, so
# Helm's server-side apply reports a field-manager conflict on every webhook
# configuration and the release fails. The injected bundle is the value we
# want, and Helm re-asserting its placeholder over it would break admission —
# --force-conflicts lets the apply proceed, after which cainjector rewrites it.
echo "--- install controller (pass 2: converge) ---"
CONVERGE_ARGS=(--wait --timeout 300s)
if helm upgrade --help 2>&1 | grep -q -- '--force-conflicts'; then
  CONVERGE_ARGS+=(--force-conflicts)
else
  echo "WARNING: this helm has no --force-conflicts; pass 2 may fail on the" >&2
  echo "         cainjector-owned caBundle field. Helm 3.17+ is required." >&2
fi
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" "${CONVERGE_ARGS[@]}"

# The fixtures are templates: the PVC, the seed Job and the ClusterBaseModel's
# storageUri all have to name $NS, because a pod cannot mount a PVC from
# another namespace and the ISVC validator rejects a model that points at one.
echo "--- side-load the fixture images ---"
while read -r image; do
  [ -n "$image" ] || continue
  if docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null 2>&1; then
    kind_load "$image"
    echo "    loaded ${image}"
  else
    # canary-metrics lives in a private registry with no public equivalent.
    echo "WARNING: cannot obtain ${image}; cases needing it will fail to start" >&2
  fi
done < <(python3 "$RENDER" --kind --images)

echo "--- apply eval runtime fixtures into ${NS} ---"
OME_TEST_NS="$NS" python3 "$RENDER" --kind \
  | kubectl --context "$KCTX" -n "$NS" apply -f -

# The PVC path always runs a metadata-extraction Job, and the ISVC validator
# rejects every case until the model it references reports Ready. Gate here
# rather than let the first case fail with a confusing admission error.
echo "--- wait for the ClusterBaseModel to become Ready ---"
for _ in $(seq 60); do
  state="$(kubectl --context "$KCTX" get clusterbasemodel ome-coordtest-noop \
    -o jsonpath='{.status.state}' 2>/dev/null || true)"
  [ "$state" = "Ready" ] && break
  sleep 5
done
if [ "${state:-}" != "Ready" ]; then
  echo "FATAL: ClusterBaseModel stuck in state '${state:-<none>}'" >&2
  kubectl --context "$KCTX" get clusterbasemodel ome-coordtest-noop \
    -o jsonpath='{.status.conditions[*].message}{"\n"}' >&2 || true
  kubectl --context "$KCTX" -n "$NS" get pods >&2 || true
  exit 1
fi
echo "ClusterBaseModel Ready"

echo "--- OME deployed to ${CLUSTER}; run the eval with: ---"
echo "    OME_TEST_KUBECONFIG=\$(kind get kubeconfig --name ${CLUSTER} > /tmp/kc && echo /tmp/kc) \\"
echo "      OME_TEST_NS=${NS} python3 hack/omenative-testing/perm/batch.py --conc 4 p01-p05"
