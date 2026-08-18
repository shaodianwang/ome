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
#   OME_KIND_OFFLINE   use only baked artifacts     (default: false)
#   OME_KIND_IMAGES_DIR  baked image archives       (default: /images)
#   OME_KIND_EVAL_DIR    baked eval manifests       (default: /eval)
#   OME_KIND_PREBUILT_BASE  base for offline images (default: golang:1.25)
#   OME_KIND_PROMETHEUS_ENABLED  install Prometheus  (default: true)
#   OME_KIND_PROMETHEUS_REPOSITORY  Prometheus repository (default: chart value)
#   OME_KIND_PROMETHEUS_TAG  Prometheus tag            (default: chart value)
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
IMAGES_DIR="${OME_KIND_IMAGES_DIR:-/images}"
EVAL_DIR="${OME_KIND_EVAL_DIR:-/eval}"
PREBUILT_BASE="${OME_KIND_PREBUILT_BASE:-golang:1.25}"
PROMETHEUS_REPOSITORY="${OME_KIND_PROMETHEUS_REPOSITORY:-prom/prometheus}"
PROMETHEUS_TAG="${OME_KIND_PROMETHEUS_TAG:-v3.0.1}"
PROMETHEUS_IMAGE="${PROMETHEUS_REPOSITORY}:${PROMETHEUS_TAG}"
SKIP_BUILD=0

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for tool in docker kind helm kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool not on PATH" >&2; exit 1; }
done

KCTX="kind-${CLUSTER}"
MANAGER_BUILD_IMAGE="ome-manager:${BUILD_TAG}"
AGENT_BUILD_IMAGE="ome-agent:${BUILD_TAG}"
RENDER="hack/omenative-testing/perm/runtime_fixtures.py"
METRICS_IMAGE="$(python3 "$RENDER" --kind --value OME_EVAL_METRICS_IMAGE)"

case "${OME_KIND_OFFLINE:-false}" in
  1|true|TRUE|yes|YES) OFFLINE=1 ;;
  0|false|FALSE|no|NO|"") OFFLINE=0 ;;
  *) echo "FATAL: OME_KIND_OFFLINE must be true or false" >&2; exit 2 ;;
esac
case "${OME_KIND_PROMETHEUS_ENABLED:-true}" in
  1|true|TRUE|yes|YES) PROMETHEUS_ENABLED=true ;;
  0|false|FALSE|no|NO) PROMETHEUS_ENABLED=false ;;
  *) echo "FATAL: OME_KIND_PROMETHEUS_ENABLED must be true or false" >&2; exit 2 ;;
esac

require_file() {
  [ -s "$1" ] || { echo "FATAL: required offline artifact is missing: $1" >&2; exit 1; }
}

CERT_IMAGES=()
NGINX_IMAGE=""
if [ "$OFFLINE" -eq 1 ]; then
  if [ "$SKIP_BUILD" -eq 0 ]; then
    for tool in cargo go make; do
      command -v "$tool" >/dev/null 2>&1 || {
        echo "FATAL: $tool not on PATH for offline image builds" >&2
        exit 1
      }
    done
  fi
  require_file "${EVAL_DIR}/cert-manager.yaml"
  require_file "${EVAL_DIR}/cert-manager-images.txt"
  require_file "${IMAGES_DIR}/eval-nginx.tar"
  require_file "${IMAGES_DIR}/fake-serving-build.tar"
  if [ "$PROMETHEUS_ENABLED" = true ]; then
    require_file "${IMAGES_DIR}/eval-prometheus.tar"
  fi
  grep -Fq "app.kubernetes.io/version: \"${CERT_MANAGER_VERSION}\"" \
    "${EVAL_DIR}/cert-manager.yaml" || {
      echo "FATAL: baked cert-manager manifest is not ${CERT_MANAGER_VERSION}" >&2
      exit 1
    }
  mapfile -t CERT_IMAGES < <(sed '/^[[:space:]]*$/d' "${EVAL_DIR}/cert-manager-images.txt")
  [ "${#CERT_IMAGES[@]}" -gt 0 ] || {
    echo "FATAL: baked cert-manager image list is empty" >&2
    exit 1
  }
  for i in "${!CERT_IMAGES[@]}"; do
    require_file "${IMAGES_DIR}/cert-manager-${i}.tar"
  done
  NGINX_IMAGE="$(python3 "$RENDER" --kind --value OME_EVAL_NGINX_IMAGE)"
  [ -n "$NGINX_IMAGE" ] || {
    echo "FATAL: runtime renderer returned an empty nginx image" >&2
    exit 1
  }
  echo "--- offline mode: using baked artifacts ---"
fi

kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || {
  echo "FATAL: no KIND cluster named $CLUSTER (create it first)" >&2; exit 1; }

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

ensure_prebuilt_base() {
  if ! docker image inspect "$PREBUILT_BASE" >/dev/null 2>&1; then
    docker load -i "${IMAGES_DIR}/fake-serving-build.tar" >/dev/null
  fi
  docker image inspect "$PREBUILT_BASE" >/dev/null 2>&1 || {
    echo "FATAL: baked archive did not provide ${PREBUILT_BASE}" >&2
    exit 1
  }
}

build_prebuilt_image() (
  set -e
  local image="$1" package="$2" binary="$3" run_user="$4" stage
  shift 4
  stage="$(mktemp -d "${TMPDIR:-/tmp}/ome-prebuilt.XXXXXX")"
  trap 'rm -rf -- "$stage"' EXIT

  echo "--- compile ${package} ---"
  CGO_ENABLED=1 GOTOOLCHAIN=local GOPROXY=off \
    go build -trimpath -o "${stage}/${binary}" "$package"
  ln -s "$binary" "${stage}/entrypoint"
  for extra in "$@"; do
    cp "$extra" "${stage}/$(basename "$extra")"
  done

  echo "--- package ${image} ---"
  DOCKER_BUILDKIT=0 docker build --pull=false --network=none \
    -f dockerfiles/kind-prebuilt.Dockerfile \
    --build-arg "BASE_IMAGE=${PREBUILT_BASE}" \
    --build-arg "RUN_USER=${run_user}" \
    -t "$image" "$stage"
)

build_canary_metrics_image() (
  set -e
  local stage
  stage="$(mktemp -d "${TMPDIR:-/tmp}/ome-metrics.XXXXXX")"
  trap 'rm -rf -- "$stage"' EXIT

  echo "--- compile canary metrics fixture ---"
  (
    cd hack/omenative-testing/metrics-server
    CGO_ENABLED=0 GOTOOLCHAIN=local GOPROXY=off \
      go build -trimpath -o "${stage}/canary-metrics" .
  )
  ln -s canary-metrics "${stage}/entrypoint"
  DOCKER_BUILDKIT=0 docker build --pull=false --network=none \
    -f dockerfiles/kind-prebuilt.Dockerfile \
    --build-arg BASE_IMAGE=scratch \
    --build-arg RUN_USER=65532:65532 \
    -t "$METRICS_IMAGE" "$stage"
)

if [ "$SKIP_BUILD" -eq 0 ] && [ "$OFFLINE" -eq 1 ]; then
  ensure_prebuilt_base
  CARGO_NET_OFFLINE=true GOTOOLCHAIN=local GOPROXY=off make xet-build
  build_prebuilt_image "$MANAGER_BUILD_IMAGE" ./cmd/manager manager 65532:65532
  build_prebuilt_image "$AGENT_BUILD_IMAGE" ./cmd/ome-agent ome-agent 0:0 \
    config/ome-agent/ome-agent.yaml
  build_canary_metrics_image
elif [ "$SKIP_BUILD" -eq 0 ]; then
  echo "--- build ${MANAGER_BUILD_IMAGE} ---"
  docker build -f dockerfiles/manager.Dockerfile -t "$MANAGER_BUILD_IMAGE" .
  # Compiles the Rust xet staticlib before the Go binary; slow on a cold cache.
  echo "--- build ${AGENT_BUILD_IMAGE} ---"
  docker build -f dockerfiles/ome-agent.Dockerfile -t "$AGENT_BUILD_IMAGE" .
  echo "--- build ${METRICS_IMAGE} ---"
  docker build -f dockerfiles/canary-metrics.Dockerfile -t "$METRICS_IMAGE" .
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
if docker image inspect "$METRICS_IMAGE" >/dev/null 2>&1; then
  kind_load "$METRICS_IMAGE"
elif [ "$OFFLINE" -eq 1 ]; then
  echo "FATAL: local image ${METRICS_IMAGE} not found" >&2
  exit 1
fi

echo "--- create namespace ${NS} ---"
kubectl --context "$KCTX" create namespace "$NS" --dry-run=client -o yaml \
  | kubectl --context "$KCTX" apply -f -

echo "--- install cert-manager ${CERT_MANAGER_VERSION} ---"
if [ "$OFFLINE" -eq 1 ]; then
  for i in "${!CERT_IMAGES[@]}"; do
    docker load -i "${IMAGES_DIR}/cert-manager-${i}.tar" >/dev/null
    docker image inspect "${CERT_IMAGES[$i]}" >/dev/null 2>&1 || {
      echo "FATAL: cert-manager archive ${i} did not provide ${CERT_IMAGES[$i]}" >&2
      exit 1
    }
    kind_load "${CERT_IMAGES[$i]}"
  done
  kubectl --context "$KCTX" apply -f "${EVAL_DIR}/cert-manager.yaml"
else
  kubectl --context "$KCTX" apply -f \
    "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
fi
kubectl --context "$KCTX" -n cert-manager wait --for=condition=Available \
  deployment --all --timeout=300s

if [ "$OFFLINE" -eq 1 ] && [ "$PROMETHEUS_ENABLED" = true ]; then
  echo "--- side-load ${PROMETHEUS_IMAGE} ---"
  docker load -i "${IMAGES_DIR}/eval-prometheus.tar" >/dev/null
  docker image inspect "$PROMETHEUS_IMAGE" >/dev/null 2>&1 || {
    echo "FATAL: baked Prometheus archive did not provide ${PROMETHEUS_IMAGE}" >&2
    exit 1
  }
  kind_load "$PROMETHEUS_IMAGE"
fi

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
  --set prometheus.enabled="$PROMETHEUS_ENABLED"
  --set prometheus.image.repository="$PROMETHEUS_REPOSITORY"
  --set prometheus.image.tag="$PROMETHEUS_TAG"
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
  echo "    helm has no --force-conflicts; using its default upgrade path"
fi
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" "${CONVERGE_ARGS[@]}"

# The fixtures are templates: the PVC, the seed Job and the ClusterBaseModel's
# storageUri all have to name $NS, because a pod cannot mount a PVC from
# another namespace and the ISVC validator rejects a model that points at one.
echo "--- side-load the fixture images ---"
if [ "$OFFLINE" -eq 1 ]; then
  docker load -i "${IMAGES_DIR}/eval-nginx.tar" >/dev/null
  docker image inspect "$NGINX_IMAGE" >/dev/null 2>&1 || {
    echo "FATAL: baked nginx archive did not provide ${NGINX_IMAGE}" >&2
    exit 1
  }
  kind_load "$NGINX_IMAGE"
  while read -r image; do
    [ -n "$image" ] || continue
    case "$image" in
      *nginxinc/nginx-unprivileged:*)
        docker image tag "$NGINX_IMAGE" "$image"
        kind_load "$image"
        ;;
      *)
        echo "FATAL: no offline source for case image ${image}" >&2
        exit 1
        ;;
    esac
  done < <(python3 "$RENDER" --case-images)
else
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
fi

echo "--- seed the eval model fixture in ${NS} ---"
OME_TEST_NS="$NS" python3 "$RENDER" --kind --stage bootstrap \
  | kubectl --context "$KCTX" -n "$NS" apply -f -
kubectl --context "$KCTX" -n "$NS" wait --for=condition=Complete \
  job/ome-coordtest-noop-seed --timeout=300s

echo "--- apply eval runtime fixtures into ${NS} ---"
OME_TEST_NS="$NS" python3 "$RENDER" --kind --stage resources \
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
