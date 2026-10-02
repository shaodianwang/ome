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
#   OME_EVAL_FORCE_DELETE  set the controller's lifecycle.forceDelete policy (default: false)
#   OME_EVAL_FORCE_DELETE_THRESHOLD  its nodeUnreachableThreshold (default: 90s)
#   OME_EVAL_FORCE_DELETE_SLACK  its overdueSlack             (default: 30s)
#   OME_EVAL_CONTROL_PLANE_PRIORITY_CLASS  PriorityClass stamped on every
#                      Deployment in OME_NAMESPACE (default: fixture value)
#   OME_EVAL_CONTROL_PLANE_PRIORITY  its value (default: fixture value)
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

# derive_image gives dst its own image id over src's filesystem. A tag that
# merely aliases src shares its id, and the runtime names a container's image
# by whichever reference of that id it lists first, so an in-place image change
# between two aliases is never confirmed by the pod status.
derive_image() {
  local src="$1" dst="$2" container
  container="$(docker create "$src")" || return 1
  if ! docker commit --change "LABEL ome.eval/derived-from=${src}" "$container" "$dst" >/dev/null; then
    docker rm -f "$container" >/dev/null 2>&1 || true
    return 1
  fi
  docker rm -f "$container" >/dev/null
}

# Keep the content-derived tag and image helpers directly testable without
# executing a cluster deployment when this file is sourced.
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
# Empty: the cluster default schedules the fixtures. A name (the scheduler lane
# sets ome-scheduler) is stamped on every fixture runtime and the matching
# chart is installed below.
SCHEDULER_NAME="$(python3 "$RENDER" --kind --value OME_EVAL_SCHEDULER)"
SCHEDULER_BUILD_IMAGE="ome-scheduler:${BUILD_TAG}"
# The harness's preempt cases press on a node with a pod one notch above the
# case's pods, and the scheduler takes the lowest pods no disruption budget
# covers first: the control plane must outrank that press or it is the
# victim. The class comes from the fixtures (the harness reads the same
# value), is created before the controller lands and is stamped on every
# Deployment this script installs into the OME namespace.
CONTROL_PLANE_CLASS="$(python3 "$RENDER" --kind --value OME_EVAL_CONTROL_PLANE_PRIORITY_CLASS)"
[ -n "$CONTROL_PLANE_CLASS" ] || {
  echo "FATAL: runtime renderer returned an empty control-plane PriorityClass name" >&2
  exit 1
}
# The gang scheduler pins every gang to a topology domain and treats a gang
# without one as unresolvable. KIND nodes carry no domain label, so the whole
# cluster is labeled as one domain and the scheduler falls back to that key.
SCHEDULER_TOPOLOGY_KEY="${OME_EVAL_SCHEDULER_TOPOLOGY_KEY:-topology.kubernetes.io/zone}"
SCHEDULER_TOPOLOGY_DOMAIN="${OME_EVAL_SCHEDULER_TOPOLOGY_DOMAIN:-ome-eval}"

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
# Force-delete dimension: the controller's lifecycle.forceDelete policy, off
# by chart default. On, the harness's node-loss cases that need the policy
# run (relocation of a Ready Instance off a dead node) and those that read
# the fate without it SKIP. The threshold is the age the node's unreachable
# evidence must reach before the policy acts: longer than the harness's
# settle window, after which a repaired case starts the kubelet again (the
# node reads NotReady for about 30-40 s then), and short enough that a
# relocation lands well inside the case budget. The slack is how long past
# a Terminating pod's own deadline before it counts as wedged.
case "${OME_EVAL_FORCE_DELETE:-false}" in
  1|true|TRUE|yes|YES) FORCE_DELETE_ENABLED=1 ;;
  0|false|FALSE|no|NO|"") FORCE_DELETE_ENABLED=0 ;;
  *) echo "FATAL: OME_EVAL_FORCE_DELETE must be true or false" >&2; exit 2 ;;
esac
FORCE_DELETE_THRESHOLD="${OME_EVAL_FORCE_DELETE_THRESHOLD:-90s}"
FORCE_DELETE_SLACK="${OME_EVAL_FORCE_DELETE_SLACK:-30s}"

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
  require_file "${EVAL_DIR}/podgroup-crd.yaml"
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

# The first calls into a freshly created cluster can hit an apiserver or etcd
# that is still settling. `kubectl apply` is idempotent, so the bring-up
# applies retry a few times before a failure counts as real.
apply_with_retry() {
  local attempt
  for attempt in 1 2 3 4 5; do
    if kubectl --context "$KCTX" apply "$@"; then
      return 0
    fi
    echo "kubectl apply failed (attempt ${attempt}/5); retrying in 10s" >&2
    sleep 10
  done
  return 1
}

# `kubectl wait` errors out instead of waiting while a just-created CRD has no
# status yet, so poll until the condition exists and then let wait watch it.
wait_for_crd_established() {
  local crd="$1" deadline
  deadline=$(( $(date +%s) + 120 ))
  until kubectl --context "$KCTX" wait --for=condition=Established "crd/${crd}" --timeout=60s 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "FATAL: crd/${crd} never became Established" >&2
      return 1
    fi
    sleep 3
  done
}

# Stamp the control-plane PriorityClass on every Deployment in the OME
# namespace. Idempotent: a template that already names the class is left as
# it is, so the second call (after the converge pass and the scheduler
# install) rolls nothing that the first already rolled.
set_control_plane_priority() {
  local deploy
  for deploy in $(kubectl --context "$KCTX" -n "$OME_NS" get deployments -o name); do
    kubectl --context "$KCTX" -n "$OME_NS" patch "$deploy" --type=merge \
      -p "{\"spec\":{\"template\":{\"spec\":{\"priorityClassName\":\"${CONTROL_PLANE_CLASS}\"}}}}"
  done
}

# Every Deployment in the OME namespace has rolled onto a template that names
# the class; a deploy that leaves one below the harness's press is wrong.
verify_control_plane_priority() {
  local deploy have
  for deploy in $(kubectl --context "$KCTX" -n "$OME_NS" get deployments -o name); do
    kubectl --context "$KCTX" -n "$OME_NS" rollout status "$deploy" --timeout=300s
    have="$(kubectl --context "$KCTX" -n "$OME_NS" get "$deploy" \
      -o jsonpath='{.spec.template.spec.priorityClassName}')"
    [ "$have" = "$CONTROL_PLANE_CLASS" ] || {
      echo "FATAL: ${deploy} runs under PriorityClass '${have:-<none>}', not ${CONTROL_PLANE_CLASS}" >&2
      exit 1
    }
  done
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

build_scheduler_image() (
  set -e
  local stage
  stage="$(mktemp -d "${TMPDIR:-/tmp}/ome-scheduler.XXXXXX")"
  trap 'rm -rf -- "$stage"' EXIT

  echo "--- compile ome-scheduler ---"
  (
    cd scheduler
    CGO_ENABLED=0 GOTOOLCHAIN=local GOPROXY=off \
      go build -trimpath -o "${stage}/ome-scheduler" ./cmd/ome-scheduler
  )
  ln -s ome-scheduler "${stage}/entrypoint"
  DOCKER_BUILDKIT=0 docker build --pull=false --network=none \
    -f dockerfiles/kind-prebuilt.Dockerfile \
    --build-arg BASE_IMAGE=scratch \
    --build-arg RUN_USER=65532:65532 \
    -t "$SCHEDULER_BUILD_IMAGE" "$stage"
)

if [ "$SKIP_BUILD" -eq 0 ] && [ "$OFFLINE" -eq 1 ]; then
  ensure_prebuilt_base
  CARGO_NET_OFFLINE=true GOTOOLCHAIN=local GOPROXY=off make xet-build
  build_prebuilt_image "$MANAGER_BUILD_IMAGE" ./cmd/manager manager 65532:65532
  build_prebuilt_image "$AGENT_BUILD_IMAGE" ./cmd/ome-agent ome-agent 0:0 \
    config/ome-agent/ome-agent.yaml
  build_canary_metrics_image
  [ -z "$SCHEDULER_NAME" ] || build_scheduler_image
elif [ "$SKIP_BUILD" -eq 0 ]; then
  echo "--- build ${MANAGER_BUILD_IMAGE} ---"
  docker build -f dockerfiles/manager.Dockerfile -t "$MANAGER_BUILD_IMAGE" .
  # Compiles the Rust xet staticlib before the Go binary; slow on a cold cache.
  echo "--- build ${AGENT_BUILD_IMAGE} ---"
  docker build -f dockerfiles/ome-agent.Dockerfile -t "$AGENT_BUILD_IMAGE" .
  echo "--- build ${METRICS_IMAGE} ---"
  docker build -f dockerfiles/canary-metrics.Dockerfile -t "$METRICS_IMAGE" .
  if [ -n "$SCHEDULER_NAME" ]; then
    echo "--- build ${SCHEDULER_BUILD_IMAGE} ---"
    docker build -f dockerfiles/ome-scheduler.Dockerfile -t "$SCHEDULER_BUILD_IMAGE" .
  fi
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
SCHEDULER_IMAGE=""
if [ -n "$SCHEDULER_NAME" ]; then
  docker image inspect "$SCHEDULER_BUILD_IMAGE" >/dev/null 2>&1 || {
    echo "FATAL: local image ${SCHEDULER_BUILD_IMAGE} not found; OME_EVAL_SCHEDULER=${SCHEDULER_NAME} needs it" >&2
    echo "       omit --skip-build or set OME_IMAGE_TAG to an existing image tag" >&2
    exit 1
  }
  SCHEDULER_IMAGE="ome-scheduler:${DEPLOY_TAG}"
  docker image tag "$SCHEDULER_BUILD_IMAGE" "$SCHEDULER_IMAGE"
  echo "    scheduler: ${SCHEDULER_IMAGE}"
fi
echo "--- deploy image set ${DEPLOY_TAG} ---"
echo "    manager: ${MANAGER_IMAGE_ID}"
echo "    agent:   ${AGENT_IMAGE_ID}"

echo "--- load images into ${CLUSTER} ---"
kind_load "$MANAGER_IMAGE"
kind_load "$AGENT_IMAGE"
[ -z "$SCHEDULER_IMAGE" ] || kind_load "$SCHEDULER_IMAGE"
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
  apply_with_retry -f "${EVAL_DIR}/cert-manager.yaml"
else
  apply_with_retry -f \
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
# The rollout policy feature is chart-gated on both charts from one key; the
# eval exercises policy references, so the CRD and its controller are on.
helm upgrade --install ome-crd charts/ome-crd/ \
  --kube-context "$KCTX" --namespace "$OME_NS" --create-namespace \
  --set ome.rolloutPolicy.enabled=true \
  --wait --timeout 180s

# Staged in a file: a retried apply must read the manifest again.
echo "--- create the control-plane PriorityClass ${CONTROL_PLANE_CLASS} ---"
control_plane_class_file="$(mktemp "${TMPDIR:-/tmp}/ome-control-plane-class.XXXXXX")"
python3 "$RENDER" --kind --control-plane-priority-class >"$control_plane_class_file"
apply_with_retry -f "$control_plane_class_file"
rm -f -- "$control_plane_class_file"

if [ "$OFFLINE" -eq 1 ]; then
  echo "--- install PodGroup CRD ---"
  apply_with_retry -f "${EVAL_DIR}/podgroup-crd.yaml"
  wait_for_crd_established podgroups.scheduling.x-k8s.io
elif [ -n "$SCHEDULER_NAME" ]; then
  # The gang scheduler lists PodGroups at startup; vendor the CRD the same way
  # the KIND suites do and install it before the chart.
  echo "--- install PodGroup CRD ---"
  make scheduler-plugins-crds
  apply_with_retry -f tests/integration/dep-crds/scheduler-plugins/
  wait_for_crd_established podgroups.scheduling.x-k8s.io
fi

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
  --set ome.rolloutPolicy.enabled=true
  --set ome.controller.resources.requests.cpu=500m
  --set ome.controller.resources.requests.memory=512Mi
  --set ome.controller.resources.limits.cpu=2
  --set ome.controller.resources.limits.memory=2Gi
)
if [ "$FORCE_DELETE_ENABLED" -eq 1 ]; then
  echo "    force-delete policy on: nodeUnreachableThreshold=${FORCE_DELETE_THRESHOLD} overdueSlack=${FORCE_DELETE_SLACK}"
  HELM_ARGS+=(
    --set ome.controller.lifecycle.forceDelete.nodeUnreachableThreshold="$FORCE_DELETE_THRESHOLD"
    --set ome.controller.lifecycle.forceDelete.overdueSlack="$FORCE_DELETE_SLACK"
  )
fi

# Two passes, because the chart both registers webhooks with failurePolicy=Fail
# and ships a ClusterServingRuntime those webhooks intercept. Helm submits the
# Deployment and that resource in a single pass, so on a fresh cluster the
# resource is rejected while the controller is not yet serving and the caBundle
# is still the chart's placeholder. The first pass lands the Deployment, webhook
# configuration and Certificate; the second converges once the webhook answers.
echo "    pass 1: land the deployment and webhook plumbing"
helm upgrade --install ome charts/ome-resources/ "${HELM_ARGS[@]}" --wait=false || true

# Before the first rollout is waited on, so the controller rolls once.
echo "--- raise the control plane to ${CONTROL_PLANE_CLASS} ---"
set_control_plane_priority

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
if [ -n "$SCHEDULER_NAME" ]; then
  # Same content-derived tag as the controller so a rebuilt scheduler rolls on a
  # reused cluster. One replica: a single-node cluster cannot host the chart's
  # HA pair, and leader election with one candidate is still correct.
  echo "--- label every node as one topology domain (${SCHEDULER_TOPOLOGY_KEY}=${SCHEDULER_TOPOLOGY_DOMAIN}) ---"
  kubectl --context "$KCTX" label nodes --all --overwrite \
    "${SCHEDULER_TOPOLOGY_KEY}=${SCHEDULER_TOPOLOGY_DOMAIN}"
  echo "--- install ${SCHEDULER_NAME} (gang scheduler) ---"
  helm upgrade --install ome-scheduler charts/ome-scheduler/ \
    --kube-context "$KCTX" --namespace "$OME_NS" \
    --set global.hub="" \
    --set scheduler.name="$SCHEDULER_NAME" \
    --set scheduler.plugin.topologyKey="$SCHEDULER_TOPOLOGY_KEY" \
    --set scheduler.image.repository=ome-scheduler \
    --set scheduler.image.tag="$DEPLOY_TAG" \
    --set scheduler.replicaCount=1 \
    --set scheduler.podDisruptionBudget.enabled=false \
    --set scheduler.resources.requests.cpu=100m \
    --set scheduler.resources.requests.memory=256Mi \
    --set scheduler.resources.limits.cpu=1 \
    --set scheduler.resources.limits.memory=1Gi \
    --wait --timeout 300s
fi

# The converge pass keeps the class (Helm leaves a field its chart never
# names alone) and the scheduler arrives without it; stamp again and check.
echo "--- verify the control plane runs under ${CONTROL_PLANE_CLASS} ---"
set_control_plane_priority
verify_control_plane_priority

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
        # The archive carries one nginx image; every other nginx tag a case
        # names is derived from it rather than aliased, so a roll between the
        # two moves to a distinct image id the runtime reports by that name.
        derive_image "$NGINX_IMAGE" "$image"
        kind_load "$image"
        ;;
      *)
        echo "FATAL: no offline source for case image ${image}" >&2
        exit 1
        ;;
    esac
  done < <(python3 "$RENDER" --kind --case-images)
else
  while read -r image; do
    [ -n "$image" ] || continue
    if docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null 2>&1; then
      kind_load "$image"
      echo "    loaded ${image}"
    else
      # canary-metrics is built from this repo, not pulled; without a local
      # build there is nothing to load.
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
  # Full diagnostic dump: which node(s) exist, where the PV/PVC actually
  # bound, and what the seed and metadata-extraction pods each saw. All of
  # this is CI-log-only (stderr, no artifact upload) so it costs nothing on
  # the success path and never runs there.
  echo "--- diagnostics: nodes ---" >&2
  kubectl --context "$KCTX" get nodes -o wide >&2 || true
  echo "--- diagnostics: PersistentVolume ---" >&2
  kubectl --context "$KCTX" get pv -o wide >&2 || true
  kubectl --context "$KCTX" get pv ome-coordtest-noop-pv -o yaml >&2 || true
  echo "--- diagnostics: PersistentVolumeClaim ---" >&2
  kubectl --context "$KCTX" -n "$NS" get pvc -o wide >&2 || true
  kubectl --context "$KCTX" -n "$NS" get pvc ome-coordtest-noop-pvc -o yaml >&2 || true
  echo "--- diagnostics: pods ---" >&2
  kubectl --context "$KCTX" -n "$NS" get pods -o wide >&2 || true
  for pod in $(kubectl --context "$KCTX" -n "$NS" get pods -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    echo "--- diagnostics: describe $pod ---" >&2
    kubectl --context "$KCTX" -n "$NS" describe pod "$pod" >&2 || true
    echo "--- diagnostics: logs $pod ---" >&2
    kubectl --context "$KCTX" -n "$NS" logs "$pod" --all-containers --tail=200 >&2 || true
  done
  exit 1
fi
echo "ClusterBaseModel Ready"

echo "--- OME deployed to ${CLUSTER}; run the eval with: ---"
echo "    OME_TEST_KUBECONFIG=\$(kind get kubeconfig --name ${CLUSTER} > /tmp/kc && echo /tmp/kc) \\"
echo "      OME_TEST_NS=${NS} python3 hack/omenative-testing/perm/batch.py --conc 4 p01-p05"
