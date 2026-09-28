#!/bin/sh
# Run one named slice of the deployed OMENative YAML harness on GKE-backed KIND.
set -eu

feature=${1:?feature is required}
topology=${2:?topology is required}
shift 2
[ "$#" -gt 0 ] || { echo "at least one case id is required" >&2; exit 2; }
for case_id in "$@"; do
  case "$case_id" in
    p[0-9]*)
      case "${case_id#p}" in
        *[!0-9]*) echo "invalid case id: $case_id" >&2; exit 2 ;;
      esac
      ;;
    *) echo "invalid case id: $case_id" >&2; exit 2 ;;
  esac
done

short_sha=$(printf '%.8s' "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}")
scheduler="${OME_EVAL_SCHEDULER:-}"
lane="${feature}-${topology}${scheduler:+-${scheduler}}"
cluster=$(printf 'ome-eval-%s' "$lane" | tr '_' '-')
images_dir="${OME_KIND_IMAGES_DIR:-/images}"
repo="${PROD_ECR_REGISTRY:-168653207203.dkr.ecr.us-east-2.amazonaws.com}/msl_infra"
run_stamp="${BUILDKITE_BUILD_NUMBER:-local}-${BUILDKITE_JOB_ID:-$$}"
results_root="hack/omenative-testing/perm/results/${run_stamp}"
diagnostics_dir="${results_root}/diagnostics"
tmp_dir=$(mktemp -d)
kubeconfig="${tmp_dir}/kubeconfig"
# Exit-code contract for the pipeline's infra-retry preset, which reruns exit
# 125 and never exit 1: a status-1 failure before the first case runs (the
# dind sidecar, the image load, `kind create cluster`, the settle gate or the
# OME deploy) is bring-up on a contended agent, not a result, and leaves as
# 125. Once the cases start the shard keeps its own status: a red case is a
# finding, never a retry. Usage and configuration errors exit 2 and stay 2.
before_cases=1
stage=setup

collect_diagnostics() {
  [ -s "$kubeconfig" ] || return 0
  mkdir -p "$diagnostics_dir"
  KUBECONFIG="$kubeconfig" kubectl get nodes -o wide \
    >"${diagnostics_dir}/nodes.txt" 2>&1 || true
  KUBECONFIG="$kubeconfig" kubectl get pods -A -o wide \
    >"${diagnostics_dir}/pods.txt" 2>&1 || true
  KUBECONFIG="$kubeconfig" kubectl get isvc,ir -A -o yaml \
    >"${diagnostics_dir}/ome-resources.yaml" 2>&1 || true
  KUBECONFIG="$kubeconfig" kubectl -n ome logs deployment/ome-controller-manager \
    --all-containers --tail=-1 >"${diagnostics_dir}/controller.log" 2>&1 || true
  # Scheduler-lane evidence: the gang scheduler's own log, the PodGroups it
  # saw, and the events (FailedScheduling reasons outlive the pods).
  if KUBECONFIG="$kubeconfig" kubectl -n ome get deployment ome-scheduler >/dev/null 2>&1; then
    KUBECONFIG="$kubeconfig" kubectl -n ome logs deployment/ome-scheduler \
      --all-containers --tail=-1 >"${diagnostics_dir}/scheduler.log" 2>&1 || true
  fi
  KUBECONFIG="$kubeconfig" kubectl get podgroups.scheduling.x-k8s.io -A -o yaml \
    >"${diagnostics_dir}/podgroups.yaml" 2>&1 || true
  KUBECONFIG="$kubeconfig" kubectl get events -A --sort-by=.lastTimestamp \
    >"${diagnostics_dir}/events.txt" 2>&1 || true
}

finish() {
  status=$?
  trap - EXIT
  set +e
  if [ "$status" -eq 1 ] && [ "$before_cases" -eq 1 ]; then
    echo "--- ${stage} failed before any case ran; exiting 125 for infra retry ---" >&2
    status=125
  fi
  collect_diagnostics
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  if command -v buildkite-agent >/dev/null 2>&1; then
    results_archive="${results_root}.tgz"
    if tar -C "$(dirname "$results_root")" -czf "$results_archive" "$(basename "$results_root")"; then
      if ! buildkite-agent artifact upload "$results_archive"; then
        echo "WARNING: result artifact upload failed; the batch summary and any failed case logs remain in this job log" >&2
      fi
      rm -f -- "$results_archive"
    else
      echo "WARNING: could not package eval result artifacts" >&2
    fi
  fi
  rm -rf -- "$tmp_dir"
  exit "$status"
}
trap finish EXIT

echo "--- ${feature}/${topology}: $# cases ---"
echo "    $*"

stage="wait for dind"
echo "--- ${stage} ---"
i=0
until docker info >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -le 60 ] || { echo "Docker daemon did not become ready" >&2; exit 1; }
  sleep 2
done

stage="load KIND and exact runtime images"
echo "--- ${stage} ---"
for archive in kindest-node busybox ome-manager ome-agent ome-scheduler canary-metrics; do
  path="${images_dir}/${archive}.tar"
  [ -s "$path" ] || { echo "missing image archive: $path" >&2; exit 1; }
  docker load -i "$path"
done

# Tag-only: kindest-node.tar loads under its RepoTag, and this agent has no
# egress to resolve a digest ref.
node_image=$(tr -d '[:space:]' <.kind-node-image | cut -d@ -f1)
[ -n "$node_image" ] || { echo ".kind-node-image is empty" >&2; exit 1; }

manager_ref="${repo}/ome-manager:${short_sha}"
agent_ref="${repo}/ome-agent:${short_sha}"
metrics_ref="${repo}/canary-metrics:${short_sha}"
scheduler_ref="${repo}/ome-scheduler:${short_sha}"
docker image inspect "$manager_ref" >/dev/null
docker image inspect "$agent_ref" >/dev/null
docker image inspect "$metrics_ref" >/dev/null
docker image inspect "$scheduler_ref" >/dev/null
docker tag "$manager_ref" "ome-manager:${short_sha}"
docker tag "$agent_ref" "ome-agent:${short_sha}"
docker tag "$metrics_ref" canary-metrics:kind-local
docker tag "$scheduler_ref" "ome-scheduler:${short_sha}"

docker run --rm --privileged busybox sh -c '
  sysctl -w fs.inotify.max_user_watches=1048576
  sysctl -w fs.inotify.max_user_instances=8192
'

mkdir -p /ome-eval-model
chmod 0777 /ome-eval-model
# Worker count: two by default; the gang scheduler places one gang member per
# node, so a multi-node twin arrives with OME_EVAL_KIND_WORKERS sized to its
# shard's peak pods.
workers="${OME_EVAL_KIND_WORKERS:-2}"
case "$workers" in
  ''|*[!0-9]*) echo "OME_EVAL_KIND_WORKERS must be a positive integer, got '$workers'" >&2; exit 2 ;;
esac
[ "$workers" -ge 1 ] || { echo "OME_EVAL_KIND_WORKERS must be at least 1" >&2; exit 2; }
{
  cat <<'KINDEOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: /ome-eval-model
        containerPath: /ome-eval-model
KINDEOF
  i=0
  while [ "$i" -lt "$workers" ]; do
    cat <<'KINDEOF'
  - role: worker
    extraMounts:
      - hostPath: /ome-eval-model
        containerPath: /ome-eval-model
KINDEOF
    i=$((i + 1))
  done
} >"${tmp_dir}/kind.yaml"
if [ -n "${OME_EVAL_SCHEDULER_SKIPPED:-}" ]; then
  echo "--- cases above the whole-node cap, not run in this scheduler twin: ${OME_EVAL_SCHEDULER_SKIPPED} ---"
fi
stage="create KIND cluster: 1 control-plane + ${workers} worker(s)"
echo "--- ${stage} ---"
kind create cluster --name "$cluster" --image "$node_image" \
  --config "${tmp_dir}/kind.yaml" --wait 180s
kind get kubeconfig --name "$cluster" >"$kubeconfig"

export KUBECONFIG="$kubeconfig"
export OME_TEST_KUBECONFIG="$kubeconfig"
export OME_TEST_NS=sandbox
export OME_EVAL_KIND=1
export OME_TEST_ALLOW_CTRL_RESTART=1
export OME_EVAL_METRICS_IMAGE=canary-metrics:kind-local
export OME_EVAL_SCHEDULER="$scheduler"
export OME_KIND_OFFLINE=true
export OME_IMAGE_TAG="$short_sha"
export OME_RUN_TS="$run_stamp"

stage="wait for the cluster to settle"
echo "--- ${stage} ---"
# `kind create --wait` reports its own timeout as a warning and returns
# success, so a cluster that is still bootstrapping arrives here looking
# created. Wait for every node to register and turn Ready and for the
# apiserver to answer three probes in a row; a cluster that never settles
# fails as bring-up, not as a case.
settle_timeout="${OME_EVAL_KIND_SETTLE_TIMEOUT:-300}"
case "$settle_timeout" in
  ''|*[!0-9]*) echo "OME_EVAL_KIND_SETTLE_TIMEOUT must be a number of seconds, got '$settle_timeout'" >&2; exit 2 ;;
esac
expected_nodes=$((workers + 1))
settle_deadline=$(( $(date +%s) + settle_timeout ))
stable=0
while [ "$stable" -lt 3 ]; do
  if [ "$(date +%s)" -ge "$settle_deadline" ]; then
    echo "cluster did not settle within ${settle_timeout}s" >&2
    kubectl get nodes 2>&1 | head -20 >&2
    exit 1
  fi
  registered=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [ "${registered:-0}" -ge "$expected_nodes" ] \
     && kubectl wait --for=condition=Ready node --all --timeout=15s >/dev/null 2>&1 \
     && kubectl get --raw=/readyz >/dev/null 2>&1; then
    stable=$((stable + 1))
  else
    stable=0
  fi
  sleep 5
done
kubectl get nodes

stage="deploy exact OME images"
echo "--- ${stage} ---"
bash hack/omenative-testing/deploy-ome-kind.sh --cluster "$cluster" --skip-build

echo "--- validate and run ${feature}/${topology} ---"
before_cases=0
(
  cd hack/omenative-testing/perm
  python3 run.py --validate-only
  python3 batch.py \
    --conc "${OME_EVAL_CASE_CONCURRENCY:-4}" \
    --pod-budget "${OME_EVAL_POD_BUDGET:-${OME_EVAL_EXCLUSIVE_PODS:-64}}" \
    --junit "results/${run_stamp}/junit-${lane}.xml" \
    "$@"
  test -s "results/${run_stamp}/junit-${lane}.xml"
  grep -q '<testcase' "results/${run_stamp}/junit-${lane}.xml"
)
