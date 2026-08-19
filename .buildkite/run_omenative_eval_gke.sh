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
cluster=$(printf 'ome-eval-%s-%s' "$feature" "$topology" | tr '_' '-')
images_dir="${OME_KIND_IMAGES_DIR:-/images}"
repo="${PROD_ECR_REGISTRY:-168653207203.dkr.ecr.us-east-2.amazonaws.com}/msl_infra"
run_stamp="${BUILDKITE_BUILD_NUMBER:-local}-${BUILDKITE_JOB_ID:-$$}"
results_root="hack/omenative-testing/perm/results/${run_stamp}"
diagnostics_dir="${results_root}/diagnostics"
tmp_dir=$(mktemp -d)
kubeconfig="${tmp_dir}/kubeconfig"

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
}

finish() {
  status=$?
  trap - EXIT
  set +e
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

echo "--- wait for dind ---"
i=0
until docker info >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -le 60 ] || { echo "Docker daemon did not become ready" >&2; exit 1; }
  sleep 2
done

echo "--- load KIND and exact runtime images ---"
for archive in kindest-node busybox ome-manager ome-agent canary-metrics; do
  path="${images_dir}/${archive}.tar"
  [ -s "$path" ] || { echo "missing image archive: $path" >&2; exit 1; }
  docker load -i "$path"
done

manager_ref="${repo}/ome-manager:${short_sha}"
agent_ref="${repo}/ome-agent:${short_sha}"
metrics_ref="${repo}/canary-metrics:${short_sha}"
docker image inspect "$manager_ref" >/dev/null
docker image inspect "$agent_ref" >/dev/null
docker image inspect "$metrics_ref" >/dev/null
docker tag "$manager_ref" "ome-manager:${short_sha}"
docker tag "$agent_ref" "ome-agent:${short_sha}"
docker tag "$metrics_ref" canary-metrics:kind-local

docker run --rm --privileged busybox sh -c '
  sysctl -w fs.inotify.max_user_watches=1048576
  sysctl -w fs.inotify.max_user_instances=8192
'

mkdir -p /ome-eval-model
chmod 0777 /ome-eval-model
cat >"${tmp_dir}/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: /ome-eval-model
        containerPath: /ome-eval-model
  - role: worker
    extraMounts:
      - hostPath: /ome-eval-model
        containerPath: /ome-eval-model
  - role: worker
    extraMounts:
      - hostPath: /ome-eval-model
        containerPath: /ome-eval-model
EOF

echo "--- create three-node KIND cluster ---"
kind create cluster --name "$cluster" --image kindest/node:v1.32.2 \
  --config "${tmp_dir}/kind.yaml" --wait 180s
kind get kubeconfig --name "$cluster" >"$kubeconfig"

export KUBECONFIG="$kubeconfig"
export OME_TEST_KUBECONFIG="$kubeconfig"
export OME_TEST_NS=sandbox
export OME_EVAL_KIND=1
export OME_TEST_ALLOW_CTRL_RESTART=1
export OME_EVAL_METRICS_IMAGE=canary-metrics:kind-local
export OME_KIND_OFFLINE=true
export OME_IMAGE_TAG="$short_sha"
export OME_RUN_TS="$run_stamp"

echo "--- deploy exact OME images ---"
bash hack/omenative-testing/deploy-ome-kind.sh --cluster "$cluster" --skip-build

echo "--- validate and run ${feature}/${topology} ---"
(
  cd hack/omenative-testing/perm
  python3 run.py --validate-only
  python3 batch.py \
    --conc "${OME_EVAL_CASE_CONCURRENCY:-4}" \
    --pod-budget "${OME_EVAL_POD_BUDGET:-${OME_EVAL_EXCLUSIVE_PODS:-64}}" \
    --junit "results/${run_stamp}/junit-${feature}-${topology}.xml" \
    "$@"
  test -s "results/${run_stamp}/junit-${feature}-${topology}.xml"
  grep -q '<testcase' "results/${run_stamp}/junit-${feature}-${topology}.xml"
)
