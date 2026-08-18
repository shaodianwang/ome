#!/bin/sh
# Exercise one production-shaped, two-node TPU gang in the RC cluster.
set -eu

SCRIPT_DIR="$(CDPATH='' cd "$(dirname "$0")" && pwd)"
# shellcheck source=.buildkite/daily_rc_common.sh
. "$SCRIPT_DIR/daily_rc_common.sh"

NAMESPACE="omenative-eval"
SMOKE_NAME="${DAILY_RC_TPU_SMOKE_NAME:-ome-rc-tpu-smoke}"
RUNTIME_NAME="${DAILY_RC_TPU_RUNTIME_NAME:-ome-rc-tpu-smoke}"
FIXTURE="${DAILY_RC_TPU_FIXTURE:-.buildkite/fixtures/rc-tpu-smoke-isvc.yaml}"
EXPECTED_ENGINE_SCHEDULER="${DAILY_RC_TPU_EXPECTED_ENGINE_SCHEDULER:-default-scheduler}"
METADATA_KEY="${DAILY_RC_TPU_METADATA_KEY:-daily-rc-tpu-smoke-b64}"
READY_TIMEOUT_SECONDS="${DAILY_RC_TPU_READY_TIMEOUT_SECONDS:-14400}"
READINESS_INTERVAL="${DAILY_RC_TPU_READINESS_INTERVAL:-15}"
MONITOR_INTERVAL="${DAILY_RC_TPU_MONITOR_INTERVAL:-60}"
ARTIFACT_DIR="artifacts/${SMOKE_NAME}"
REPORT="$ARTIFACT_DIR/report.txt"
MONITOR_LOG="$ARTIFACT_DIR/monitor.log"
mkdir -p "$ARTIFACT_DIR"
: >"$REPORT"
: >"$MONITOR_LOG"

case "$SMOKE_NAME" in
  ""|*[!a-z0-9-]*) echo "invalid smoke name: $SMOKE_NAME" >&2; exit 1 ;;
esac
case "$EXPECTED_ENGINE_SCHEDULER" in
  ""|*[!a-z0-9.-]*) echo "invalid expected engine scheduler: $EXPECTED_ENGINE_SCHEDULER" >&2; exit 1 ;;
esac
if [ ! -f "$FIXTURE" ]; then
  echo "TPU smoke fixture does not exist: $FIXTURE" >&2
  exit 1
fi
case "$READY_TIMEOUT_SECONDS" in
  ""|*[!0-9]*) echo "invalid ready timeout: $READY_TIMEOUT_SECONDS" >&2; exit 1 ;;
esac
case "$READINESS_INTERVAL" in
  ""|*[!0-9]*) echo "invalid readiness interval: $READINESS_INTERVAL" >&2; exit 1 ;;
esac
case "$MONITOR_INTERVAL" in
  ""|*[!0-9]*) echo "invalid monitor interval: $MONITOR_INTERVAL" >&2; exit 1 ;;
esac

buildkite-agent meta-data get daily-candidate-b64 \
  | base64 -d >"$ARTIFACT_DIR/candidate.json"
daily_rc_load_candidate "$ARTIFACT_DIR/candidate.json"
daily_rc_setup_kubeconfig "$NAMESPACE"

record() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

MONITOR_PID=""

stop_monitor() {
  if [ -n "$MONITOR_PID" ]; then
    kill "$MONITOR_PID" >/dev/null 2>&1 || true
    wait "$MONITOR_PID" 2>/dev/null || true
    MONITOR_PID=""
  fi
}

cleanup_fixture() {
  kubectl -n "$NAMESPACE" delete inferenceservice "$SMOKE_NAME" \
    --ignore-not-found --wait=true --timeout=10m >/dev/null
}

collect_state() {
  kubectl -n "$NAMESPACE" get inferenceservice "$SMOKE_NAME" -o wide 2>&1 || true
  kubectl -n "$NAMESPACE" get inferencereplicas \
    -l "ome.io/inferenceservice=${SMOKE_NAME}" -o wide 2>&1 || true
  kubectl -n "$NAMESPACE" get leaderworkersets,podgroups \
    -l "ome.io/inferenceservice=${SMOKE_NAME}" -o wide 2>&1 || true
  kubectl -n "$NAMESPACE" get pods \
    -l "ome.io/inferenceservice=${SMOKE_NAME}" -o wide 2>&1 || true
}

finish() {
  status=$?
  trap - EXIT INT TERM
  set +e
  stop_monitor

  echo "--- RC TPU smoke final state"
  collect_state | tee -a "$ARTIFACT_DIR/final-state.log"
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp \
    2>&1 | tail -200 >"$ARTIFACT_DIR/events.log" || true

  if [ "$status" -ne 0 ]; then
    kubectl -n "$NAMESPACE" describe inferenceservice "$SMOKE_NAME" \
      >"$ARTIFACT_DIR/isvc-describe.log" 2>&1 || true
    kubectl -n "$NAMESPACE" describe inferencereplicas \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" \
      >"$ARTIFACT_DIR/irep-describe.log" 2>&1 || true
    for pod in $(kubectl -n "$NAMESPACE" get pods \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" -o name 2>/dev/null); do
      pod_name="${pod#pod/}"
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=300 \
        >"$ARTIFACT_DIR/${pod_name}.log" 2>&1 || true
    done
  fi

  echo "--- clean up RC TPU smoke fixture"
  cleanup_fixture || true
  REPORT_B64="$(base64 <"$REPORT" | tr -d '\n')"
  buildkite-agent meta-data set "$METADATA_KEY" "$REPORT_B64" 2>/dev/null || true
  unset REPORT_B64
  buildkite-agent artifact upload "$ARTIFACT_DIR/*" 2>/dev/null || true
  daily_rc_cleanup_kubeconfig
  exit "$status"
}
trap finish EXIT INT TERM

require_permission() {
  verb="$1"
  resource="$2"
  if [ "$(kubectl auth can-i "$verb" "$resource" -n "$NAMESPACE" 2>/dev/null)" != "yes" ]; then
    echo "missing permission: $verb $resource in $NAMESPACE" >&2
    exit 1
  fi
}

monitor_state() {
  while :; do
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ready="$(kubectl -n "$NAMESPACE" get inferenceservice "$SMOKE_NAME" \
      -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}:{.reason}{end}' \
      2>/dev/null || true)"
    printf '\n[%s] Ready=%s\n' "$now" "${ready:-not-reported}"
    kubectl -n "$NAMESPACE" get inferencereplicas \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" \
      -o custom-columns='NAME:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas,UPDATED:.status.updatedReplicas' \
      2>&1 || true
    kubectl -n "$NAMESPACE" get pods \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" \
      -o custom-columns='NAME:.metadata.name,ROLE:.metadata.labels.ome\.io/runner,NODE:.spec.nodeName,READY:.status.containerStatuses[*].ready,PHASE:.status.phase' \
      2>&1 || true
    sleep "$MONITOR_INTERVAL"
  done
}

wait_for_production_ready() {
  deadline=$((START_EPOCH + READY_TIMEOUT_SECONDS))
  while :; do
    daily_rc_fail_if_new_release_detected
    pods_json="$(kubectl -n "$NAMESPACE" get pods \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" -o json)"
    engine_pods="$(printf '%s' "$pods_json" | jq \
      '[.items[] | select(.metadata.labels.component == "engine")] | length')"
    ready_engine_pods="$(printf '%s' "$pods_json" | jq \
      '[.items[] | select(.metadata.labels.component == "engine") | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')"
    router_pods="$(printf '%s' "$pods_json" | jq \
      '[.items[] | select(.metadata.labels.component == "router")] | length')"
    ready_router_pods="$(printf '%s' "$pods_json" | jq \
      '[.items[] | select(.metadata.labels.component == "router") | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')"
    engine_ready_replicas="$(kubectl -n "$NAMESPACE" get inferencereplica \
      "${SMOKE_NAME}-engine" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"

    if [ "$engine_pods" -eq 2 ] && [ "$ready_engine_pods" -eq 2 ] \
      && [ "$router_pods" -ge 1 ] && [ "$ready_router_pods" -eq "$router_pods" ] \
      && [ "${engine_ready_replicas:-0}" -eq 1 ]; then
      return 0
    fi

    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out waiting for production readiness: engine=${ready_engine_pods}/${engine_pods} router=${ready_router_pods}/${router_pods} engine-ready-replicas=${engine_ready_replicas:-0}" >&2
      return 1
    fi
    sleep "$READINESS_INTERVAL"
  done
}

echo "--- verify scoped TPU smoke permissions"
for resource in inferenceservices.ome.io servingruntimes.ome.io configmaps pods; do
  require_permission get "$resource"
done
require_permission create inferenceservices.ome.io
require_permission delete inferenceservices.ome.io

echo "--- verify Argo-managed TPU prerequisites"
kubectl -n "$NAMESPACE" get configmap gcs-rapid-reader >/dev/null
kubectl -n "$NAMESPACE" get servingruntime "$RUNTIME_NAME" >/dev/null
record "prerequisites: runtime=$RUNTIME_NAME gcs-config=gcs-rapid-reader"
echo "--- wait for RC to run the discovered release"
daily_rc_wait_for_live_release 900
record "release-images: manager=$live_manager scheduler=$live_scheduler agent=$live_agent"
record "release-charts: resources=$live_resources_chart scheduler=$live_scheduler_chart"

echo "--- remove any stale fixed-name TPU fixture"
cleanup_fixture

echo "--- create one two-node TPU gang"
START_EPOCH="$(date +%s)"
START_RFC3339="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl apply -f "$FIXTURE"
record "started-at: $START_RFC3339"

monitor_state 2>&1 | tee -a "$MONITOR_LOG" &
MONITOR_PID=$!
# InferenceService Ready has previously become True while the engine pods were
# still PodInitializing. Gate the smoke test on the serving workloads instead:
# the two TPU engine pods, router, and engine InferenceReplica must all agree.
wait_for_production_ready
stop_monitor

# Status should also project the workload readiness once the pods are serving.
kubectl -n "$NAMESPACE" wait --for=condition=Ready \
  "inferenceservice/${SMOKE_NAME}" --timeout=2m

END_EPOCH="$(date +%s)"
ELAPSED_SECONDS=$((END_EPOCH - START_EPOCH))

ENGINE_PODS_JSON="$(kubectl -n "$NAMESPACE" get pods \
  -l "ome.io/inferenceservice=${SMOKE_NAME},component=engine" -o json)"
ENGINE_PODS="$(printf '%s' "$ENGINE_PODS_JSON" | jq '.items | length')"
LEADERS="$(printf '%s' "$ENGINE_PODS_JSON" | jq '[.items[] | select(.metadata.labels["ome.io/runner"] == "leader")] | length')"
WORKERS="$(printf '%s' "$ENGINE_PODS_JSON" | jq '[.items[] | select(.metadata.labels["ome.io/runner"] == "worker")] | length')"
TPU_CONTAINERS="$(printf '%s' "$ENGINE_PODS_JSON" | jq '[.items[].spec.containers[] | select(.name == "ome-container" and .resources.requests["google.com/tpu"] == "4")] | length')"
if [ "$ENGINE_PODS" -ne 2 ] || [ "$LEADERS" -ne 1 ] \
  || [ "$WORKERS" -ne 1 ] || [ "$TPU_CONTAINERS" -ne 2 ]; then
  echo "unexpected TPU gang shape: pods=$ENGINE_PODS leaders=$LEADERS workers=$WORKERS tpu-containers=$TPU_CONTAINERS" >&2
  exit 1
fi

ENGINE_SCHEDULERS="$(printf '%s' "$ENGINE_PODS_JSON" | jq -r '[.items[].spec.schedulerName] | unique | join(",")')"
ROUTER_SCHEDULERS="$(kubectl -n "$NAMESPACE" get pods \
  -l "ome.io/inferenceservice=${SMOKE_NAME},component=router" -o json \
  | jq -r '[.items[].spec.schedulerName] | unique | join(",")')"
if [ "$ENGINE_SCHEDULERS" != "$EXPECTED_ENGINE_SCHEDULER" ]; then
  echo "unexpected engine scheduler: got=$ENGINE_SCHEDULERS expected=$EXPECTED_ENGINE_SCHEDULER" >&2
  exit 1
fi
if [ "$ROUTER_SCHEDULERS" != "default-scheduler" ]; then
  echo "unexpected router scheduler: got=$ROUTER_SCHEDULERS expected=default-scheduler" >&2
  exit 1
fi

daily_rc_assert_live_release_matches_candidate
record "release-images-final: manager=$live_manager scheduler=$live_scheduler agent=$live_agent"
record "release-charts-final: resources=$live_resources_chart scheduler=$live_scheduler_chart"
record "gang: Ready engine-pods=2 leaders=1 workers=1 tpu-per-pod=4 engine-scheduler=$ENGINE_SCHEDULERS router-scheduler=$ROUTER_SCHEDULERS"
record "time-to-ready-seconds: $ELAPSED_SECONDS"
record "result: PASS"
