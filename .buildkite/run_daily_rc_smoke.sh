#!/bin/sh
# One-replica CPU smoke test against the Argo-managed OME RC control plane.
set -eu

SCRIPT_DIR="$(CDPATH='' cd "$(dirname "$0")" && pwd)"
# shellcheck source=.buildkite/daily_rc_common.sh
. "$SCRIPT_DIR/daily_rc_common.sh"

NAMESPACE="${DAILY_RC_SMOKE_NAMESPACE:-omenative-eval}"
SMOKE_NAME="${DAILY_RC_SMOKE_NAME:-ome-rc-daily-smoke}"
ARTIFACT_DIR="artifacts/daily-rc-smoke"
REPORT="$ARTIFACT_DIR/report.txt"
mkdir -p "$ARTIFACT_DIR"
: >"$REPORT"

case "$NAMESPACE" in
  ""|*[!a-z0-9-]*) echo "invalid smoke namespace: $NAMESPACE" >&2; exit 1 ;;
esac
case "$SMOKE_NAME" in
  ""|*[!a-z0-9-]*) echo "invalid smoke name: $SMOKE_NAME" >&2; exit 1 ;;
esac

buildkite-agent meta-data get daily-candidate-b64 \
  | base64 -d >"$ARTIFACT_DIR/candidate.json"
daily_rc_load_candidate "$ARTIFACT_DIR/candidate.json"
EXPECTED_CRD_CHART="$(jq -er '.charts["ome-crd"]' "$ARTIFACT_DIR/candidate.json")"

daily_rc_setup_kubeconfig "$NAMESPACE"

record() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

cleanup_resources() {
  # Delete only the fixed, concurrency-protected smoke fixture. The controller
  # owns and garbage-collects all child workloads.
  kubectl -n "$NAMESPACE" delete inferenceservice "$SMOKE_NAME" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete basemodel "$SMOKE_NAME" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete servingruntime "$SMOKE_NAME" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete job "${SMOKE_NAME}-seed" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete persistentvolumeclaim "${SMOKE_NAME}-model" \
    --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
}

finish() {
  status=$?
  trap - EXIT INT TERM
  set +e

  echo "--- RC smoke final state"
  kubectl -n "$NAMESPACE" get basemodel,servingruntime,inferenceservice,pvc,job,pod \
    -l "ome.io/rc-smoke=${SMOKE_NAME}" -o wide 2>&1 || true
  kubectl -n "$NAMESPACE" get job,pod \
    -l "models.ome/model-name=${SMOKE_NAME}" -o wide 2>&1 || true
  kubectl -n "$NAMESPACE" get pod \
    -l "ome.io/inferenceservice=${SMOKE_NAME}" -o wide 2>&1 || true
  if [ "$status" -ne 0 ]; then
    kubectl -n "$NAMESPACE" describe basemodel "$SMOKE_NAME" 2>&1 || true
    kubectl -n "$NAMESPACE" describe inferenceservice "$SMOKE_NAME" 2>&1 || true
    for pod in $(kubectl -n "$NAMESPACE" get pod \
      -l "ome.io/rc-smoke=${SMOKE_NAME}" -o name 2>/dev/null); do
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=100 2>&1 || true
    done
    for pod in $(kubectl -n "$NAMESPACE" get pod \
      -l "models.ome/model-name=${SMOKE_NAME}" -o name 2>/dev/null); do
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=100 2>&1 || true
    done
    for pod in $(kubectl -n "$NAMESPACE" get pod \
      -l "ome.io/inferenceservice=${SMOKE_NAME}" -o name 2>/dev/null); do
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=100 2>&1 || true
    done
  fi

  echo "--- clean up RC smoke fixture"
  cleanup_resources
  REPORT_B64="$(base64 <"$REPORT" | tr -d '\n')"
  buildkite-agent meta-data set daily-rc-smoke-b64 "$REPORT_B64" 2>/dev/null || true
  unset REPORT_B64
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

echo "--- verify scoped smoke permissions"
for resource in persistentvolumeclaims jobs.batch basemodels.ome.io servingruntimes.ome.io inferenceservices.ome.io; do
  require_permission get "$resource"
  require_permission create "$resource"
  require_permission delete "$resource"
done

echo "--- wait for RC to run the discovered release"
record "candidate-chart: ome-crd=$EXPECTED_CRD_CHART"
record "candidate-chart: ome-resources=$EXPECTED_RESOURCES_CHART"
record "candidate-chart: ome-scheduler=$EXPECTED_SCHEDULER_CHART"
daily_rc_wait_for_live_release 900
record "release-images: manager=$live_manager scheduler=$live_scheduler agent=$live_agent"
record "release-charts: resources=$live_resources_chart scheduler=$live_scheduler_chart"

echo "--- remove any stale fixed-name fixture"
cleanup_resources

echo "--- seed a tiny PVC-backed model"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${SMOKE_NAME}-model
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: ome-rc-smoke
    ome.io/rc-smoke: ${SMOKE_NAME}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: dynamic-rwo
  resources:
    requests:
      storage: 4Gi
---
apiVersion: batch/v1
kind: Job
metadata:
  name: ${SMOKE_NAME}-seed
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: ome-rc-smoke
    ome.io/rc-smoke: ${SMOKE_NAME}
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 900
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: ome-rc-smoke
        ome.io/rc-smoke: ${SMOKE_NAME}
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/arch: amd64
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: seed
          image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/nginxinc/nginx-unprivileged:1.27-alpine
          command: [sh, -c]
          args:
            - |
              mkdir -p /seed/model
              cat > /seed/model/config.json <<'JSON'
              {"architectures":["TestArchitecture"],"model_type":"test","hidden_size":1,"num_attention_heads":1,"num_hidden_layers":1,"vocab_size":1,"torch_dtype":"float32"}
              JSON
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {cpu: 100m, memory: 64Mi}
          volumeMounts:
            - {name: model, mountPath: /seed}
      volumes:
        - name: model
          persistentVolumeClaim: {claimName: ${SMOKE_NAME}-model}
EOF
kubectl -n "$NAMESPACE" wait --for=condition=complete "job/${SMOKE_NAME}-seed" --timeout=5m
record "seed-job: complete"

echo "--- exercise BaseModel metadata extraction with ome-agent"
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: ${SMOKE_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: ome-rc-smoke
    ome.io/rc-smoke: ${SMOKE_NAME}
spec:
  modelArchitecture: TestArchitecture
  modelFormat: {name: noop, operator: Equal, version: "1.0.0", weight: 1}
  modelFramework: {name: noop, operator: Equal, weight: 1}
  modelParameterSize: "0"
  modelType: test
  storage:
    storageUri: pvc://${SMOKE_NAME}-model/model
EOF

deadline=$(( $(date +%s) + 180 ))
metadata_job=""
while [ -z "$metadata_job" ]; do
  metadata_job="$(kubectl -n "$NAMESPACE" get jobs \
    -l "models.ome/model-name=${SMOKE_NAME},models.ome/model-scope=namespaced" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -z "$metadata_job" ] && [ "$(date +%s)" -ge "$deadline" ]; then
    echo "OME did not create the BaseModel metadata Job within 3 minutes" >&2
    exit 1
  fi
  [ -n "$metadata_job" ] || sleep 5
done
metadata_image="$(kubectl -n "$NAMESPACE" get job "$metadata_job" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="model-metadata")].image}')"
if [ "$metadata_image" != "$EXPECTED_AGENT" ]; then
  echo "metadata Job image mismatch: expected $EXPECTED_AGENT, got $metadata_image" >&2
  exit 1
fi
kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$metadata_job" --timeout=5m

deadline=$(( $(date +%s) + 180 ))
while [ "$(kubectl -n "$NAMESPACE" get basemodel "$SMOKE_NAME" -o jsonpath='{.status.state}' 2>/dev/null || true)" != "Ready" ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "BaseModel did not become Ready within 3 minutes" >&2
    exit 1
  fi
  sleep 5
done
record "ome-agent: job=$metadata_job image=$metadata_image basemodel=Ready"

echo "--- create one CPU OMENative replica through ome-scheduler"
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: ${SMOKE_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: ome-rc-smoke
    ome.io/rc-smoke: ${SMOKE_NAME}
spec:
  disabled: false
  protocolVersions: [openAI]
  schedulerName: ome-scheduler
  supportedModelFormats:
    - autoSelect: false
      modelArchitecture: TestArchitecture
      modelFormat: {name: noop, operator: Equal, version: "1.0.0", weight: 1}
      modelFramework: {name: noop, operator: Equal, weight: 1}
      priority: 1
  engineConfig:
    nodeSelector: {kubernetes.io/arch: amd64}
    runner:
      name: ome-container
      image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/nginxinc/nginx-unprivileged:1.27-alpine
      command: [sh, -c, "sed 's/^worker_processes.*/worker_processes 2;/' /etc/nginx/nginx.conf > /tmp/nginx.conf && exec nginx -c /tmp/nginx.conf -g 'daemon off;'"]
      ports: [{containerPort: 8080, name: http, protocol: TCP}]
      readinessProbe: {httpGet: {path: /, port: 8080}, periodSeconds: 2, failureThreshold: 5}
      livenessProbe: {httpGet: {path: /, port: 8080}, periodSeconds: 5, failureThreshold: 3}
      resources:
        requests: {cpu: 25m, memory: 32Mi}
        limits: {cpu: 200m, memory: 128Mi}
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: ${SMOKE_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: ome-rc-smoke
    ome.io/rc-smoke: ${SMOKE_NAME}
spec:
  model: {apiGroup: ome.io, kind: BaseModel, name: ${SMOKE_NAME}}
  runtime: {apiGroup: ome.io, kind: ServingRuntime, name: ${SMOKE_NAME}, autoSync: true}
  engine:
    minReplicas: 1
    maxReplicas: 1
    annotations: {ome.io/deploymentMode: OMENative}
EOF

deadline=$(( $(date +%s) + 300 ))
workload_pods=""
while [ -z "$workload_pods" ]; do
  workload_pods="$(kubectl -n "$NAMESPACE" get pods \
    -l "ome.io/inferenceservice=${SMOKE_NAME}" -o name 2>/dev/null || true)"
  if [ -z "$workload_pods" ] && [ "$(date +%s)" -ge "$deadline" ]; then
    echo "OME did not create an InferenceService pod within 5 minutes" >&2
    exit 1
  fi
  [ -n "$workload_pods" ] || sleep 5
done
for pod in $workload_pods; do
  scheduler_name="$(kubectl -n "$NAMESPACE" get "$pod" -o jsonpath='{.spec.schedulerName}')"
  if [ "$scheduler_name" != "ome-scheduler" ]; then
    echo "$pod uses scheduler $scheduler_name, expected ome-scheduler" >&2
    exit 1
  fi
done
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod \
  -l "ome.io/inferenceservice=${SMOKE_NAME}" --timeout=10m
kubectl -n "$NAMESPACE" wait --for=condition=Ready \
  "inferenceservice/${SMOKE_NAME}" --timeout=5m
record "inferenceservice: Ready replicas=1 scheduler=ome-scheduler pods=$(printf '%s' "$workload_pods" | tr '\n' ',')"
daily_rc_assert_live_release_matches_candidate
record "release-images-final: manager=$live_manager scheduler=$live_scheduler agent=$live_agent"
record "release-charts-final: resources=$live_resources_chart scheduler=$live_scheduler_chart"
record "result: PASS"
