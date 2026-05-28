#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-podgroup-operator}"
IMAGE="${IMAGE:-podgroup-operator:latest}"
GPU_NODE_COUNT="${GPU_NODE_COUNT:-24}"
GPU_PER_NODE="${GPU_PER_NODE:-4}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-${PROFILE}}"
TARGETARCH="${TARGETARCH:-$(docker version --format '{{.Server.Arch}}')}"

kubectl() {
  command kubectl --context "${KUBECTL_CONTEXT}" "$@"
}

cd "$(dirname "$0")/.."

go test ./...

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT
CGO_ENABLED=0 GOOS=linux GOARCH="${TARGETARCH}" go build -o "${BUILD_DIR}/podgroup-operator" ./cmd/podgroup-operator
docker build -t "${IMAGE}" -f - "${BUILD_DIR}" <<'DOCKERFILE'
FROM scratch
COPY podgroup-operator /podgroup-operator
USER 65532:65532
ENTRYPOINT ["/podgroup-operator"]
DOCKERFILE

minikube start -p "${PROFILE}" --nodes="$((GPU_NODE_COUNT + 1))" --memory=4096 --cpus=4
minikube profile "${PROFILE}"
minikube -p "${PROFILE}" image load "${IMAGE}"

CONTROL_PLANE_NODE="$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "${CONTROL_PLANE_NODE}" ]]; then
  CONTROL_PLANE_NODE="${PROFILE}"
fi

kubectl taint node "${CONTROL_PLANE_NODE}" control-plane=true:NoSchedule --overwrite

GPU_NODES="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v -x "${CONTROL_PLANE_NODE}" | sort)"
ACTUAL_GPU_NODE_COUNT="$(printf "%s\n" "${GPU_NODES}" | sed '/^$/d' | wc -l | tr -d ' ')"
if [[ "${ACTUAL_GPU_NODE_COUNT}" != "${GPU_NODE_COUNT}" ]]; then
  echo "expected ${GPU_NODE_COUNT} GPU nodes, found ${ACTUAL_GPU_NODE_COUNT}" >&2
  exit 1
fi

echo "Configuring ${GPU_NODE_COUNT} GB300 GPU nodes with mock nvidia.com/gpu=${GPU_PER_NODE} capacity..."
gpu_index=0
while IFS= read -r node; do
  if [[ -z "${node}" ]]; then
    continue
  fi
  gpu_index=$((gpu_index + 1))
  nvlink_domain=$(((gpu_index - 1) / 8 + 1))

  kubectl label node "${node}" \
    nvidia.com/gpu=true \
    ds.coreweave.com/nvlink.domain="${nvlink_domain}" \
    --overwrite
  kubectl taint node "${node}" nvidia.com/gpu=true:NoSchedule --overwrite
  kubectl patch node "${node}" --subresource=status --type=merge \
    -p "{\"status\":{\"capacity\":{\"nvidia.com/gpu\":\"${GPU_PER_NODE}\"},\"allocatable\":{\"nvidia.com/gpu\":\"${GPU_PER_NODE}\"}}}"
done <<<"${GPU_NODES}"

kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/pgdr-crd.yaml
kubectl apply -f manifests/rbac.yaml
kubectl apply -f manifests/deployment.yaml
kubectl apply -f examples/namespace.yaml

KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" ./scheduler-plugin/install-minikube.sh
kubectl apply -f examples/gpu-1-podgroupdeployment.yaml

cat <<'MSG'

Scheduler prerequisite:
  scheduler-plugin/install-minikube.sh replaces the Minikube default scheduler
  with scheduler-plugins Coscheduling enabled for the default-scheduler profile.

Inspect with:
  kubectl --context podgroup-operator -n podgroup-demo get podgroupdeployments,podgroups,pods,svc -o wide
  kubectl --context podgroup-operator -n kube-system logs deploy/podgroup-operator -f
MSG
