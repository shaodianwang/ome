#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-podgroup-operator}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-${PROFILE}}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCHEDULER_DIR="${ROOT}/scheduler-plugin"

kubectl() {
  command kubectl --context "${KUBECTL_CONTEXT}" "$@"
}

minikube profile "${PROFILE}" >/dev/null

kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/master/manifests/coscheduling/crd.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/master/manifests/capacityscheduling/crd.yaml
kubectl apply -f "${SCHEDULER_DIR}/controller-rbac.yaml"

minikube -p "${PROFILE}" cp "${SCHEDULER_DIR}/sched-cc.yaml" /tmp/sched-cc.yaml
minikube -p "${PROFILE}" cp "${SCHEDULER_DIR}/kube-scheduler.yaml" /tmp/kube-scheduler.yaml

minikube -p "${PROFILE}" ssh -- 'if [ ! -f /etc/kubernetes/kube-scheduler.yaml.backup-podgroup-operator ]; then sudo cp /etc/kubernetes/manifests/kube-scheduler.yaml /etc/kubernetes/kube-scheduler.yaml.backup-podgroup-operator; fi'
minikube -p "${PROFILE}" ssh -- sudo mv /tmp/sched-cc.yaml /etc/kubernetes/sched-cc.yaml
minikube -p "${PROFILE}" ssh -- sudo mv /tmp/kube-scheduler.yaml /etc/kubernetes/manifests/kube-scheduler.yaml

kubectl -n kube-system wait --for=condition=Ready pod/kube-scheduler-"${PROFILE}" --timeout=120s
kubectl -n scheduler-plugins rollout status deployment/scheduler-plugins-controller --timeout=120s

kubectl auth can-i list podgroups.scheduling.x-k8s.io --as=system:kube-scheduler --all-namespaces
kubectl auth can-i list elasticquotas.scheduling.x-k8s.io --as=system:kube-scheduler --all-namespaces
kubectl auth can-i list podgroups.scheduling.x-k8s.io --as=system:serviceaccount:scheduler-plugins:scheduler-plugins-controller --all-namespaces
kubectl auth can-i list elasticquotas.scheduling.x-k8s.io --as=system:serviceaccount:scheduler-plugins:scheduler-plugins-controller --all-namespaces

kubectl -n kube-system get pod kube-scheduler-"${PROFILE}" -o jsonpath='{.spec.containers[0].image}{"\n"}'
