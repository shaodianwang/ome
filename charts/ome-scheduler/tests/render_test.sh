#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"
kube_version="1.35.0"

fail() {
  echo "ome-scheduler chart test: $*" >&2
  exit 1
}

assert_contains() {
  local needle="$1"
  grep -Fq -- "${needle}" <<<"${rendered}" || fail "render missing: ${needle}"
}

assert_absent() {
  local needle="$1"
  if grep -Fq -- "${needle}" <<<"${rendered}"; then
    fail "render unexpectedly contains: ${needle}"
  fi
}

"${helm_bin}" lint --strict --kube-version "${kube_version}" "${chart_dir}" >/dev/null
rendered="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system)"

# Embedded KubeSchedulerConfiguration contract. Helm cannot schema-check the
# opaque ConfigMap string, so keep indentation-sensitive assertions for its
# extension-point and plugin-argument structure.
assert_contains '          multiPoint:'
assert_contains '              - name: OMEGangPack'
assert_contains '          score:'
assert_contains '              - name: NodeResourcesFit'
assert_contains '                type: MostAllocated'
assert_contains '              - name: PodTopologySpread'
assert_contains '              defaultPermitTimeoutSeconds: 600'
assert_contains '              podGroupTopologyKeyAnnotation: "ome.io/topology-key"'
assert_contains '              unsupportedPlacementGroupLabel: "ome.io/placement-group"'
assert_contains '              podGroupSyncTimeoutSeconds: 30'
assert_contains '              gcIntervalSeconds: 60'

# Production deployment surface.
assert_contains 'kind: PodDisruptionBudget'
assert_contains 'name: ome-scheduler-metrics'
assert_contains 'path: /healthz'
assert_contains 'path: /readyz'
assert_contains 'requiredDuringSchedulingIgnoredDuringExecution:'
assert_contains 'topologyKey: kubernetes.io/hostname'
assert_contains 'whenUnsatisfiable: ScheduleAnyway'
assert_contains 'maxUnavailable: 1'
assert_contains 'maxSurge: 0'
assert_contains $'        resources:\n          limits:\n            cpu: "2"\n            memory: 12Gi\n          requests:\n            cpu: 200m\n            memory: 4Gi'

# Resource sizing remains configurable for constrained clusters.
custom_resources="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.resources.requests.memory=1Gi \
  --set scheduler.resources.limits.memory=2Gi)"
grep -Fq $'        resources:\n          limits:\n            cpu: "2"\n            memory: 2Gi\n          requests:\n            cpu: 200m\n            memory: 1Gi' \
  <<<"${custom_resources}" || fail "custom scheduler memory resources were not rendered"

# Core and volume permissions come from the cluster's version-managed roles;
# the chart-owned role is restricted to the distinct Lease/PodGroup contract.
assert_contains 'name: system:kube-scheduler'
assert_contains 'name: system:volume-scheduler'
assert_contains 'name: ome-scheduler-extras'
assert_contains 'resources: ["podgroups"]'
assert_absent 'resources: ["nodes"]'
assert_absent 'resources: ["pods"]'
assert_absent 'resources: ["persistentvolumeclaims", "persistentvolumes"]'

# Cross-field safety: replicated schedulers must use leader election.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=2 \
  --set scheduler.leaderElect=false >/dev/null 2>&1; then
  fail "unsafe HA configuration unexpectedly rendered"
fi

# The values schema must reject invalid behavioral timeouts.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.gcIntervalSeconds=0 >/dev/null 2>&1; then
  fail "zero GC interval unexpectedly passed values schema validation"
fi
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.name=Invalid_Name >/dev/null 2>&1; then
  fail "invalid scheduler resource/profile name unexpectedly passed values schema validation"
fi
too_long_name="$(printf '%056d' 0 | tr '0' 'a')"
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set "scheduler.name=${too_long_name}" >/dev/null 2>&1; then
  fail "overlong scheduler resource/profile name unexpectedly passed values schema validation"
fi

# The image contains kube-scheduler 1.35 APIs and is not a cross-minor binary.
for unsupported_version in 1.34.99 1.36.0; do
  if "${helm_bin}" template ome-scheduler "${chart_dir}" \
    --kube-version "${unsupported_version}" >/dev/null 2>&1; then
    fail "unsupported Kubernetes ${unsupported_version} unexpectedly rendered"
  fi
done

# OME's controller publishes a fixed topology annotation. Generic producers
# must explicitly disable that integration guard before selecting another key.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.podGroupTopologyKeyAnnotation=example.com/topology-key \
  >/dev/null 2>&1; then
  fail "OME integration accepted a mismatched PodGroup topology annotation"
fi
generic="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.omeControllerIntegration.enabled=false \
  --set scheduler.plugin.podGroupTopologyKeyAnnotation=example.com/topology-key)"
if ! grep -Fq 'podGroupTopologyKeyAnnotation: "example.com/topology-key"' <<<"${generic}"; then
  fail "generic producer annotation was not rendered after disabling OME integration"
fi

# A PDB cannot require every replica without blocking voluntary maintenance.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=1 \
  --set scheduler.podDisruptionBudget.minAvailable=1 >/dev/null 2>&1; then
  fail "blocking singleton PodDisruptionBudget unexpectedly rendered"
fi
"${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=1 \
  --set scheduler.podDisruptionBudget.minAvailable=0 >/dev/null

# Optional resources must be removable without leaving malformed YAML.
minimal="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system \
  --set scheduler.replicaCount=1 \
  --set scheduler.metrics.service.enabled=false \
  --set scheduler.podDisruptionBudget.enabled=false)"
if grep -Fq 'kind: PodDisruptionBudget' <<<"${minimal}"; then
  fail "disabled PodDisruptionBudget was rendered"
fi
if grep -Fq 'name: ome-scheduler-metrics' <<<"${minimal}"; then
  fail "disabled metrics Service was rendered"
fi
