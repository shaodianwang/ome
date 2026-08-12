#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-resources chart test: $*" >&2
  exit 1
}

rendered="$("${helm_bin}" template ome-resources "${chart_dir}" --namespace ome)"
controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/deployment.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${controller}" ||
  fail "controller chart version label was not rendered"
grep -Fq 'app.kubernetes.io/version: "1.16.0"' <<<"${controller}" ||
  fail "controller app version label was not rendered"
model_agent="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set modelAgent.enabled=true \
  --show-only templates/model-agent-daemonset/daemonset.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${model_agent}" ||
  fail "model agent chart version label was not rendered"
grep -Fq 'sizeLimit: 8Gi' <<<"${rendered}" ||
  fail "default Prometheus storage sizeLimit is not 8Gi"
grep -Fq -- '--storage.tsdb.retention.size=6GB' <<<"${rendered}" ||
  fail "default Prometheus size retention is not 6GB"

overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.storage.sizeLimit=16Gi)"
grep -Fq 'sizeLimit: 16Gi' <<<"${overridden}" ||
  fail "Prometheus storage sizeLimit override was not rendered"

without_size_retention="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string prometheus.retentionSize=)"
if grep -Fq -- '--storage.tsdb.retention.size=' <<<"${without_size_retention}"; then
  fail "Prometheus size retention was rendered when disabled"
fi
