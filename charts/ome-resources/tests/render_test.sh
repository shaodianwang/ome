#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-resources chart test: $*" >&2
  exit 1
}

rendered="$("${helm_bin}" template ome-resources "${chart_dir}" --namespace ome)"
grep -Fq 'sizeLimit: 8Gi' <<<"${rendered}" ||
  fail "default Prometheus storage sizeLimit is not 8Gi"

overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.storage.sizeLimit=16Gi)"
grep -Fq 'sizeLimit: 16Gi' <<<"${overridden}" ||
  fail "Prometheus storage sizeLimit override was not rendered"
