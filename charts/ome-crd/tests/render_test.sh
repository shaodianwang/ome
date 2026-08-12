#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-crd chart test: $*" >&2
  exit 1
}

"${helm_bin}" lint --strict "${chart_dir}" >/dev/null
rendered="$("${helm_bin}" template ome-crd "${chart_dir}" --namespace ome)"

grep -Fq 'helm.sh/chart: ome-crd-0.1.0' <<<"${rendered}" ||
  fail "release marker chart version label was not rendered"
grep -Fq 'name: ome-crd-release' <<<"${rendered}" ||
  fail "release marker ConfigMap was not rendered"
grep -Fq 'chart-version: "0.1.0"' <<<"${rendered}" ||
  fail "release marker chart version was not rendered"
grep -Fq 'app-version: "1.16.0"' <<<"${rendered}" ||
  fail "release marker app version was not rendered"
