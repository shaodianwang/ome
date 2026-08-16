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
controller_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${controller}" ||
  fail "controller chart version label was not rendered"
grep -Fq 'app.kubernetes.io/version: "1.16.0"' <<<"${controller}" ||
  fail "controller app version label was not rendered"
grep -Fq '"scaleUpPodBatchSize":100' <<<"${controller_config}" ||
  fail "default OMENative scale-up Pod batch size was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${controller_config}" ||
  fail "default OMENative scale-down Pod batch size was not rendered"
grep -Fq '"scaleDownRequeueInterval":"5s"' <<<"${controller_config}" ||
  fail "default OMENative scale-down requeue interval was not rendered"

scale_up_batch_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleUpPodBatchSize=37 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleUpPodBatchSize":37' <<<"${scale_up_batch_overridden}" ||
  fail "OMENative scale-up Pod batch size override was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_up_batch_overridden}" ||
  fail "scale-up override unexpectedly changed the scale-down Pod batch size"

scale_up_batch_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleUpPodBatchSize=0 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleUpPodBatchSize":0' <<<"${scale_up_batch_zero}" ||
  fail "explicit zero OMENative scale-up Pod batch size was not rendered"

scale_down_batch_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=41 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownPodBatchSize":41' <<<"${scale_down_batch_overridden}" ||
  fail "OMENative scale-down Pod batch size override was not rendered"
grep -Fq '"scaleUpPodBatchSize":100' <<<"${scale_down_batch_overridden}" ||
  fail "scale-down override unexpectedly changed the scale-up Pod batch size"

scale_down_batch_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=0 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownPodBatchSize":0' <<<"${scale_down_batch_zero}" ||
  fail "explicit zero OMENative scale-down Pod batch size was not rendered"

scale_down_batch_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'scaleDownPodBatchSize' <<<"${scale_down_batch_omitted}"; then
  fail "OMENative scale-down Pod batch size was rendered when omitted"
fi
grep -Fq '"scaleUpPodBatchSize":100' <<<"${scale_down_batch_omitted}" ||
  fail "omitting scale-down unexpectedly removed the scale-up Pod batch size"

scale_down_interval_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=37s \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownRequeueInterval":"37s"' <<<"${scale_down_interval_overridden}" ||
  fail "OMENative scale-down requeue interval override was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_down_interval_overridden}" ||
  fail "requeue interval override unexpectedly changed the scale-down Pod batch size"

scale_down_interval_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=0s \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownRequeueInterval":"0s"' <<<"${scale_down_interval_zero}" ||
  fail "explicit zero OMENative scale-down requeue interval was not rendered"

scale_down_interval_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownRequeueInterval=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'scaleDownRequeueInterval' <<<"${scale_down_interval_omitted}"; then
  fail "OMENative scale-down requeue interval was rendered when omitted"
fi
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_down_interval_omitted}" ||
  fail "omitting the requeue interval unexpectedly removed the scale-down Pod batch size"

default_controller_checksum="$(grep -m1 'checksum/config:' <<<"${controller}" | awk '{print $2}')"
scale_down_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=41 \
  --show-only templates/ome-controller/deployment.yaml)"
scale_down_controller_checksum="$(grep -m1 'checksum/config:' <<<"${scale_down_controller}" | awk '{print $2}')"
[[ -n "${default_controller_checksum}" && -n "${scale_down_controller_checksum}" ]] ||
  fail "controller ConfigMap checksum was not rendered"
[[ "${default_controller_checksum}" != "${scale_down_controller_checksum}" ]] ||
  fail "changing scale-down Pod batch size did not roll the controller checksum"

scale_down_interval_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=37s \
  --show-only templates/ome-controller/deployment.yaml)"
scale_down_interval_checksum="$(grep -m1 'checksum/config:' <<<"${scale_down_interval_controller}" | awk '{print $2}')"
[[ -n "${scale_down_interval_checksum}" ]] ||
  fail "scale-down requeue interval controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${scale_down_interval_checksum}" ]] ||
  fail "changing scale-down requeue interval did not roll the controller checksum"

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
