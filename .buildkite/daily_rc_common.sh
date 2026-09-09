#!/bin/sh
# Shared release and Kubernetes helpers for RC smoke-test steps.

daily_rc_load_candidate() {
  candidate_file="$1"
  EXPECTED_CHART_REVISION=""
  EXPECTED_MANAGER="$(jq -er '.images["ome-manager"]' "$candidate_file")"
  EXPECTED_AGENT="$(jq -er '.images["ome-agent"]' "$candidate_file")"
  EXPECTED_SCHEDULER="$(jq -er '.images["ome-scheduler"]' "$candidate_file")"
  EXPECTED_RESOURCES_CHART="$(jq -er '.charts["ome-resources"]' "$candidate_file")"
  EXPECTED_SCHEDULER_CHART="$(jq -er '.charts["ome-scheduler"]' "$candidate_file")"
}

daily_rc_kargo_build_enabled() {
  case "${KARGO_BUILD:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

daily_rc_load_kargo_candidate() {
  requested_commit="$1"
  case "$requested_commit" in
    ""|*[!0-9a-f]*)
      echo "KARGO_BUILD_COMMIT must be a lowercase hexadecimal commit ID" >&2
      return 1
      ;;
  esac
  if [ "${#requested_commit}" -lt 8 ] || [ "${#requested_commit}" -gt 64 ]; then
    echo "KARGO_BUILD_COMMIT must contain between 8 and 64 characters" >&2
    return 1
  fi

  EXPECTED_CHART_REVISION="$(printf '%.8s' "$requested_commit")"
  registry="${PROD_ECR_REGISTRY:-168653207203.dkr.ecr.us-east-2.amazonaws.com}"
  image_prefix="${OME_IMAGE_REPOSITORY_PREFIX:-msl_infra}"
  EXPECTED_MANAGER="${registry}/${image_prefix}/ome-manager:${EXPECTED_CHART_REVISION}"
  EXPECTED_AGENT="${registry}/${image_prefix}/ome-agent:${EXPECTED_CHART_REVISION}"
  EXPECTED_SCHEDULER="${registry}/${image_prefix}/ome-scheduler:${EXPECTED_CHART_REVISION}"
  EXPECTED_RESOURCES_CHART="revision ${EXPECTED_CHART_REVISION}"
  EXPECTED_SCHEDULER_CHART="revision ${EXPECTED_CHART_REVISION}"
}

daily_rc_load_requested_candidate() {
  candidate_file="$1"
  if daily_rc_kargo_build_enabled; then
    daily_rc_load_kargo_candidate "${KARGO_BUILD_COMMIT:?KARGO_BUILD_COMMIT is required when KARGO_BUILD is enabled}"
    return
  fi

  buildkite-agent meta-data get daily-candidate-b64 \
    | base64 -d >"$candidate_file"
  daily_rc_load_candidate "$candidate_file"
}

daily_rc_setup_kubeconfig() {
  namespace="$1"
  sa_dir="/var/run/secrets/kubernetes.io/serviceaccount"
  test -s "$sa_dir/token"
  test -s "$sa_dir/ca.crt"
  test -n "${KUBERNETES_SERVICE_HOST:-}"
  test -n "${KUBERNETES_SERVICE_PORT:-}"

  case "$KUBERNETES_SERVICE_HOST" in
    *:*) api_server="https://[${KUBERNETES_SERVICE_HOST}]:${KUBERNETES_SERVICE_PORT}" ;;
    *) api_server="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}" ;;
  esac

  DAILY_RC_KUBE_TMP="$(mktemp -d)"
  KUBECONFIG="$DAILY_RC_KUBE_TMP/kubeconfig"
  export KUBECONFIG
  cat >"$KUBECONFIG" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: rc
    cluster:
      server: ${api_server}
      certificate-authority: ${sa_dir}/ca.crt
users:
  - name: daily-rc
    user:
      tokenFile: ${sa_dir}/token
contexts:
  - name: daily-rc
    context:
      cluster: rc
      user: daily-rc
      namespace: ${namespace}
current-context: daily-rc
EOF
  chmod 600 "$KUBECONFIG"
}

daily_rc_cleanup_kubeconfig() {
  if [ -n "${DAILY_RC_KUBE_TMP:-}" ] && [ -d "$DAILY_RC_KUBE_TMP" ]; then
    rm -rf -- "$DAILY_RC_KUBE_TMP"
  fi
  DAILY_RC_KUBE_TMP=""
}

daily_rc_read_live_release() {
  manager_json="$(kubectl -n ome get deployment ome-controller-manager -o json 2>/dev/null || true)"
  scheduler_json="$(kubectl -n ome get deployment ome-scheduler -o json 2>/dev/null || true)"
  live_manager="$(printf '%s' "$manager_json" | jq -r '.spec.template.spec.containers[0].image // empty' 2>/dev/null || true)"
  live_scheduler="$(printf '%s' "$scheduler_json" | jq -r '.spec.template.spec.containers[0].image // empty' 2>/dev/null || true)"
  live_agent="$(kubectl -n ome get configmap inferenceservice-config -o json 2>/dev/null \
    | jq -r '.data.omeAgent | fromjson | .image' 2>/dev/null || true)"
  manager_rollout_ready="$(printf '%s' "$manager_json" | jq -r '
    (.spec.replicas // 1) as $desired |
    (.status.observedGeneration // 0) >= .metadata.generation and
    (.status.updatedReplicas // 0) == $desired and
    (.status.availableReplicas // 0) == $desired and
    (.status.replicas // 0) == $desired' 2>/dev/null || true)"
  scheduler_rollout_ready="$(printf '%s' "$scheduler_json" | jq -r '
    (.spec.replicas // 1) as $desired |
    (.status.observedGeneration // 0) >= .metadata.generation and
    (.status.updatedReplicas // 0) == $desired and
    (.status.availableReplicas // 0) == $desired and
    (.status.replicas // 0) == $desired' 2>/dev/null || true)"
  live_resources_chart="$(printf '%s' "$manager_json" | jq -r '.metadata.labels["helm.sh/chart"] // empty | sub("^ome-resources-"; "")' 2>/dev/null || true)"
  live_scheduler_chart="$(printf '%s' "$scheduler_json" | jq -r '.metadata.labels["helm.sh/chart"] // empty | sub("^ome-scheduler-"; "")' 2>/dev/null || true)"
}

daily_rc_live_release_matches_candidate() {
  daily_rc_live_release_identity_matches_candidate \
    && [ "$manager_rollout_ready" = true ] \
    && [ "$scheduler_rollout_ready" = true ]
}

daily_rc_live_release_identity_matches_candidate() {
  [ "$live_manager" = "$EXPECTED_MANAGER" ] \
    && [ "$live_scheduler" = "$EXPECTED_SCHEDULER" ] \
    && [ "$live_agent" = "$EXPECTED_AGENT" ] \
    && daily_rc_chart_matches_candidate "$live_resources_chart" "$EXPECTED_RESOURCES_CHART" \
    && daily_rc_chart_matches_candidate "$live_scheduler_chart" "$EXPECTED_SCHEDULER_CHART"
}

daily_rc_chart_matches_candidate() {
  live_chart="$1"
  expected_chart="$2"
  if [ -z "${EXPECTED_CHART_REVISION:-}" ]; then
    [ "$live_chart" = "$expected_chart" ]
    return
  fi

  case "$live_chart" in
    *.g"$EXPECTED_CHART_REVISION"|*."$EXPECTED_CHART_REVISION") return 0 ;;
    *) return 1 ;;
  esac
}

daily_rc_print_release_mismatch() {
  printf 'expected manager=%s scheduler=%s agent=%s\n' \
    "$EXPECTED_MANAGER" "$EXPECTED_SCHEDULER" "$EXPECTED_AGENT" >&2
  printf 'live manager=%s scheduler=%s agent=%s\n' \
    "$live_manager" "$live_scheduler" "$live_agent" >&2
  printf 'expected charts: resources=%s scheduler=%s\n' \
    "$EXPECTED_RESOURCES_CHART" "$EXPECTED_SCHEDULER_CHART" >&2
  printf 'live charts: resources=%s scheduler=%s\n' \
    "$live_resources_chart" "$live_scheduler_chart" >&2
  printf 'rollouts complete: manager=%s scheduler=%s\n' \
    "$manager_rollout_ready" "$scheduler_rollout_ready" >&2
}

daily_rc_wait_for_live_release() {
  timeout_seconds="$1"
  deadline=$(( $(date +%s) + timeout_seconds ))
  while :; do
    daily_rc_read_live_release
    if daily_rc_live_release_matches_candidate; then
      return
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "RC did not converge to the requested release within ${timeout_seconds}s" >&2
      daily_rc_print_release_mismatch
      return 1
    fi
    printf 'waiting: manager=%s scheduler=%s agent=%s\n' \
      "$live_manager" "$live_scheduler" "$live_agent"
    sleep 15
  done
}

daily_rc_fail_if_new_release_detected() {
  daily_rc_read_live_release
  if ! daily_rc_live_release_identity_matches_candidate; then
    echo "new RC version detected while the smoke test was running; terminating immediately" >&2
    daily_rc_print_release_mismatch
    return 1
  fi
}

daily_rc_assert_live_release_matches_candidate() {
  daily_rc_fail_if_new_release_detected || return 1
  if ! daily_rc_live_release_matches_candidate; then
    echo "RC candidate rollout is no longer healthy" >&2
    daily_rc_print_release_mismatch
    return 1
  fi
}
