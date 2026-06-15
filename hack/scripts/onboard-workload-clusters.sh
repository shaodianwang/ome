#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 --management-context CONTEXT --management-namespace NAMESPACE --workload-namespace NAMESPACE CLUSTER_NAME [...]
  $0 --management-context CONTEXT --management-namespace NAMESPACE --workload-namespace NAMESPACE CLUSTER_CONTEXT=CLUSTER_NAME [...]

Example:
  clusters=(msl-cw-use2-dev-2)
  $0 "\${clusters[@]}"

  $0 \\
    --management-context mgmt \\
    --management-namespace inference-deployment-system \\
    --workload-namespace ome
EOF
}

management_context="msl-mks-cp-lco-0"
management_namespace="inference-deployment-system"
workload_namespace="ome"
token_secret_name="ome-multicluster-access-token"
clusters=(msl-cw-use2-dev-2)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --management-context)
      management_context="$2"
      shift 2
      ;;
    --management-namespace)
      management_namespace="$2"
      shift 2
      ;;
    --workload-namespace)
      workload_namespace="$2"
      shift 2
      ;;
    --token-secret-name)
      token_secret_name="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      clusters+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$management_context" || ${#clusters[@]} -eq 0 ]]; then
  usage >&2
  exit 1
fi

kubectl --context "$management_context" create namespace "$management_namespace" --dry-run=client -o yaml | kubectl --context "$management_context" apply -f -

for entry in "${clusters[@]}"; do
  if [[ "$entry" == *"="* ]]; then
    workload_context="${entry%%=*}"
    cluster_name="${entry#*=}"
  else
    workload_context="$entry"
    cluster_name="$entry"
  fi

  if [[ -z "$workload_context" || -z "$cluster_name" ]]; then
    echo "invalid cluster entry: $entry" >&2
    exit 1
  fi

  server="$(kubectl config view --raw --minify --context "$workload_context" -o jsonpath='{.clusters[0].cluster.server}')"
  ca_data="$(kubectl config view --raw --minify --context "$workload_context" -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
  token="$(kubectl --context "$workload_context" -n "$workload_namespace" get secret "$token_secret_name" -o jsonpath='{.data.token}' | base64 --decode)"

  kubectl --context "$management_context" -n "$management_namespace" create secret generic "workload-${cluster_name}" \
    --from-literal=clusterName="$cluster_name" \
    --from-literal=server="$server" \
    --from-literal=bearerToken="$token" \
    --from-literal=certificateAuthorityData="$ca_data" \
    --dry-run=client -o yaml | \
    kubectl --context "$management_context" label --local -f - \
      "ome.io/cluster-credential=true" \
      "ome.io/cluster-name=${cluster_name}" \
      -o yaml | \
    kubectl --context "$management_context" apply -f -
done
