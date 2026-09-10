# shellcheck shell=bash
#
# Sourced, not executed. Namespaces the AcceleratorQuota fixtures bind.
#
# Kueue's LocalQueue is namespaced, so a leaf naming a namespace that does not
# exist cannot be fully materialized: the ClusterQueue lands and the budget is
# real, but nothing binds the namespace and no workload there can reach the
# queue. It is not reported as Degraded either — the tree is sound, one object
# under it could not be written — so the gap reads as a working tenant.
#
# In a real cluster the namespace exists long before anyone writes a budget
# against it. A simulator has to make that true for itself.

# ENROLLED_NAMESPACES are the namespaces the simulated clusters serve budgeted
# workloads from. It is a list here rather than something scraped out of the
# fixtures because that is where it lives in a real install too: enrollment is
# deploy config on ome-quota-manager, not a field on any AcceleratorQuota. Every
# leaf gets a LocalQueue named after it in each of these, and every rendered
# ClusterQueue selects exactly this set.
ENROLLED_NAMESPACES="kwok-sim kwok-sim-batch"

# enrolled_namespaces_csv renders them for --set / --enrolled-namespaces.
enrolled_namespaces_csv() {
  echo "$ENROLLED_NAMESPACES" | tr ' ' ','
}

# ensure_namespaces creates them on one cluster, idempotently.
ensure_namespaces() { # $1 = kube context
  local ctx="$1"
  local ns
  for ns in $ENROLLED_NAMESPACES; do
    kubectl --context "$ctx" create namespace "$ns" --dry-run=client -o yaml |
      kubectl --context "$ctx" apply -f - >/dev/null
  done
}
