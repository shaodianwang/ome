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

# fixture_namespaces prints every namespace the given fixtures bind, one per
# line. Read from the fixtures rather than listed here, so a fixture that binds
# a new namespace needs no second edit elsewhere to work.
fixture_namespaces() { # $@ = fixture files
  awk '
    /^[[:space:]]*namespaces:[[:space:]]*$/ { in_ns = 1; next }
    in_ns && /^[[:space:]]*-[[:space:]]/ {
      sub(/^[[:space:]]*-[[:space:]]*/, ""); print; next
    }
    { in_ns = 0 }
  ' "$@" | sort -u
}

# ensure_namespaces creates them on one cluster, idempotently.
ensure_namespaces() { # $1 = kube context, rest = fixture files
  local ctx="$1"
  shift
  local ns
  for ns in $(fixture_namespaces "$@"); do
    kubectl --context "$ctx" create namespace "$ns" --dry-run=client -o yaml |
      kubectl --context "$ctx" apply -f - >/dev/null
  done
}
