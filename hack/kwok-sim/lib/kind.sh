# shellcheck shell=bash
#
# Sourced, not executed. Resolves $KIND to a kind binary whose version matches
# the one this repo pins, and KWOK_REPO_ROOT to the tree it was sourced from.
#
# A node image ships a containerd whose config version an older CLI cannot
# parse, and the failure reads "unknown containerd config version: N (supported
# versions: ...)" with nothing naming kind at all — so a stale binary on PATH
# becomes an unreadable error several steps into a cluster create. Four scripts
# need that answer; one copy rather than four, because a copy that drifts
# reintroduces exactly that error in whichever script kept it.

# From this file's own location, so the caller's working directory is irrelevant.
KWOK_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

resolve_kind() {
  if [ -n "${KIND:-}" ]; then
    return 0
  fi
  if [ -x "${KWOK_REPO_ROOT}/bin/kind" ]; then
    KIND="${KWOK_REPO_ROOT}/bin/kind"
    return 0
  fi

  KIND=kind
  if ! command -v kind >/dev/null 2>&1; then
    echo "kind not found on PATH; run 'make kind' to install the pinned version" >&2
    return 1
  fi

  # Warn only on a real mismatch. A PATH that already carries the pinned version
  # needs no advice, and a note printed on every run is one nobody reads on the
  # run where it matters.
  local want have
  want="$(awk '$1 == "sigs.k8s.io/kind" { print $2 }' \
    "${KWOK_REPO_ROOT}/hack/internal/tools/go.mod" 2>/dev/null)"
  have="$(kind version 2>/dev/null | awk '{print $2}')"
  if [ -n "$want" ] && [ "$want" != "$have" ]; then
    echo "note: kind on PATH is ${have:-unknown}; this repo pins ${want} — run 'make kind'" >&2
  fi
}
