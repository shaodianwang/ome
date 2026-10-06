#!/usr/bin/env bash
# Fail the build when the change set rewrites the rollout behavior lock (a
# golden, a decided cell or a harness contract string) with no Eval: line in
# the HEAD commit message; hack/check_lock_rewrite.py states what counts.
#
# On main with no pull request the check is a post-merge audit: a finding is
# a warning annotation, not a failure, so a trailer lost in a squash message
# cannot redden main.

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

# The CI image carries python3; the cells files need PyYAML.
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends python3-yaml > /dev/null

python3 -m unittest hack/check_lock_rewrite_test.py

build_commit=${BUILDKITE_COMMIT:-HEAD}
base_branch=${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}
git check-ref-format --branch "$base_branch" >/dev/null
git fetch --no-tags origin "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}"
# A shallow checkout may lack the merge base, or the parent a main build is
# judged against.
if ! git merge-base "origin/${base_branch}" "$build_commit" >/dev/null 2>&1 \
  || ! git rev-parse --verify --quiet "${build_commit}^" >/dev/null; then
  git fetch --no-tags --unshallow origin || true
fi

advisory=false
if [[ ${BUILDKITE_BRANCH:-} == main && ${BUILDKITE_PULL_REQUEST:-false} == false ]]; then
  advisory=true
fi

# The same command as `make rollout-lock-tripwire`, called directly so the
# script's exit code (1 a finding, 2 an error) reaches this wrapper.
set +e
report=$(python3 hack/check_lock_rewrite.py --base "origin/${base_branch}" --head "$build_commit" 2>&1)
status=$?
set -e
printf '%s\n' "$report"

if [[ $status -ne 1 || $advisory != true ]]; then
  exit "$status"
fi

message="Rollout lock tripwire (advisory on main): the merged change rewrote part of the behavior lock with no Eval: line naming a rerun."
if command -v buildkite-agent >/dev/null 2>&1; then
  printf '%s\n\n```\n%s\n```\n' "$message" "$report" \
    | buildkite-agent annotate --style warning --context rollout-lock-tripwire
else
  echo "WARNING: $message"
fi
exit 0
