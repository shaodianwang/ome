#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

bash hack/check-git-author-email_test.sh

build_commit=${BUILDKITE_COMMIT:-HEAD}
if [[ ${BUILDKITE_PULL_REQUEST:-false} != false ]]; then
  base_branch=${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}
  git check-ref-format --branch "$base_branch" >/dev/null
  git fetch --no-tags origin \
    "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}"
  if ! merge_base=$(git merge-base "origin/${base_branch}" "$build_commit"); then
    git fetch --no-tags --unshallow origin
    merge_base=$(git merge-base "origin/${base_branch}" "$build_commit")
  fi
  revision="${merge_base}..${build_commit}"
else
  revision="${build_commit}^!"
fi

bash hack/check-git-author-email.sh --revision "$revision"
