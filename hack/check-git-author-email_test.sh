#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
checker="${repo_root}/hack/check-git-author-email.sh"
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT

expect_success() {
  if ! "$@" >/dev/null 2>&1; then
    echo "expected success: $*" >&2
    return 1
  fi
}

expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected failure: $*" >&2
    return 1
  fi
}

check_test_revision() {
  local revision=$1
  (cd "$test_dir" && "$checker" --revision "$revision")
}

expect_success env \
  GIT_AUTHOR_NAME='Example User' \
  GIT_AUTHOR_EMAIL='12345+example@users.noreply.github.com' \
  "$checker" --current
expect_success env \
  GIT_AUTHOR_NAME='Example User' \
  GIT_AUTHOR_EMAIL='example@users.noreply.github.com' \
  "$checker" --current
expect_success env \
  GIT_AUTHOR_NAME='dependabot[bot]' \
  GIT_AUTHOR_EMAIL='49699333+dependabot[bot]@users.noreply.github.com' \
  "$checker" --current
expect_failure env \
  GIT_AUTHOR_NAME='Example User' \
  GIT_AUTHOR_EMAIL='example@example.com' \
  "$checker" --current

git -C "$test_dir" init -q
git -C "$test_dir" config user.name 'Example User'
git -C "$test_dir" config user.email '12345+example@users.noreply.github.com'
git -C "$test_dir" config commit.gpgsign false
printf 'valid\n' >"${test_dir}/fixture.txt"
git -C "$test_dir" add fixture.txt
git -C "$test_dir" commit -q -m 'valid identity'
expect_success check_test_revision 'HEAD^!'

git -C "$test_dir" config user.email 'example@example.com'
printf 'invalid\n' >>"${test_dir}/fixture.txt"
git -C "$test_dir" add fixture.txt
git -C "$test_dir" commit -q -m 'invalid identity'
expect_failure check_test_revision 'HEAD^!'
expect_failure check_test_revision 'HEAD..HEAD'

echo "Git author email checks passed."
