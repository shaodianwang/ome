#!/usr/bin/env bash

set -euo pipefail

readonly github_noreply_pattern='^[^@[:space:]<>]+@users[.]noreply[.]github[.]com$'
readonly github_email_settings_url='https://github.com/settings/emails'

usage() {
  echo "usage: $0 --current | --revision <revision>" >&2
}

is_github_noreply_email() {
  local email=${1,,}
  [[ "$email" =~ $github_noreply_pattern ]]
}

print_configuration_help() {
  cat >&2 <<EOF
GitHub recommends using a GitHub-provided noreply address as your commit email.
Find your address at ${github_email_settings_url}, then configure it with:

  git config --global user.email '<id>+<username>@users.noreply.github.com'
EOF
}

check_current_author() {
  local identity email
  identity=$(git var GIT_AUTHOR_IDENT)
  email=${identity#*<}
  email=${email%%>*}

  if ! is_github_noreply_email "$email"; then
    echo "Git author email '$email' is not a GitHub noreply address." >&2
    print_configuration_help
    return 1
  fi

  echo "Git author email uses a GitHub noreply address."
}

check_revision() {
  local revision=$1
  local commit_sha author_name author_email
  local found=false
  local failed=false

  while IFS= read -r commit_sha; do
    if [[ -z "$commit_sha" ]]; then
      continue
    fi
    found=true
    author_name=$(git show -s --format='%an' "$commit_sha")
    author_email=$(git show -s --format='%ae' "$commit_sha")
    if ! is_github_noreply_email "$author_email"; then
      echo "Commit ${commit_sha} has author ${author_name} <${author_email}>; expected a GitHub noreply address." >&2
      failed=true
    fi
  done < <(git rev-list --reverse "$revision")

  if [[ "$found" != true ]]; then
    echo "Revision '$revision' does not contain any commits." >&2
    return 1
  fi
  if [[ "$failed" == true ]]; then
    print_configuration_help
    return 1
  fi

  echo "All commit authors in '$revision' use GitHub noreply addresses."
}

main() {
  case ${1:-} in
    --current)
      if [[ $# -ne 1 ]]; then
        usage
        return 2
      fi
      check_current_author
      ;;
    --revision)
      if [[ $# -ne 2 ]]; then
        usage
        return 2
      fi
      check_revision "$2"
      ;;
    *)
      usage
      return 2
      ;;
  esac
}

main "$@"
