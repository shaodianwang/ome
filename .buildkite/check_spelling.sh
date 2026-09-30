#!/usr/bin/env bash
# Fail the build on a misspelling in any tracked text file. The codespell
# version, ignore-words list and excluded paths come from the pre-commit hook
# in .pre-commit-config.yaml; hack/check_spelling.py reads them from there.

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends python3-venv > /dev/null

venv=$(mktemp -d)/venv
python3 -m venv "$venv"
version=$(python3 hack/check_spelling.py --print-version)
"$venv/bin/pip" install --quiet --disable-pip-version-check "codespell==${version}"

"$venv/bin/python" -m unittest hack/check_spelling_test.py
"$venv/bin/python" hack/check_spelling.py
