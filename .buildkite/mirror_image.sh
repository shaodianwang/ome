#!/bin/sh
# Copy an image from the dev ECR (588...) to the prod ECR (168...) via
# `crane copy` — pure-registry, no docker daemon required. Necessary so
# policy-restricted clusters (cf-replace-image-registry admission policy)
# can pull the image.
#
# Usage: sh .buildkite/mirror_image.sh [--only-main] <ecr-name> <tag> [<tag>...]
#
# --only-main: exit 0 on non-main branches; prod receives main-qualified
#              images only, whatever a branch build pushed to dev.
set -eu

if [ "${1:-}" = "--only-main" ]; then
  shift
  if [ "${BUILDKITE_BRANCH:-}" != "main" ]; then
    echo "branch=${BUILDKITE_BRANCH:-?} is not main; skipping mirror"
    exit 0
  fi
fi

ECR_NAME="${1:?ecr-name required}"
shift
[ "$#" -gt 0 ] || { echo "at least one tag required" >&2; exit 1; }

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
PROD_AWS_ACCOUNT_ID="${PROD_AWS_ACCOUNT_ID:-168653207203}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-${PROD_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"

# ci-base doesn't ship docker (it's a Rust/RDMA build image), so use
# crane — single static binary, pure-registry manifest+blob copy.
CRANE_VERSION="${CRANE_VERSION:-0.20.6}"
if ! command -v crane >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64|amd64) CRANE_ARCH="x86_64" ;;
    aarch64|arm64) CRANE_ARCH="arm64" ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
    | tar -xz -C /usr/local/bin crane
fi

. "$(dirname "$0")/_ecr_login.sh"
ecr_login_for dev crane auth login --username AWS --password-stdin "$ECR_REGISTRY"
ecr_login_for prod crane auth login --username AWS --password-stdin "$PROD_ECR_REGISTRY"

for tag in "$@"; do
  src="${ECR_REGISTRY}/${ECR_NAME}:${tag}"
  dst="${PROD_ECR_REGISTRY}/${ECR_NAME}:${tag}"
  echo "--- :docker: Mirror ${src} → ${dst}"
  crane copy "$src" "$dst"
done
