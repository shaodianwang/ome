#!/bin/sh
# Package exact main-build runtime images into the offline GKE KIND runner.
set -eu

[ "${BUILDKITE_BRANCH:-}" = "main" ] || {
  echo "package_kind_eval_image.sh only publishes from main" >&2
  exit 1
}

short_sha=$(printf '%.8s' "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}")
case "$short_sha" in
  *[!0-9a-f]*) echo "invalid commit SHA: $short_sha" >&2; exit 1 ;;
esac

AWS_REGION="${AWS_REGION:-us-east-2}"
PROD_AWS_ACCOUNT_ID="${PROD_AWS_ACCOUNT_ID:-168653207203}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-${PROD_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
repo="${PROD_ECR_REGISTRY}/msl_infra"
target="${repo}/kind-ci:eval-${short_sha}"

if ! command -v crane >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64|amd64) crane_arch=x86_64 ;;
    aarch64|arm64) crane_arch=arm64 ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
  crane_version="${CRANE_VERSION:-0.20.6}"
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${crane_version}/go-containerregistry_Linux_${crane_arch}.tar.gz" \
    | tar -xz -C /usr/local/bin crane
fi

. "$(dirname "$0")/_ecr_login.sh"
ecr_login_for prod crane auth login --username AWS --password-stdin "$PROD_ECR_REGISTRY"

stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
mkdir -p "$stage/images"

for image in ome-manager ome-agent ome-scheduler canary-metrics; do
  ref="${repo}/${image}:${short_sha}"
  echo "--- pull ${ref}"
  crane pull --platform linux/amd64 "$ref" "$stage/images/${image}.tar"
  test -s "$stage/images/${image}.tar"
  crane validate --tarball "$stage/images/${image}.tar"
  printf '%s\t%s\n' "$image" "$ref" >>"$stage/images/runtime-image-refs.txt"
done

tar -C "$stage" -cf "$stage/runtime-images.layer.tar" images
echo "--- publish ${target}"
crane append --platform linux/amd64 \
  --base "${repo}/kind-ci:${short_sha}" \
  --new_layer "$stage/runtime-images.layer.tar" \
  --new_tag "$target"

digest=$(crane digest "$target")
echo "published ${target}@${digest}"
