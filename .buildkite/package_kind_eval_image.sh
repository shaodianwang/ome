#!/bin/sh
# Package this commit's runtime images into the offline GKE KIND runner.
#
# generate_pipeline.py decides what this build publishes and passes it in:
#   OME_PUBLISH_IMAGES        1 when the build publishes its images; unset,
#                             main publishes and every other branch does not
#   OME_EVAL_SOURCE_REGISTRY  where this commit's images are: the prod
#                             registry for main (the mirrors), the dev
#                             registry for a branch (its commit-tag pushes);
#                             unset means the prod registry
# The eval image always publishes to the prod registry, the one the GKE
# queue pulls from, under kind-ci:eval-<sha8>.
#
# Reuses before it packages: generate_pipeline.py passes the tag naming the
# source images' inputs (OME_EVAL_IMAGE_INPUTS_TAG), and an eval image already
# published under it is retagged for this commit instead of being assembled
# again. The archives inside such an image carry the refs of the commit that
# first packaged them; runtime-image-refs.txt records them for the runner.
# OME_IMAGE_REBUILD=1 skips the probe and still retags what it packages.
set -eu

publish="${OME_PUBLISH_IMAGES:-}"
if [ -z "$publish" ]; then
  if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then publish=1; else publish=0; fi
fi
[ "$publish" = 1 ] || {
  echo "package_kind_eval_image.sh: this build publishes no images" \
    "(OME_PUBLISH_IMAGES=${OME_PUBLISH_IMAGES:-unset}, branch ${BUILDKITE_BRANCH:-?})" >&2
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
source_registry="${OME_EVAL_SOURCE_REGISTRY:-$PROD_ECR_REGISTRY}"
source_repo="${source_registry}/msl_infra"

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
if [ "$source_registry" != "$PROD_ECR_REGISTRY" ]; then
  ecr_login_for dev crane auth login --username AWS --password-stdin "$source_registry"
fi
echo "--- eval image sources: ${source_repo}/<image>:${short_sha} (branch ${BUILDKITE_BRANCH:-?})"

stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
mkdir -p "$stage/images"

inputs_tag="${OME_EVAL_IMAGE_INPUTS_TAG:-}"
if [ -n "$inputs_tag" ]; then
  candidate="${repo}/kind-ci:${inputs_tag}"
  echo "--- eval image reuse check: ${target}"
  echo "commit:        ${BUILDKITE_COMMIT}"
  echo "inputs tag:    ${inputs_tag}"
  echo "inputs hashed: ${OME_EVAL_IMAGE_INPUTS:-?}"
  if [ "${OME_IMAGE_REBUILD:-0}" = 1 ]; then
    echo "OME_IMAGE_REBUILD=1: packaging without probing"
  elif digest=$(crane digest "$candidate" 2>"$stage/probe.err"); then
    echo "found ${candidate}@${digest}; retagging as ${target}"
    crane tag "$candidate" "eval-${short_sha}"
    echo "reused ${candidate}@${digest} as ${target}"
    exit 0
  else
    echo "no eval image for these inputs: $(tr '\n' ' ' <"$stage/probe.err"); packaging"
  fi
fi

for image in ome-manager ome-agent ome-scheduler canary-metrics; do
  ref="${source_repo}/${image}:${short_sha}"
  echo "--- pull ${ref}"
  crane pull --platform linux/amd64 "$ref" "$stage/images/${image}.tar"
  test -s "$stage/images/${image}.tar"
  crane validate --tarball "$stage/images/${image}.tar"
  printf '%s\t%s\n' "$image" "$ref" >>"$stage/images/runtime-image-refs.txt"
done

tar -C "$stage" -cf "$stage/runtime-images.layer.tar" images
echo "--- publish ${target}"
crane append --platform linux/amd64 \
  --base "${source_repo}/kind-ci:${short_sha}" \
  --new_layer "$stage/runtime-images.layer.tar" \
  --new_tag "$target"

digest=$(crane digest "$target")
echo "published ${target}@${digest}"
if [ -n "$inputs_tag" ]; then
  crane tag "$target" "$inputs_tag"
  echo "tagged ${target} as ${repo}/kind-ci:${inputs_tag} for reuse"
fi
