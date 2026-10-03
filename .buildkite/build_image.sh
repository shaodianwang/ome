#!/bin/sh
# Reuse the dev-registry image built from these same inputs, or build one.
#
# generate_pipeline.py hashes everything an image build reads and every
# published image carries that hash as a tag next to its commit tag. This step
# probes the dev registry for the hash tag: when it exists the image is
# retagged for this commit and no build runs; otherwise the generator-rendered
# build step is uploaded behind this one, and the steps that wait on this
# step's key wait for the build as well. Every decision prints the inputs it
# compared, the registry's answer and the tags it published, so a wrong reuse
# is visible in the log and in the build annotation.
#
# Usage: sh .buildkite/build_image.sh <ecr-name>
#
# Environment (set by generate_pipeline.py):
#   OME_IMAGE_INPUTS_TAG      tag naming the inputs hash, e.g. inputs-<hex>
#   OME_IMAGE_INPUTS          the hashed paths, space separated (for the log)
#   OME_PUBLISH_IMAGES        1 when this step publishes from this build, 0
#                             when it builds for validation only; unset, main
#                             publishes and every other branch does not
#   OME_IMAGE_PUBLISH_TAGS    tags to publish on reuse, space separated; empty
#                             when this build publishes nothing for the image
#   OME_IMAGE_BUILD_STEP_B64  base64 of the pipeline fragment that builds the
#                             image, uploaded when no reusable image exists
#   ECR_REGISTRY, AWS_REGION  the dev registry and its region (_ecr_login.sh)
set -eu

ecr_name="${1:?ecr-name required}"
inputs_tag="${OME_IMAGE_INPUTS_TAG:?OME_IMAGE_INPUTS_TAG is required}"
build_step_b64="${OME_IMAGE_BUILD_STEP_B64:?OME_IMAGE_BUILD_STEP_B64 is required}"
publish="${OME_PUBLISH_IMAGES:-}"
if [ -z "$publish" ]; then
  if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then publish=1; else publish=0; fi
fi
publish_tags="${OME_IMAGE_PUBLISH_TAGS:-}"
[ "$publish" = 1 ] || publish_tags=""
registry="${ECR_REGISTRY:?ECR_REGISTRY is required}"
: "${AWS_REGION:?AWS_REGION is required}"
image="${registry}/${ecr_name}"
candidate="${image}:${inputs_tag}"

echo "--- image reuse check: ${image}"
echo "commit:        ${BUILDKITE_COMMIT:-?} (branch ${BUILDKITE_BRANCH:-?})"
echo "inputs tag:    ${inputs_tag}"
echo "inputs hashed: ${OME_IMAGE_INPUTS:-?}"
echo "publishes:     ${publish} (OME_PUBLISH_IMAGES=${OME_PUBLISH_IMAGES:-unset})"
if [ -n "$publish_tags" ]; then
  echo "publish tags:  ${publish_tags}"
else
  echo "publish tags:  none (this build publishes nothing for the image)"
fi

# One shared build annotation, a line per image, so a wrong reuse is visible
# without opening each step.
annotate() {
  command -v buildkite-agent >/dev/null 2>&1 || return 0
  printf -- '- `%s`: %s\n' "$ecr_name" "$2" \
    | buildkite-agent annotate --style "$1" --context image-reuse --append || true
}

# The uploaded fragment is final text: --no-interpolation keeps the agent from
# expanding anything that looks like a variable inside it.
upload_build_step() {
  echo "--- build ${image}: uploading the build step"
  step=$(printf '%s' "$build_step_b64" | base64 -d)
  printf '%s\n' "$step"
  printf '%s\n' "$step" | buildkite-agent pipeline upload --no-interpolation
}

# ci-base doesn't ship docker, so use crane: single static binary, pure-registry.
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
ecr_login_for dev crane auth login --username AWS --password-stdin "$registry"

probe_err=$(mktemp)
trap 'rm -f -- "$probe_err"' EXIT
echo "--- probe ${candidate}"
if ! digest=$(crane digest "$candidate" 2>"$probe_err"); then
  echo "no image for these inputs: $(tr '\n' ' ' <"$probe_err")"
  annotate info "no image for ${inputs_tag}; building"
  upload_build_step
  exit 0
fi
echo "found ${candidate}@${digest}"

if [ -z "$publish_tags" ]; then
  echo "reused: this build publishes nothing for the image, so there is nothing to retag and no build to run"
  annotate info "reused ${inputs_tag} (${digest}); nothing to publish from this build"
  exit 0
fi

# A failed retag (for example a credential that can read but not write) falls
# back to the build, which publishes through the plugin's own login, so a
# permission problem costs the saving and never the image.
for tag in $publish_tags; do
  echo "--- retag ${candidate} as ${image}:${tag}"
  if ! crane tag "$candidate" "$tag"; then
    echo "retag failed; building instead so this commit still gets its image" >&2
    annotate warning "retag of ${inputs_tag} as ${tag} failed; building"
    upload_build_step
    exit 0
  fi
done
echo "reused ${candidate}@${digest} as: ${publish_tags}"
annotate info "reused ${inputs_tag} (${digest}) as ${publish_tags}"
