#!/bin/sh
# Build (and optionally push) one OME container image.
#
# Usage: sh .buildkite/build_image.sh <name> <dockerfile> [context]
#
# Lives in its own file so $VAR refs survive BuildKite's command-field
# interpolation (which expands shell vars at job-submit time and replaces
# unset ones with the empty string).
set -eu

NAME="${1:?name required}"
DOCKERFILE="${2:?dockerfile required}"
CONTEXT="${3:-.}"

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
IMAGE_REPO="${ECR_REGISTRY}/ome"

# Tag layout: <YYYYMMDD>-<short-sha>, plus :latest on main pushes.
DATE_TAG="$(date -u +%Y%m%d)"
SHORT_SHA="$(echo "${BUILDKITE_COMMIT:-HEAD}" | cut -c1-8)"
IMAGE_TAG="${DATE_TAG}-${SHORT_SHA}"
FULL_TAG="${IMAGE_REPO}/${NAME}:${IMAGE_TAG}"
LATEST_TAG="${IMAGE_REPO}/${NAME}:latest"

# The dind image is Alpine-based and has docker but not aws CLI. Install
# it here so we can fetch ECR credentials. apk-cache is per-pod so this
# only runs once per build.
if ! command -v aws >/dev/null 2>&1; then
  apk add --no-cache aws-cli
fi

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

docker build \
  --build-arg "GIT_TAG=${IMAGE_TAG}" \
  --build-arg "GIT_COMMIT=${BUILDKITE_COMMIT:-HEAD}" \
  -f "$DOCKERFILE" \
  -t "$FULL_TAG" \
  -t "$LATEST_TAG" \
  "$CONTEXT"

# Only push on main. PR builds validate the Dockerfile but don't publish.
if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then
  docker push "$FULL_TAG"
  docker push "$LATEST_TAG"
  echo "pushed ${FULL_TAG} and ${LATEST_TAG}"
else
  echo "branch=${BUILDKITE_BRANCH:-?} is not main; skipping push"
fi
