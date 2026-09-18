#!/bin/sh
# Package (and on main, push) one Helm chart to OCI ECR.
#
# Usage: sh .buildkite/push_chart.sh <chart-name>
#
# Runs in the prod-inference-ci-base image under the TW docker plugin;
# ECR auth via cloud_oss identity or Buildkite cluster secrets (see
# _ecr_login.sh).
set -eu

CHART="${1:?chart required}"

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
# Prod ECR is the cluster-foundation managed registry; cf-replace-image-registry
# admission policy fast-paths images already on this account, so the chart's
# default hub points here.
PROD_AWS_ACCOUNT_ID="${PROD_AWS_ACCOUNT_ID:-168653207203}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-${PROD_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
# Use the msl_infra/ prefix so chart repos auto-provision via the
# same template that backs msl_infra/ome-manager and the other
# images. ECR otherwise 404s on first `helm push` because OCI
# repos are not auto-created.
DEV_CHART_REPO="oci://${ECR_REGISTRY}/msl_infra/ome-charts"
PROD_CHART_REPO="oci://${PROD_ECR_REGISTRY}/msl_infra/ome-charts"

TIMESTAMP="$(date -u +%Y%m%d%H%M)"
SHORT_SHA="$(echo "${BUILDKITE_COMMIT:-HEAD}" | cut -c1-8)"
CHART_VERSION="1.0.0-${TIMESTAMP}.g${SHORT_SHA}"

HELM_VERSION="${HELM_VERSION:-v3.16.2}"
if ! command -v helm >/dev/null 2>&1; then
  # Runtime image is not Debian-based (no apt). curl + tar ship with
  # the prod-inference-ci-base image, so download helm directly.
  curl -sSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" -o /tmp/helm.tgz
  tar -xzf /tmp/helm.tgz -C /tmp
  mv /tmp/linux-amd64/helm /usr/local/bin/helm
fi

# Stamp the version into Chart.yaml before packaging.
sed -i "s/^version:.*/version: ${CHART_VERSION}/" "charts/${CHART}/Chart.yaml"
sed -i "s/^appVersion:.*/appVersion: \"${CHART_VERSION}\"/" "charts/${CHART}/Chart.yaml"

# For ome-resources, stamp the image hub and tag so the chart references
# the images built from the same commit. Hub points at PROD ECR so charts
# installed in policy-restricted clusters resolve images without overrides;
# dev clusters can override via --set global.hub=$ECR_REGISTRY/msl_infra.
if [ "${CHART}" = "ome-resources" ]; then
  IMAGE_TAG="${SHORT_SHA}"
  sed -i "s|hub: \"ghcr.io/moirai-internal\"|hub: \"${PROD_ECR_REGISTRY}/msl_infra\"|" "charts/${CHART}/values.yaml"
  sed -i "s|version: \&defaultVersion .*|version: \&defaultVersion \"${IMAGE_TAG}\"|" "charts/${CHART}/values.yaml"
  # model-agent is not built here; consumers who flip
  # modelAgent.enabled=true must override image.hub/repository to
  # point at an upstream-published image themselves.
fi

# For ome-scheduler, likewise stamp the image hub (global.hub) + tag so the
# published chart resolves the ome-scheduler image built from this commit on
# policy-restricted clusters; dev clusters can override global.hub.
if [ "${CHART}" = "ome-scheduler" ]; then
  IMAGE_TAG="${SHORT_SHA}"
  sed -i "s|hub: \"\"|hub: \"${PROD_ECR_REGISTRY}/msl_infra\"|" "charts/${CHART}/values.yaml"
  sed -i "s|tag: latest|tag: \"${IMAGE_TAG}\"|" "charts/${CHART}/values.yaml"
fi

# ome-quota-manager ships its own image, so it needs the same stamp. Without it
# the published chart keeps hub: "" and tag: latest, which a kubelet resolves as
# docker.io/library/ome-quota-manager:latest and cannot pull.
if [ "${CHART}" = "ome-quota-manager" ]; then
  IMAGE_TAG="${SHORT_SHA}"
  sed -i "s|hub: \"\"|hub: \"${PROD_ECR_REGISTRY}/msl_infra\"|" "charts/${CHART}/values.yaml"
  sed -i "s|tag: latest|tag: \"${IMAGE_TAG}\"|" "charts/${CHART}/values.yaml"
fi

# The muse-server image is mirrored to the prod ECR from this same commit, so
# point the chart's default hub/tag at it — gitops then needs no image overrides.
if [ "${CHART}" = "muse" ]; then
  sed -i "s|hub: \"\"|hub: \"${PROD_ECR_REGISTRY}/msl_infra\"|" "charts/${CHART}/values.yaml"
  sed -i "s|tag: latest|tag: \"${SHORT_SHA}\"|" "charts/${CHART}/values.yaml"
fi

helm package "charts/${CHART}"

. "$(dirname "$0")/_ecr_login.sh"

if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then
  ecr_login_for dev helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  helm push "${CHART}-${CHART_VERSION}.tgz" "$DEV_CHART_REPO"
  echo "pushed ${CHART}-${CHART_VERSION}.tgz to ${DEV_CHART_REPO}"

  ecr_login_for prod helm registry login --username AWS --password-stdin "$PROD_ECR_REGISTRY"
  helm push "${CHART}-${CHART_VERSION}.tgz" "$PROD_CHART_REPO"
  echo "pushed ${CHART}-${CHART_VERSION}.tgz to ${PROD_CHART_REPO}"
else
  # Exercise both registry logins on PR branches too — pushes are
  # main-only, but a broken credential path must fail the PR that
  # breaks it, not the next main build.
  ecr_login_for dev helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  ecr_login_for prod helm registry login --username AWS --password-stdin "$PROD_ECR_REGISTRY"
  echo "branch=${BUILDKITE_BRANCH:-?} is not main; logins OK, skipping push"
fi

# Buildkite uploads this file after the command succeeds. The release-marker
# step downloads it from this exact chart step, so it can only qualify a
# version whose required registry operations completed.
VERSION_ARTIFACT=".buildkite/artifacts/chart-versions/${CHART}"
mkdir -p "$(dirname "$VERSION_ARTIFACT")"
printf '%s\n' "$CHART_VERSION" >"$VERSION_ARTIFACT"
