#!/bin/sh
# Package (and on main, push) one Helm chart to OCI ECR.
#
# Usage: sh .buildkite/push_chart.sh <chart-name>
#
# Runs in the prod-inference-ci-base image with MKS pod-identity
# annotations on the job pod (set by .buildkite/generate_pipeline.py).
set -eu

CHART="${1:?chart required}"

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
# Use the msl_infra/ prefix so chart repos auto-provision via the
# same template that backs msl_infra/ome-manager and the other
# images. ECR otherwise 404s on first `helm push` because OCI
# repos are not auto-created.
CHART_REPO="oci://${ECR_REGISTRY}/msl_infra/ome-charts"

DATE_TAG="$(date -u +%Y%m%d)"
SHORT_SHA="$(echo "${BUILDKITE_COMMIT:-HEAD}" | cut -c1-8)"
CHART_VERSION="${DATE_TAG}-${SHORT_SHA}"

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

helm package "charts/${CHART}"

if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then
  # Mirrors the cf-docker-image plugin's three-tier ECR auth so chart
  # pushes use the same credential path as image pushes:
  #   1. Meta cloud_oss + x509 identity (the path that runs in prod)
  #   2. Pre-baked ECR_AUTH_TOKEN env var
  #   3. aws CLI fallback (resolved via MKS pod-identity annotations)
  if [ -x /shared/cloud_oss ] && [ -f /var/facebook/x509_identities/combined.pem ]; then
    /shared/cloud_oss ecr get-credentials --dev \
        --role SSOContainerRegistryReadWrite --region "$AWS_REGION" --outform text \
      | helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  elif [ -n "${ECR_AUTH_TOKEN:-}" ]; then
    echo "$ECR_AUTH_TOKEN" \
      | helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  else
    aws ecr get-login-password --region "$AWS_REGION" \
      | helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  fi
  helm push "${CHART}-${CHART_VERSION}.tgz" "$CHART_REPO"
  echo "pushed ${CHART}-${CHART_VERSION}.tgz to ${CHART_REPO}"
else
  echo "branch=${BUILDKITE_BRANCH:-?} is not main; skipping push"
fi
