#!/usr/bin/env bash
# Registry mirror for the OMENative eval harness and the quota demo fixtures.
#
# Everything under hack/ names public images (Docker Hub) or the bare
# canary-metrics tag built from this repo, and the perm harness substitutes
# ${OME_EVAL_*_IMAGE} tokens at apply time. Clusters that pull only through the
# ECR mirror source this file first:
#
#   source .buildkite/eval_image_mirror.sh            # AWS_REGION picks the replica
#   python3 hack/omenative-testing/perm/run.py p400
#   mirror_images < hack/quota-mc-demo/fixtures/01-runtime.yaml | kubectl apply -f -
#
# Never source it for a KIND run: deploy-ome-kind.sh side-loads the images under
# the public names, and an exported token overrides the KIND names.

PROD_AWS_ACCOUNT_ID="${PROD_AWS_ACCOUNT_ID:-168653207203}"
AWS_REGION="${AWS_REGION:-us-east-2}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-${PROD_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
# Docker Hub is mirrored as pt_dockerio/<namespace>/<repository>.
DOCKERHUB_MIRROR="${PROD_ECR_REGISTRY}/pt_dockerio"

export OME_EVAL_NGINX_IMAGE="${DOCKERHUB_MIRROR}/nginxinc/nginx-unprivileged:1.27-alpine"
export OME_EVAL_NGINX_ALT_IMAGE="${DOCKERHUB_MIRROR}/nginxinc/nginx-unprivileged:1.28-alpine"
export OME_EVAL_METRICS_IMAGE="${PROD_ECR_REGISTRY}/msl_infra/canary-metrics:latest"

# Rewrite the `image: docker.io/...` lines of a manifest on stdin to the mirror.
mirror_images() {
  sed -E "s#(image:[[:space:]]*[\"']?)docker\\.io/#\\1${DOCKERHUB_MIRROR}/#"
}
