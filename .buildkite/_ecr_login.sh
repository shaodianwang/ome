# Shared 3-tier ECR-login helper for the dev (588...) and prod (168...)
# Cloud Foundation ECRs. Source from another script and call:
#
#   ecr_login_for <env> <login-cmd> [<args>...]
#
# where <env> is "dev" or "prod" and <login-cmd> ... reads the password
# from stdin (e.g. `crane auth login --username AWS --password-stdin REG`,
# `helm registry login --username AWS --password-stdin REG`).
#
# Auth tiers:
#   1. Meta cloud_oss + x509 identity — env-specific cross-account args
#      (source of truth: mslsrc/dev libs/containers-common cf.py).
#      CLOUD_OSS_PATH overrides the binary location; the default is
#      where cf-built images bake it.
#   2. Buildkite cluster secrets: ECR_AUTH_TOKEN (dev) and
#      PROD_ECR_AUTH_TOKEN (prod). On TW agents these are the working
#      path: the agent resolves each name a step lists under `secrets:`
#      (see generate_pipeline.py) into the job env, and the docker
#      plugin's propagate-environment carries them into the workload
#      container.
#   3. aws CLI fallback (MKS pod-identity era). TW workload containers
#      don't have aws on the plugin-forced PATH, so this tier is
#      guarded — absence degrades to the final error, not exit 127.
#
# Reads: AWS_REGION (required).

ecr_login_for() {
  env=$1
  shift
  case "$env" in
    dev)
      # mslx-aws-1 has no cross-account access to dev; use the --dev shortcut.
      cloud_oss_args="--dev --role SSOContainerRegistryReadWrite"
      ;;
    prod)
      # Prod ECR (168...) reached via mslx-aws-1 (652086758864) cross-account
      # SSOContainerRegistryReadWrite. There is NO `--prod` shortcut.
      cloud_oss_args="--account 652086758864 --role SSOContainerRegistryReadWrite"
      ;;
    *)
      echo "ecr_login_for: unknown env '$env'" >&2
      return 1
      ;;
  esac

  token=""
  cloud_oss_bin="${CLOUD_OSS_PATH:-/opt/cf/cloud_oss/cloud_oss}"
  if [ -x "$cloud_oss_bin" ] && [ -f /var/facebook/x509_identities/combined.pem ]; then
    # Capture cloud_oss output explicitly so a non-zero exit falls through
    # to the next tier instead of silently feeding empty stdin to the
    # downstream login (which would "succeed" with an empty password and
    # then 401 on the actual push).
    token=$("$cloud_oss_bin" ecr get-credentials $cloud_oss_args \
              --region "$AWS_REGION" --outform text) || {
      echo "ecr_login_for: cloud_oss failed for env=$env, falling back" >&2
      token=""
    }
  fi
  if [ -z "$token" ]; then
    case "$env" in
      dev) token="${ECR_AUTH_TOKEN:-}" ;;
      prod) token="${PROD_ECR_AUTH_TOKEN:-}" ;;
    esac
  fi
  if [ -z "$token" ] && command -v aws >/dev/null 2>&1; then
    token=$(aws ecr get-login-password --region "$AWS_REGION") || {
      echo "ecr_login_for: aws CLI failed for env=$env" >&2
      token=""
    }
  fi
  if [ -z "$token" ]; then
    echo "ecr_login_for: no ECR credential source produced a token for env=$env." >&2
    echo "  cloud_oss missing/failed ($cloud_oss_bin), token env not set (does the" >&2
    echo "  step list ECR_AUTH_TOKEN / PROD_ECR_AUTH_TOKEN under secrets: in" >&2
    echo "  generate_pipeline.py?), and no aws CLI on PATH." >&2
    return 1
  fi
  printf '%s' "$token" | "$@"
}
