# Shared 3-tier ECR-login helper for the dev (588...) and prod (168...)
# Cloud Foundation ECRs. Source from another script and call:
#
#   ecr_login_for <env> <login-cmd> [<args>...]
#
# where <env> is "dev" or "prod" and <login-cmd> ... reads the password
# from stdin (e.g. `crane auth login --username AWS --password-stdin REG`,
# `helm registry login --username AWS --password-stdin REG`).
#
# Auth tiers (matches the cf-docker-image plugin's containers-common):
#   1. Meta cloud_oss + x509 identity — env-specific cross-account args
#      (source of truth: mslsrc/dev libs/containers-common cf.py)
#   2. Pre-baked ECR_AUTH_TOKEN env var (dev-only; not issued for prod)
#   3. aws CLI fallback (via MKS pod-identity)
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
  if [ -x /opt/cf/cloud_oss/cloud_oss ] && [ -f /var/facebook/x509_identities/combined.pem ]; then
    # Capture cloud_oss output explicitly so a non-zero exit falls through
    # to the next tier instead of silently feeding empty stdin to the
    # downstream login (which would "succeed" with an empty password and
    # then 401 on the actual push).
    token=$(/opt/cf/cloud_oss/cloud_oss ecr get-credentials $cloud_oss_args \
              --region "$AWS_REGION" --outform text) || {
      echo "ecr_login_for: cloud_oss failed for env=$env, falling back" >&2
      token=""
    }
  fi
  if [ -z "$token" ] && [ "$env" = "dev" ] && [ -n "${ECR_AUTH_TOKEN:-}" ]; then
    token="$ECR_AUTH_TOKEN"
  fi
  if [ -z "$token" ]; then
    token=$(aws ecr get-login-password --region "$AWS_REGION")
  fi
  printf '%s' "$token" | "$@"
}
