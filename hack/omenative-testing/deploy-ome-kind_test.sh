#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=hack/omenative-testing/deploy-ome-kind.sh
source "${repo_root}/hack/omenative-testing/deploy-ome-kind.sh"

fail() {
  echo "deploy-ome-kind test: $*" >&2
  exit 1
}

manager_a="sha256:$(printf 'a%.0s' {1..64})"
manager_b="sha256:$(printf 'c%.0s' {1..64})"
agent_a="sha256:$(printf 'b%.0s' {1..64})"
agent_b="sha256:$(printf 'd%.0s' {1..64})"

tag="$(deploy_tag_from_image_ids "$manager_a" "$agent_a")"
[[ "$tag" == "kind-864f51ecf848c035c5c4549e5a7c5151e6786d7eb37a91e1cec7f674e81fa821" ]] ||
  fail "unexpected content-derived tag: ${tag}"
[[ "$(deploy_tag_from_image_ids "$manager_a" "$agent_a")" == "$tag" ]] ||
  fail "the same image pair did not produce a stable tag"
[[ "$(deploy_tag_from_image_ids "$manager_b" "$agent_a")" != "$tag" ]] ||
  fail "changing the manager image ID did not change the common tag"
[[ "$(deploy_tag_from_image_ids "$manager_a" "$agent_b")" != "$tag" ]] ||
  fail "changing the agent image ID did not change the common tag"

test_dir="$(mktemp -d)"
trap 'rm -rf -- "$test_dir"' EXIT
derive_bin="${test_dir}/derive-bin"
derive_log="${test_dir}/derive.log"
mkdir -p "$derive_bin"
cat >"${derive_bin}/docker" <<'FAKE'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$DERIVE_LOG"
[ "${1:-}" = "create" ] && echo container-1
exit 0
FAKE
chmod +x "${derive_bin}/docker"
DERIVE_LOG="$derive_log" PATH="${derive_bin}:$PATH" \
  derive_image example.com/web:base example.com/web:alt
grep -Fqx "docker create example.com/web:base" "$derive_log" ||
  fail "the derived image was not created from its source"
grep -Fqx "docker commit --change LABEL ome.eval/derived-from=example.com/web:base container-1 example.com/web:alt" "$derive_log" ||
  fail "the derived image was not committed under its own id and the case tag"
grep -Fqx "docker rm -f container-1" "$derive_log" ||
  fail "the scratch container of the derived image was not removed"
if grep -Fq "docker image tag" "$derive_log" || grep -Fq "docker tag" "$derive_log"; then
  fail "the case image must be derived, not aliased: an alias shares the source image id"
fi

fake_bin="${test_dir}/bin"
fake_log="${test_dir}/commands.log"
mkdir -p "$fake_bin"

cat >"${fake_bin}/fake-tool" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail

tool="${0##*/}"
case "$tool" in
  docker)
    case "${1:-} ${2:-}" in
      "info --format") echo x86_64 ;;
      "image inspect")
        image="${!#}"
        case "$image" in
          ome-manager:source) echo "$FAKE_MANAGER_ID" ;;
          ome-agent:source) echo "$FAKE_AGENT_ID" ;;
          *) exit 1 ;;
        esac
        ;;
      "image tag") printf 'docker image tag %s %s\n' "$3" "$4" >>"$FAKE_LOG" ;;
      "save --platform")
        while [ "$#" -gt 0 ]; do
          if [ "$1" = "-o" ]; then
            : >"$2"
            break
          fi
          shift
        done
        ;;
      "build "*) printf 'unexpected docker build\n' >>"$FAKE_LOG"; exit 1 ;;
      *) exit 0 ;;
    esac
    ;;
  kind)
    if [ "${1:-} ${2:-}" = "get clusters" ]; then
      echo ome-eval-kind
    elif [ "${1:-}" = "load" ]; then
      printf 'kind %s\n' "$*" >>"$FAKE_LOG"
    fi
    ;;
  helm)
    if [ "${1:-} ${2:-}" = "upgrade --help" ]; then
      echo --force-conflicts
    else
      printf 'helm' >>"$FAKE_LOG"
      printf ' <%s>' "$@" >>"$FAKE_LOG"
      printf '\n' >>"$FAKE_LOG"
    fi
    ;;
  kubectl)
    case " $* " in
      *" apply -f - "*) cat >/dev/null ;;
      *" create namespace "*) echo 'apiVersion: v1' ;;
      *" get validatingwebhookconfiguration "*) echo ZmFrZQ== ;;
      *" get endpointslices "*) echo 10.0.0.1 ;;
      *" get clusterbasemodel "*) echo Ready ;;
    esac
    ;;
  python3)
    case " $* " in
      *" --images "*) ;;
      *" --case-images "*) ;;
      *" --value OME_EVAL_SCHEDULER"*) ;;
      *) echo '{}' ;;
    esac
    ;;
esac
FAKE
chmod +x "${fake_bin}/fake-tool"
for tool in docker kind helm kubectl python3; do
  ln -s fake-tool "${fake_bin}/${tool}"
done

FAKE_MANAGER_ID="$manager_a" \
FAKE_AGENT_ID="$agent_a" \
FAKE_LOG="$fake_log" \
OME_IMAGE_TAG=source \
PATH="${fake_bin}:$PATH" \
  "$repo_root/hack/omenative-testing/deploy-ome-kind.sh" --skip-build >/dev/null

grep -Fqx "docker image tag ome-manager:source ome-manager:${tag}" "$fake_log" ||
  fail "manager image was not retagged with the common deploy tag"
grep -Fqx "docker image tag ome-agent:source ome-agent:${tag}" "$fake_log" ||
  fail "agent image was not retagged with the common deploy tag"
grep -F -- "<--set> <ome.controller.tag=${tag}>" "$fake_log" >/dev/null ||
  fail "Helm did not receive the common manager tag"
grep -F -- "<--set> <ome.omeAgent.tag=${tag}>" "$fake_log" >/dev/null ||
  fail "Helm did not receive the common agent tag"
if grep -Fq 'docker build' "$fake_log"; then
  fail "--skip-build invoked a Docker build"
fi

echo "deploy-ome-kind tag tests passed."
