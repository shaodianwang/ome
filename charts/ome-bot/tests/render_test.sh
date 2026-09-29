#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-bot chart test: $*" >&2
  exit 1
}

cluster='clusters=[{"name":"local","provider":"mks","inCluster":true}]'
secrets='secrets.volume={"secret":{"secretName":"ome-bot"}}'

render() {
  "${helm_bin}" template rel "${chart_dir}" --namespace ome-bot "$@"
}

# --- guards fire instead of rendering an unusable Deployment ---

if render --set-json "${secrets}" >/dev/null 2>&1; then
  fail "an empty registry rendered instead of failing"
fi
if render --set-json "${cluster}" >/dev/null 2>&1; then
  fail "hmac auth without secrets.volume rendered instead of failing"
fi
if render --set-json "${cluster}" --set config.auth.mode=none --set route.enabled=true >/dev/null 2>&1; then
  fail "a route in front of an unauthenticated API rendered instead of failing"
fi
render --set-json "${cluster}" --set config.auth.mode=none >/dev/null ||
  fail "auth.mode none without secrets did not render"

# --- default values with one cluster and a secrets volume ---

default="$(render --set-json "${cluster}" --set-json "${secrets}")"

grep -Fq -- '--config=/etc/ome-bot/config/config.yaml' <<<"${default}" ||
  fail "the --config flag does not point at the config mount"
grep -Fq 'mountPath: /etc/ome-bot/config' <<<"${default}" ||
  fail "the config ConfigMap is not mounted at /etc/ome-bot/config"
grep -Fq 'mountPath: /etc/ome-bot/secrets' <<<"${default}" ||
  fail "the secrets volume is not mounted at /etc/ome-bot/secrets"
grep -Fq 'secretName: ome-bot' <<<"${default}" ||
  fail "the secrets volume source was not rendered verbatim"
grep -Fq 'secretFile: "/etc/ome-bot/secrets/hmac"' <<<"${default}" ||
  fail "the auth key path is not under the secrets mount"
for key in 'maxRequestBytes: 65536' 'maxInFlight: 8' 'readHeaderSeconds: 10' 'shutdownSeconds: 10' \
  'maxMatches: 50' 'maxReadBytes: 24000' 'path: /opt/ome/upstream/site/content/en/docs'; do
  grep -Fq "${key}" <<<"${default}" || fail "config.yaml lacks ${key}"
done
if grep -Eq '^    (tools|kargo):' <<<"${default}"; then
  fail "empty tools or kargo values rendered a config block"
fi

# --- tools and kargo render verbatim when set ---

extras="$(render --set-json "${cluster}" --set-json "${secrets}" \
  --set-json 'config.tools=[{"name":"builds","command":"/usr/bin/python3","subcommands":["list"],"timeoutSeconds":60,"envFromFiles":{"API_TOKEN":"/etc/ome-bot/secrets/token"}}]' \
  --set-json 'config.kargo={"cluster":"local","namespace":"kargo-proj-a","recentFreight":5}')"

for key in 'name: builds' 'API_TOKEN: /etc/ome-bot/secrets/token' 'timeoutSeconds: 60' \
  'namespace: kargo-proj-a' 'recentFreight: 5'; do
  grep -Fq -- "${key}" <<<"${extras}" || fail "config.yaml lacks ${key}"
done
grep -Eq '^    kargo:' <<<"${extras}" || fail "kargo block did not render"

# --- docs can be switched off ---

nodocs="$(render --set-json "${cluster}" --set-json "${secrets}" --set config.docs=null)"
if grep -Eq '^    docs:' <<<"${nodocs}"; then
  fail "empty docs values rendered a config block"
fi

echo "ome-bot chart test: ok"
