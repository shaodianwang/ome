#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-quota-manager chart test: $*" >&2
  exit 1
}

# Release name deliberately unlike the component name: object names come from
# quotaManager.name, and a release-derived name would slip through unnoticed if
# the two matched.
render() {
  "${helm_bin}" template rel "${chart_dir}" --namespace ome "$@"
}

# --- mode is required, and the guard must fire rather than render a bad flag ---

if render >/dev/null 2>&1; then
  fail "an unset mode rendered instead of failing"
fi
for mode in workload management; do
  render --set quotaManager.mode="${mode}" >/dev/null ||
    fail "mode ${mode} did not render"
done
if render --set quotaManager.mode=both >/dev/null 2>&1; then
  fail "an invalid mode rendered instead of failing"
fi

# --- object names track quotaManager.name, never the release ---

default="$(render --set quotaManager.mode=workload)"

grep -Fq 'name: ome-quota-manager' <<<"${default}" ||
  fail "objects are not named from quotaManager.name"
if grep -Eq '^\s+name: rel(-|$)' <<<"${default}"; then
  fail "an object was named from the release instead of quotaManager.name"
fi

renamed="$(render --set quotaManager.mode=workload --set quotaManager.name=aq-mgr)"
grep -Fq 'name: aq-mgr' <<<"${renamed}" ||
  fail "overriding quotaManager.name did not rename objects"
# The RBAC scopes itself by name, so a rename that misses one grant would leave
# the component unable to inject its own caBundle.
grep -Fq 'resourceNames: ["aq-mgr.ome.io"]' <<<"${renamed}" ||
  fail "the webhook-config grant did not follow the rename"
grep -Fq 'resourceNames: ["aq-mgr-webhook-cert"]' <<<"${renamed}" ||
  fail "the secret grant did not follow the rename"
grep -Fq -- '--webhook-config-name=aq-mgr.ome.io' <<<"${renamed}" ||
  fail "the binary flag did not follow the rename"

# --- internal cert management: the out-of-the-box default ---

grep -Fq -- '--internal-cert-management=true' <<<"${default}" ||
  fail "internal cert management is not the default"
grep -Fq -- '--cert-namespace=ome' <<<"${default}" ||
  fail "the cert namespace was not passed"
grep -Fq -- '--cert-secret-name=ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the cert secret name was not passed"
grep -Fq -- '--webhook-config-name=ome-quota-manager.ome.io' <<<"${default}" ||
  fail "the webhook config name was not passed"

# The rotator only ever Updates the Secret, so the chart creating it empty is
# load-bearing: without it the component never becomes ready.
grep -Fq 'name: ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the placeholder cert Secret was not rendered"
grep -Fq 'helm.sh/resource-policy: keep' <<<"${default}" ||
  fail "the cert Secret is not retained across an uninstall"

# The rotator writes the Secret and the kubelet projects it back down, so the
# mount is the delivery path, not just the cert-manager hand-off.
grep -Fq 'secretName: ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the cert Secret is not mounted"

if grep -Fq 'cert-manager.io' <<<"${default}"; then
  fail "the default install still references cert-manager"
fi

# Self-injection is scoped to this component's own webhook config. A grant that
# lost its resourceNames could rewrite ome-manager's fail-closed pod mutator.
grep -Fq 'resourceNames: ["ome-quota-manager.ome.io"]' <<<"${default}" ||
  fail "webhook-config update is not scoped by name"
if grep -Fq 'resources: ["mutatingwebhookconfigurations"]' <<<"${default}"; then
  fail "the quota plane was granted access to mutating webhooks"
fi
grep -Fq 'resourceNames: ["ome-quota-manager-webhook-cert"]' <<<"${default}" ||
  fail "secret update is not scoped by name"

# --- capacity derivation carries its own RBAC, and only in workload mode ---

# The grants and the flags have to appear together: flags without the grants is
# a crash-looping RBAC error, grants without the flags is a cluster-wide Node
# read nothing uses.
grep -Fq 'resources: ["nodes"]' <<<"${default}" ||
  fail "workload mode did not grant the Node read capacity derivation needs"
grep -Fq 'resources: ["resourceflavors"]' <<<"${default}" ||
  fail "workload mode did not grant the ResourceFlavor read"
grep -Fq -- '--accelerator-resources=google.com/tpu,nvidia.com/gpu' <<<"${default}" ||
  fail "the accelerator suffixes were not passed"
grep -Fq -- '--capacity-hysteresis-percent=10' <<<"${default}" ||
  fail "the hysteresis band was not passed"

# A management-mode install holds the authored fleet tree and has no local
# silicon, so a cluster-wide Node read there would be unused privilege.
management="$(render --set quotaManager.mode=management)"
for unwanted in 'resources: ["nodes"]' 'resources: ["resourceflavors"]' '--accelerator-resources'; do
  if grep -Fq -- "${unwanted}" <<<"${management}"; then
    fail "management mode rendered ${unwanted}"
  fi
done

# Turning derivation off must drop the grants with it, or the privilege
# outlives the feature that justified it. Every spelling an operator might
# reach for, because Helm deletes a defaulted map on a null rather than merging
# it and an unguarded lookup then aborts the whole render.
for off in \
  'quotaManager.capacity.resources=null' \
  'quotaManager.capacity=null' \
  'quotaManager.capacity.resources={}' \
  ; do
  nocapacity="$(render --set quotaManager.mode=workload --set "${off}")" ||
    fail "--set ${off} failed to render at all"
  for unwanted in 'resources: ["nodes"]' 'resources: ["resourceflavors"]' '--accelerator-resources'; do
    if grep -Fq -- "${unwanted}" <<<"${nocapacity}"; then
      fail "--set ${off} still rendered ${unwanted}"
    fi
  done
done

# A blank entry is not a resource. The chart must agree with the binary, which
# trims and drops blanks — otherwise a list of blanks grants cluster-wide Node
# reads for a feature the binary treats as switched off.
blanks="$(render --set quotaManager.mode=workload --set 'quotaManager.capacity.resources[0]=')"
if grep -Fq -- 'resources: ["nodes"]' <<<"${blanks}"; then
  fail "a list of blank resources still granted the Node read"
fi

# A flag rendered with no value crash-loops the pod on parse.
allrendered="$(render --set quotaManager.mode=workload --set quotaManager.capacity.hysteresisPercent=null)"
if grep -Eq -- '--[a-z-]+=(")?$' <<<"${allrendered}"; then
  fail "a flag rendered with an empty value"
fi

# --- the CRD is opt-in, and retained when it is opted into ---

if grep -Fq 'kind: CustomResourceDefinition' <<<"${default}"; then
  fail "the CRD rendered by default, which collides with ome-crd"
fi

withcrd="$(render --set quotaManager.mode=workload --set quotaManager.crd.install=true)"
grep -Fq 'name: acceleratorquotas.ome.io' <<<"${withcrd}" ||
  fail "opting in did not render the CRD"
# Deleting the CRD cascades to every AcceleratorQuota, and each of those deletes
# is gated by this chart's own fail-closed webhook — which by uninstall time has
# no endpoints. Without the retention annotation the CRD wedges in Terminating.
"${helm_bin}" template rel "${chart_dir}" --namespace ome \
  --set quotaManager.mode=workload --set quotaManager.crd.install=true \
  --show-only templates/crd.yaml | grep -Fq 'helm.sh/resource-policy: keep' ||
  fail "the opted-in CRD is not retained on uninstall"

# --- cert-manager remains available as an opt-out ---

certmanager="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.internalCertManagement=false)"

grep -Fq 'kind: Certificate' <<<"${certmanager}" ||
  fail "opting out did not render a cert-manager Certificate"
grep -Fq 'cert-manager.io/inject-ca-from: ome/ome-quota-manager-serving-cert' <<<"${certmanager}" ||
  fail "opting out did not annotate the webhook config for cainjector"
grep -Fq 'secretName: ome-quota-manager-webhook-cert' <<<"${certmanager}" ||
  fail "opting out did not mount the cert Secret"
if grep -Fq 'resources: ["validatingwebhookconfigurations"]' <<<"${certmanager}"; then
  fail "opting out left the self-injection RBAC in place"
fi

# --- disabling the webhook drops the whole cert story ---

nowebhook="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.enabled=false)"

for unwanted in ValidatingWebhookConfiguration cert-manager.io ome-quota-manager-webhook-cert; do
  if grep -Fq "${unwanted}" <<<"${nowebhook}"; then
    fail "disabling the webhook still rendered ${unwanted}"
  fi
done

echo "ome-quota-manager chart contracts OK"
