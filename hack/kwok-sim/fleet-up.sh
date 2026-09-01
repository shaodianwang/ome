#!/usr/bin/env bash
# Stand up a three-cluster quota fleet on kind: one management plane and two
# workload clusters, wired together the way a real deployment is.
#
# What this exists to exercise, and what up.sh on one cluster cannot:
#
#   - The transport. A hub reaching a member over a kubeconfig it was handed,
#     rather than a client a test constructed.
#   - The member-issued grant. The hub authenticates as ome-quota-access, whose
#     ClusterRole allows exactly acceleratorquotas and nothing else — so a
#     projector that reached for a Node, a ResourceFlavor or a status write
#     would be refused here and pass everywhere it is tested with admin
#     credentials.
#   - The split against capacity the members actually derived from their own
#     (simulated) hardware, rather than numbers a fixture wrote.
#
# The two members are deliberately unequal — one cube against two — so a
# proportional split has to produce different numbers on each, and a bug that
# handed both the fleet total would be visible rather than plausible.
#
# On a host with no direct egress, wrap the whole invocation rather than any
# step inside it — the chart and image pulls all happen in child processes and
# inherit the environment:
#
#   with-proxy hack/kwok-sim/fleet-up.sh
#
# Usage:
#   hack/kwok-sim/fleet-up.sh [--skip-build] [--component NAME]... [--with-kueue]
#                             [--with-placement] [--topology MODE]
#                             [--cubes A-SPEC;B-SPEC]
#
#   --cubes A;B     the silicon each member starts with, one spec per member,
#                   separated by a semicolon. Within a member the syntax is
#                   up.sh's: comma-separated cubes, each a carve topology.
#                   Default: 4x4x4,4x4x4;4x4x4 — two cubes on member A and one
#                   on member B, deliberately unequal so a proportional split
#                   has to produce different numbers on each and a bug that
#                   handed both the fleet total would be visible rather than
#                   plausible.
#
#                   Re-runnable, and additive. Node names and addresses derive
#                   from the cube index, so a later run with more cubes keeps
#                   every existing node and adds the new ones:
#
#                     fleet-up.sh --cubes '4x4x4;2x2x2'
#                     fleet-up.sh --cubes '4x4x4,4x4x4;2x2x2' --skip-build
#
#                   grows member A by a cube in place. Shrinking does NOT
#                   remove nodes: nothing prunes, so a smaller spec leaves the
#                   surplus behind. Tear the fleet down for that.
#
#                   Changing this changes what a proportional split divides by,
#                   so a fixture's numbers stop matching the hardware. The
#                   closing banner prints the chips each member ends up with;
#                   author a tree against those.
#
#   --component NAME
#                   which OME components to deploy on each WORKLOAD MEMBER;
#                   repeat the flag for more than one. Default: quota-manager.
#                   quota-manager must stay in the set — member A's run is the
#                   only thing that builds the image the hub then side-loads.
#   --with-kueue    install Kueue itself on each member, not just its CRDs, so a
#                   rendered ClusterQueue is judged by the thing that judges it
#                   in production. Adds an image pull and a load per member.
#   --with-placement
#                   also exercise OEP-0018 fan-out, not only the quota plane.
#                   Adds `manager` and `scheduler` to the member component set,
#                   widens the members' grant so the hub may write derived
#                   InferenceServices there, and labels each WorkloadCluster so
#                   an ISVC's placement requirements have something to match.
#                   Pair it with --with-kueue: a derived workload carries Kueue's
#                   queue-name label, so without an admitting Kueue and a tree
#                   its pods gate and never run. See fixtures/6*-placement-*.
#   --topology M    podgroup (default) | plugin | strict, forwarded to each
#                   member. Needs --with-placement, which is what puts a
#                   scheduler there for the mode to mean anything.
#   --skip-build    reuse the images already in the host docker daemon.
#
# Every flag above reaches the members only; the hub's component set is fixed.
# It runs the quota manager in management mode, which never reads or writes a
# Kueue object — capacity derivation and the Kueue backend are both
# workload-mode-only — so there is nothing there for Kueue to do. It schedules
# nothing either, so a scheduler and simulated silicon would be dead weight.
#
# The hub also runs ome-manager in the control-plane role, and must: the
# WorkloadCluster registry reconciler lives there and is the single writer of
# the Ready condition. The quota plane deliberately only reads that condition,
# so with no registry nothing is ever reachable, and a projector that cannot
# see the whole fleet holds every projection rather than splitting a budget
# across the part of it that answered. The role also switches OFF the local
# InferenceService, BaseModel and ServingRuntime controllers, which have no
# catalog to reconcile on a cluster that carries no silicon.
#
# Flags up.sh takes that this script does NOT forward are rejected by name:
# --cluster, --cubes and --block are set per member here, and forwarding one
# would apply a single value to both.
#
# Teardown: hack/kwok-sim/fleet-down.sh
set -euo pipefail

# Before anything reads a repo-relative path.
. "$(dirname "${BASH_SOURCE[0]}")/lib/kind.sh"
. "$(dirname "${BASH_SOURCE[0]}")/lib/fixtures.sh"
cd "$KWOK_REPO_ROOT"

HUB="${OME_FLEET_HUB:-ome-quota-hub}"
MEMBER_A="${OME_FLEET_MEMBER_A:-ome-quota-a}"
MEMBER_B="${OME_FLEET_MEMBER_B:-ome-quota-b}"
OME_NS=ome
ORIGIN="${OME_FLEET_ORIGIN:-kwok-hub}"
PROJECTOR="${OME_FLEET_FIELD_MANAGER:-ome-quota-projector}"
SKIP_BUILD=0
# Empty means the default below. Space-separated rather than an array: the
# values are validated component names, which cannot contain a space, and
# expanding an empty array under `set -u` is an error on the bash 3.2 that is
# still macOS's default.
COMPONENTS=""
WITH_KUEUE=0
WITH_PLACEMENT=0
# Per-member silicon, "<member-a cubes>;<member-b cubes>". Deliberately unequal:
# see --cubes above.
DEFAULT_CUBES="4x4x4,4x4x4;4x4x4"
CUBES="$DEFAULT_CUBES"
# Empty means up.sh's own default. Only forwarded under --with-placement, which
# is what puts a manager and a scheduler on the members for it to mean anything.
TOPOLOGY=""
# The same node image up.sh pins. Without it kind picks its own built-in
# default, which tracks the CLI rather than this repo — so the fleet would run a
# different Kubernetes from every other simulator and every test lane, which is
# exactly the drift .kind-node-image exists to prevent.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-$(tr -d '[:space:]' <.kind-node-image)}"

while [ $# -gt 0 ]; do
  # A value-taking flag left last on the line leaves $2 unset, which `set -u`
  # reports as "$2: unbound variable" — a bash internal, from a line number, for
  # what is an ordinary usage mistake.
  case "$1" in
    --component|--topology|--cubes)
      [ $# -ge 2 ] || { echo "$1 needs a value" >&2; exit 2; } ;;
  esac
  case "$1" in
    --component) COMPONENTS="${COMPONENTS} $2"; shift 2 ;;
    --with-kueue) WITH_KUEUE=1; shift ;;
    --with-placement) WITH_PLACEMENT=1; shift ;;
    --topology) TOPOLOGY="$2"; shift 2 ;;
    --cubes) CUBES="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    # Named rather than left to the catch-all: each is a flag up.sh really does
    # take, so "unknown flag" would be a lie, and the reason it is refused is
    # not guessable from the name.
    --cluster|--block)
      echo "$1 is set per member by this script; it cannot be forwarded" >&2
      exit 2 ;;
    --ip-prefix)
      echo "$1 is detected from the docker bridge; an off-link value wedges kindnetd" >&2
      exit 2 ;;
    --with-isvcs)
      echo "$1 is not forwarded: a fleet member's workloads arrive from the hub" >&2
      exit 2 ;;
    --no-scheduler)
      echo "$1 would subtract the component --with-placement adds; omit --with-placement instead" >&2
      exit 2 ;;
    -h|--help) sed -n '2,71p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# Validated here rather than left to up.sh: up.sh does make both of these
# checks, but its run does not begin until three kind clusters exist and the
# kwok image has been pulled, saved and loaded into all of them. A typo should
# cost a second, not several minutes.
COMPONENTS="${COMPONENTS:-quota-manager}"
# The membership idiom up.sh uses, for the reason it gives: bash 3.2 is still
# macOS's default and errors on expanding an empty array under `set -u`.
in_list() { case " $2 " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# Placement needs both on every member: the manager to reconcile a derived ISVC
# into pods, and the scheduler because the runtimes render with
# schedulerName: ome-scheduler under the default topology mode, so without it
# every gang sits Pending. Added rather than required so --with-placement is the
# only thing anyone has to remember.
if [ "$WITH_PLACEMENT" -eq 1 ]; then
  for c in manager scheduler; do
    in_list "$c" "$COMPONENTS" || COMPONENTS="${COMPONENTS} ${c}"
  done
fi

[ -z "$TOPOLOGY" ] || [ "$WITH_PLACEMENT" -eq 1 ] || {
  echo "--topology needs --with-placement, which is what puts a scheduler on the members" >&2
  exit 2; }

for c in $COMPONENTS; do
  in_list "$c" "manager scheduler quota-manager" || {
    echo "unknown component: $c (want one of: manager scheduler quota-manager)" >&2
    exit 2; }
done
# The one invariant with no up.sh equivalent. Member A's run is the sole builder
# of the ome-quota-manager image; member B loads it and the hub side-loads it,
# both straight from the host daemon. Drop quota-manager and the build silently
# does not happen, and the run dies at `docker save` on the hub with an error
# naming neither the flag nor the cause.
in_list quota-manager "$COMPONENTS" || {
  echo "--component must include quota-manager: it builds the image the hub side-loads" >&2
  exit 2; }

# One spec per member, semicolon-separated. Split before any cluster work: a
# malformed spec should cost a second, not the several minutes it takes to
# discover it when the generator is finally reached.
case "$CUBES" in
  *\;*\;*) echo "--cubes takes two specs separated by one ';', got: $CUBES" >&2; exit 2 ;;
  *\;*) ;;
  *) echo "--cubes needs a spec per member, e.g. '4x4x4;2x2x2', got: $CUBES" >&2; exit 2 ;;
esac
CUBES_A="${CUBES%%;*}"
CUBES_B="${CUBES#*;}"
for spec in "$CUBES_A" "$CUBES_B"; do
  [ -n "$spec" ] || { echo "--cubes has an empty member spec: $CUBES" >&2; exit 2; }
done

# What the generator will actually mint, asked of the generator rather than
# recomputed here: the carve arithmetic lives there, and a second copy of it
# would be a second thing to get wrong.
chips_for() { # $1 = a member's cube spec
  hack/kwok-sim/gen-tpu-nodes.sh --cubes "$1" --block 1 2>/dev/null | awk '
    /^kind: Node$/            { nodes++ }
    /google\.com\/tpu:/ && !per { gsub(/"/, "", $2); per = $2 }
    END                       { print nodes * per }'
}

resolve_kind

# A rebuild reuses the tag, so the Deployment's image ref is byte-identical and
# nothing rolls: helm reports success, the load reports success, and the pod
# keeps serving the previous binary. up.sh stamps the image id into a pod
# annotation for the members; the hub needs the same or a re-run leaves it on
# the old code while both members move.
image_id() { docker image inspect -f '{{.Id}}' "$1" | cut -d: -f2 | cut -c1-16; }

hubctl()  { kubectl --context "kind-${HUB}" "$@"; }
actl()    { kubectl --context "kind-${MEMBER_A}" "$@"; }
bctl()    { kubectl --context "kind-${MEMBER_B}" "$@"; }

# Each cluster gets its own pod and service CIDRs. kind's default is the same
# 10.244.0.0/16 everywhere, and three clusters on one docker bridge programming
# routes for the same prefix is a silent packet-loss story rather than an error.
kind_config() { # $1 = name, $2 = pod CIDR index
  cat <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${1}
nodes:
- role: control-plane
  image: ${KIND_NODE_IMAGE}
networking:
  podSubnet: "10.2${2}.0.0/16"
  serviceSubnet: "10.9${2}.0.0/16"
YAML
}

echo "==> three clusters"
i=0
for c in "$HUB" "$MEMBER_A" "$MEMBER_B"; do
  if "$KIND" get clusters 2>/dev/null | grep -qx "$c"; then
    echo "    ${c} exists"
  else
    kind_config "$c" "$i" | "$KIND" create cluster --config - >/dev/null
    echo "    ${c} created"
  fi
  i=$((i + 1))
done

# kwok's controller image, side-loaded into every cluster before anything asks
# for it. A kind node reaches no registry on a host without egress, and the
# failure surfaces as a helm --wait timing out several minutes later rather than
# as anything naming an image. The daemon on the host can pull it, so the pull
# happens once and the bytes are pushed in.
KWOK_IMAGE="${KWOK_IMAGE:-registry.k8s.io/kwok/kwok:v0.8.0}"
echo "==> side-loading ${KWOK_IMAGE}"
if [ "$(docker image inspect -f '{{.Architecture}}' "$KWOK_IMAGE" 2>/dev/null)" != "$(docker version --format '{{.Server.Arch}}')" ]; then
  docker pull --platform "linux/$(docker version --format '{{.Server.Arch}}')" "$KWOK_IMAGE" >/dev/null
fi
KWOK_TAR="$(mktemp -t kwok-image-XXXXXX.tar)"
docker save --platform "linux/$(docker version --format '{{.Server.Arch}}')" "$KWOK_IMAGE" -o "$KWOK_TAR"
# Members only. The hub runs no kwok and no fake nodes, so an image loaded there
# is one nothing will ever start.
for c in "$MEMBER_A" "$MEMBER_B"; do
  "$KIND" load image-archive "$KWOK_TAR" --name "$c" >/dev/null
done
rm -f "$KWOK_TAR"

# The members carry simulated silicon; the hub carries none. A management plane
# measures nothing of its own — it learns capacity by reading each member's root.
#
# --block is the reservation-block ordinal, and it differs per member so the two
# mint distinguishable node names: the generator hashes it into every pool id,
# and identical names across clusters make a log line unreadable as belonging to
# one of them. It is formatted with %04d, so it has to be a number.
#
# No --ip-prefix here. Those addresses sit on fake nodes that route nowhere, so
# they need not differ between clusters, and pinning one would bake in an
# assumption about the host's docker bridge — a /24 on some hosts and something
# else on others, where an off-link prefix is the failure the generator's own
# comment warns about. up.sh takes the flag for anyone who needs a layout.
# Unquoted on purpose: this is a list of validated, space-free tokens, and it
# has to word-split to become separate arguments.
UP_FLAGS=""
for c in $COMPONENTS; do UP_FLAGS="${UP_FLAGS} --component ${c}"; done
[ "$WITH_KUEUE" -eq 1 ] && UP_FLAGS="${UP_FLAGS} --with-kueue"
[ -n "$TOPOLOGY" ] && UP_FLAGS="${UP_FLAGS} --topology ${TOPOLOGY}"

echo "==> member A: ${CUBES_A}"
OME_KWOK_CLUSTER="$MEMBER_A" hack/kwok-sim/up.sh \
  --cluster "$MEMBER_A" $UP_FLAGS \
  --cubes "$CUBES_A" --block 1 \
  $([ "$SKIP_BUILD" -eq 1 ] && echo --skip-build) >/dev/null

echo "==> member B: ${CUBES_B}"
OME_KWOK_CLUSTER="$MEMBER_B" hack/kwok-sim/up.sh \
  --cluster "$MEMBER_B" $UP_FLAGS \
  --cubes "$CUBES_B" --block 2 \
  --skip-build >/dev/null

# Before any tree is authored, the way a real cluster has them before anyone
# budgets against one. Both members get every namespace the fleet fixtures bind,
# not just the one the fixture in hand uses, so switching between fixtures needs
# no second step.
echo "==> the namespaces the fleet fixtures bind"
for ctx in "kind-${MEMBER_A}" "kind-${MEMBER_B}"; do
  ensure_namespaces "$ctx" hack/kwok-sim/fixtures/5*-fleet-*.yaml
done

echo "==> the member-issued grant on each member"
# Only the remote-access objects, rendered from the chart and applied directly.
# A full `helm upgrade` re-sends the whole release, including the webhook config
# whose caBundle the manager's own cert rotator owns — helm's apply collides
# with that field, and forcing the conflict hands it to helm, which blanks it
# and leaves the webhook unable to verify until the rotator writes it back.
for ctx in "kind-${MEMBER_A}" "kind-${MEMBER_B}"; do
  helm template ome-quota-manager charts/ome-quota-manager/ \
    --namespace "$OME_NS" --show-only templates/remote_access.yaml \
    --set quotaManager.mode=workload \
    --set quotaManager.remoteAccess.enabled=true \
    --set quotaManager.remoteAccess.serviceAccount.staticToken=true \
  | kubectl --context "$ctx" apply --server-side --force-conflicts -f - >/dev/null
done

# One WorkloadCluster carries one credential, and the placer shares it with the
# projector — so placing as well as budgeting means widening the identity the
# members already issued rather than minting a second one.
#
# The ClusterRole is the chart's own, rendered the same way the quota grant
# above is, so the fleet exercises the grant a real control plane is given
# instead of a copy of it. Its ServiceAccount and token come along and go
# unused; only the rules matter here. The binding onto ome-quota-access is the
# fixture, because the chart has no reason to know about this fleet's identity.
if [ "$WITH_PLACEMENT" -eq 1 ]; then
  echo "==> the placement grant on each member"
  for ctx in "kind-${MEMBER_A}" "kind-${MEMBER_B}"; do
    helm template ome charts/ome-resources/ \
      --namespace "$OME_NS" --show-only templates/ome-controller/rbac/multicluster_access.yaml \
      --set ome.multiclusterAccess.enabled=true \
    | kubectl --context "$ctx" apply --server-side --force-conflicts -f - >/dev/null
    kubectl --context "$ctx" apply -f hack/kwok-sim/fixtures/60-placement-rbac.yaml >/dev/null
  done
fi

# Same script the members run, with the silicon left out and the manager put in
# the control-plane role. Reused rather than reimplemented: standing up
# ome-manager means cert-manager, a two-pass helm install around a fail-closed
# webhook, and a wait for the caBundle and ready endpoints — all of which up.sh
# already gets right, and a second copy of would drift from.
#
# This also installs the OME CRDs on the hub, so there is no separate ome-crd
# step here.
echo "==> hub: ome-manager in the control-plane role"
OME_KWOK_CLUSTER="$HUB" hack/kwok-sim/up.sh \
  --cluster "$HUB" --component manager \
  --no-nodes --multicluster-role control-plane \
  $([ "$SKIP_BUILD" -eq 1 ] && echo --skip-build) >/dev/null

echo "==> hub: the management-mode quota manager"

# The image the members already run, side-loaded rather than pulled: the kind
# nodes have no registry egress.
QUOTA_IMAGE="ome-quota-manager:${OME_IMAGE_TAG:-kind-local}"
TAR="$(mktemp -t quota-image-XXXXXX.tar)"
docker save "$QUOTA_IMAGE" -o "$TAR"
"$KIND" load image-archive "$TAR" --name "$HUB" >/dev/null
rm -f "$TAR"

helm upgrade --install ome-quota-manager charts/ome-quota-manager/ \
  --kube-context "kind-${HUB}" --namespace "$OME_NS" --create-namespace \
  --set global.hub="" \
  --set quotaManager.image.tag="${OME_IMAGE_TAG:-kind-local}" \
  --set quotaManager.mode=management \
  --set quotaManager.crd.install=false \
  --set quotaManager.replicaCount=1 \
  --set quotaManager.leaderElect=false \
  --set quotaManager.podDisruptionBudget.enabled=false \
  --set quotaManager.projection.origin="$ORIGIN" \
  --set quotaManager.projection.fieldManager="$PROJECTOR" \
  --set quotaManager.projection.defaultDistributionPolicy=Proportional \
  --set quotaManager.podAnnotations.kwok-sim/image-id="$(image_id "$QUOTA_IMAGE")" \
  --wait --timeout 300s >/dev/null

echo "==> registering the members with the hub"
# The hub authenticates as ome-quota-access, NOT as an admin. That is the point:
# a projector reaching past what the members granted is refused here. Without
# --with-placement that grant is acceleratorquotas and nothing else; with it,
# the placement ClusterRole above is bound onto the same identity, and the
# refusal still holds for everything neither of them names.
register() { # $1 = member cluster, $2 = kubectl fn, $3 = WorkloadCluster name, $4 = zone
  local server token ca secret="$3-kubeconfig"
  server="https://${1}-control-plane:6443"
  # The token controller populates the secret after it is created, so the
  # secret existing is not the same as the secret holding a token.
  for _ in $(seq 60); do
    token="$($2 -n "$OME_NS" get secret ome-quota-access-token \
      -o jsonpath='{.data.token}' 2>/dev/null || true)"
    [ -n "$token" ] && break
    sleep 1
  done
  [ -n "$token" ] || { echo "no token issued on ${1}" >&2; return 1; }
  token="$(printf %s "$token" | base64 -d)"
  ca="$($2 -n "$OME_NS" get secret ome-quota-access-token -o jsonpath='{.data.ca\.crt}')"

  hubctl -n "$OME_NS" delete secret "$secret" --ignore-not-found >/dev/null
  hubctl -n "$OME_NS" create secret generic "$secret" \
    --from-literal=kubeconfig="$(cat <<YAML
apiVersion: v1
kind: Config
clusters:
- name: member
  cluster:
    server: ${server}
    certificate-authority-data: ${ca}
users:
- name: quota
  user:
    token: ${token}
contexts:
- name: member
  context: {cluster: member, user: quota}
current-context: member
YAML
)" >/dev/null

  # The labels are the placer's whole candidate-selection input: an ISVC's
  # spec.placement.requirements is matched against them and nothing else, and an
  # ISVC declaring no requirement matches no cluster. accelerator is what every
  # placement fixture requires; zone tells the two members apart so one can be
  # singled out. Harmless to the quota plane, which never reads them.
  hubctl apply -f - >/dev/null <<YAML
apiVersion: ome.io/v1beta1
kind: WorkloadCluster
metadata:
  name: ${3}
  labels:
    accelerator: tpu7x
    zone: ${4}
spec:
  clusterSource:
    kubeConfig:
      secretRef: {name: ${secret}, namespace: ${OME_NS}}
YAML
}

register "$MEMBER_A" actl member-a a
register "$MEMBER_B" bctl member-b b

# The hub and the members are separate kind clusters on one docker bridge, so
# the members' apiservers are reachable by container name — which is what the
# server URL above uses, and why it is not the 127.0.0.1 form kind writes by
# default.
echo "==> waiting for both members to report Ready"
for _ in $(seq 60); do
  ready=$(hubctl get workloadclusters -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null | grep -c True || true)
  [ "${ready:-0}" -eq 2 ] && break
  sleep 2
done
hubctl get workloadclusters

# No tree is applied, for the reason the single-cluster fixtures give: a tree is
# what an operator is here to write, and the fleet stands up without one. It also
# keeps the harness from prejudging which split is being exercised.
CHIPS_A="$(chips_for "$CUBES_A")"
CHIPS_B="$(chips_for "$CUBES_B")"
cat <<DONE

The fleet is up: a hub holding no silicon, and two members holding ${CHIPS_A} and
${CHIPS_B} chips. Nothing is budgeted yet. Author a tree on the hub to project one:

  kubectl --context kind-ome-quota-hub apply -f hack/kwok-sim/fixtures/<one of>
DONE

if [ "$CUBES" = "$DEFAULT_CUBES" ]; then
  cat <<'DONE'

  50-fleet-proportional.yaml   192 split against capacity      -> 128 / 64
  51-fleet-explicit.yaml        96 split by hand, inverted     ->  32 / 64
  52-fleet-mixed.yaml          two tenants, one policy each    ->  80 / 64
  53-fleet-oversubscribed.yaml a share member-b cannot hold    -> member-b frozen
DONE
else
  # The fixtures carry fixed numbers, so a non-default sizing makes their
  # arithmetic wrong rather than merely different. Say so instead of printing a
  # table that no longer describes this fleet.
  cat <<DONE

  The 5*-fleet-* and 6*-placement-* fixtures assume 192 chips fleet-wide, which
  is the default sizing and not this one. Author a tree against $((CHIPS_A + CHIPS_B)) instead:
  a budget above what a member holds freezes it with CapacityExceeded, and one
  below leaves capacity unbudgeted.
DONE
fi

cat <<'DONE'

  hack/kwok-sim/fleet-status.sh      # what landed where
  hack/kwok-sim/fleet-down.sh        # tear the fleet down
DONE

if [ "$WITH_PLACEMENT" -eq 1 ]; then
  cat <<'DONE'
Placement is wired: both members are labelled accelerator=tpu7x (zone=a, zone=b)
and the hub may write derived InferenceServices there. Apply a tree FIRST — a
derived workload carries Kueue's queue-name label, so with no LocalQueue in
kwok-sim its pods gate and never run — then one of:

  kubectl --context kind-ome-quota-hub apply -f hack/kwok-sim/fixtures/<one of>

  61-placement-all.yaml           every member serves        -> 16 pods each
  62-placement-single.yaml        race, then sweep the loser -> one winner
  63-placement-split-spread.yaml  3 replicas, 192 chips      -> 2 on a, 1 on b
  64-placement-split-capped.yaml  a per-cluster ceiling      -> 1 each
  65-placement-pinned.yaml        clusterSelector zone=b     -> member-b only
  66-placement-nomatch.yaml       matches nothing            -> never placed

They all charge the same leaf, so run one at a time.
DONE
fi
