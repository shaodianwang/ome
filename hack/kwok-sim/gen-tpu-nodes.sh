#!/usr/bin/env bash
# Emit kwok-simulated GKE TPU nodes that mirror the shape of the reference TPU cluster.
#
# The unit of capacity is a CUBE: one tpu7x sub-block, 64 chips, 16 nodes of
# tpu7x-standard-4t (4 chips each). A cube is carved into node pools by slice
# topology, and every node in a pool advertises that pool's topology:
#
#   carve    chips/slice   nodes/pool   pools/cube
#   2x2x1        4             1            16
#   2x2x2        8             2             8
#   2x2x4       16             4             4
#   2x4x4       32             8             2
#   4x4x4       64            16             1
#
# Usage:
#   gen-tpu-nodes.sh [--cubes 4x4x4,2x2x2] [--model NAME] [--block 1] > nodes.yaml
#
#   --cubes   comma-separated carve per cube; one entry per cube. The default
#             "4x4x4,2x2x2" is two cubes = 32 nodes: cube 1 whole (one 16-node
#             4x4x4 pool), cube 2 split into eight 2-node 2x2x2 pools.
#   --model   ClusterBaseModel name to mark Ready on every node, via the
#             models.ome.io/clusterbasemodel.<name>=Ready label the OME model
#             agent would normally apply. OME's InferenceService controller
#             adds that key as a nodeSelector for non-PVC models, so without it
#             no engine pod can ever be placed. Repeat the flag for more models.
#   --block   reservation block ordinal baked into pool and reservation names.
#   --ip-prefix
#             first two octets of the kind bridge subnet. Each node gets its own
#             InternalIP under <prefix>.100+/16. Both halves matter:
#             * Unique. kwok stamps --node-ip, wired to the kwok pod's own
#               address, so nodes that do not carry one all report the real kind
#               node's IP. kindnetd finds "its" node by matching that IP, picks
#               a fake one, and rewrites the real node's CNI conflist with the
#               fake node's podCIDR — real pods then get unroutable addresses.
#             * Inside the bridge subnet. An off-link range leaves kindnetd in a
#               permanent route-reconcile failure loop.
#
# Every node carries a full .status. That is deliberate: the apiserver honours
# status on CREATE but drops it on apply, and a node that reaches kwok without
# capacity gets the node-initialize default of 1k CPU / 1Ti / no accelerators —
# a node that accepts any pod, which silently voids any scheduling result.
set -euo pipefail

CUBES="4x4x4,2x2x2"
BLOCK=1
IP_PREFIX="172.18"
MODELS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --cubes) CUBES="$2"; shift 2 ;;
    --model) MODELS+=("$2"); shift 2 ;;
    --block) BLOCK="$2"; shift 2 ;;
    --ip-prefix) IP_PREFIX="$2"; shift 2 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Stable 32-hex id, standing in for the opaque GCE topology hashes.
hash32() { printf '%s' "$1" | md5sum 2>/dev/null | cut -c1-32 || printf '%s' "$1" | md5 -q | cut -c1-32; }
# Short pool suffix, standing in for the per-pool hash in gke-tpu-<hash>-<n>.
hash8() { hash32 "$1" | cut -c1-8; }

CHIPS_PER_NODE=4
NODES_PER_CUBE=16

# tpu7x-standard-4t, as reported by the reference cluster's node pools. pods=32
# is the 4x4x4 pool's max-pods; the 2x2x2 pools allow 110. The smaller number
# everywhere is the conservative choice for a scheduling simulator.
emit_status() { # $1 = node name, $2 = ip
  cat <<YAML
status:
  addresses:
    - { type: InternalIP, address: ${2} }
    - { type: Hostname, address: ${1} }
  capacity:
    cpu: "224"
    ephemeral-storage: 303624428Ki
    google.com/tpu: "4"
    hugepages-1Gi: "0"
    hugepages-2Mi: "0"
    memory: 990700576Ki
    pods: "32"
  allocatable:
    cpu: 223370m
    ephemeral-storage: "172446089982"
    google.com/tpu: "4"
    hugepages-1Gi: "0"
    hugepages-2Mi: "0"
    memory: 963544096Ki
    pods: "32"
  nodeInfo:
    architecture: amd64
    containerRuntimeVersion: containerd://2.1.5
    kernelVersion: 6.8.0-1048-gke
    kubeletVersion: v1.35.3-gke.1585001
    operatingSystem: linux
    osImage: Ubuntu 24.04.4 LTS
YAML
}

node_seq=0
cube_idx=0
IFS=',' read -ra CARVES <<< "$CUBES"
for carve in "${CARVES[@]}"; do
  cube_idx=$((cube_idx + 1))
  IFS='x' read -r dx dy dz <<< "$carve"
  chips=$((dx * dy * dz))
  nodes_per_pool=$((chips / CHIPS_PER_NODE))
  [ "$nodes_per_pool" -ge 1 ] || { echo "carve $carve is smaller than one node" >&2; exit 2; }
  [ $((NODES_PER_CUBE % nodes_per_pool)) -eq 0 ] || { echo "carve $carve does not tile a 16-node cube" >&2; exit 2; }
  pools=$((NODES_PER_CUBE / nodes_per_pool))

  sblk=$(printf 'sblk%02d' "$cube_idx")
  subblock_id=$(hash32 "block${BLOCK}/${sblk}")

  for p in $(seq 0 $((pools - 1))); do
    pool="b3p${BLOCK}-blk${BLOCK}-${sblk}-${carve}-${p}"
    pool_hash=$(hash8 "$pool")
    # Every node in a pool shares the slice id for its own topology: that
    # shared value is what a topology-aware scheduler gangs a slice on.
    slice_id=$(hash32 "${pool}/slice")

    for n in $(seq 0 $((nodes_per_pool - 1))); do
      name="gke-tpu-${pool_hash}-$(printf '%02d' "$n")"
      cat <<YAML
---
apiVersion: v1
kind: Node
metadata:
  name: ${name}
  annotations:
    kwok.x-k8s.io/node: fake
    node.alpha.kubernetes.io/ttl: "0"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/instance-type: tpu7x-standard-4t
    beta.kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    kubernetes.io/hostname: ${name}
    kubernetes.io/os: linux
    node.kubernetes.io/instance-type: tpu7x-standard-4t
    topology.kubernetes.io/region: us-central1
    topology.kubernetes.io/zone: us-central1-ai1a
    failure-domain.beta.kubernetes.io/region: us-central1
    failure-domain.beta.kubernetes.io/zone: us-central1-ai1a
    topology.gke.io/zone: us-central1-ai1a
    cloud.google.com/gce-topology-block: $(hash32 "block${BLOCK}")
    cloud.google.com/gce-topology-subblock: ${subblock_id}
    cloud.google.com/gce-topology-host: $(hash32 "${name}/host")
    cloud.google.com/gke-accelerator-count: "${CHIPS_PER_NODE}"
    cloud.google.com/gke-container-runtime: containerd
    cloud.google.com/gke-nodepool: ${pool}
    cloud.google.com/gke-os-distribution: ubuntu
    cloud.google.com/gke-provisioning: standard
    cloud.google.com/gke-tpu-accelerator: tpu7x
    cloud.google.com/gke-tpu-topology: ${carve}
    cloud.google.com/gke-tpu-partition-${carve}-id: ${slice_id}
    cloud.google.com/machine-family: tpu7x
    cloud.google.com/reservation-blocks: batch3pod5co-block-$(printf '%04d' "$BLOCK")
    cloud.google.com/reservation-name: batch3pod5co
    cloud.google.com/reservation-subblocks: batch3pod5co-block-$(printf '%04d' "$BLOCK")-subblock-$(printf '%04d' "$cube_idx")
    is_tpu: "true"
    node.example.com/type: tpu7x
    kwok-sim/cube: "${cube_idx}"
YAML
      for m in ${MODELS+"${MODELS[@]}"}; do
        echo "    models.ome.io/clusterbasemodel.${m}: Ready"
      done
      # .100+ keeps the fake range clear of the real nodes at .0.2/.0.3.
      ip="${IP_PREFIX}.$((100 + node_seq / 250)).$((1 + node_seq % 250))"
      node_seq=$((node_seq + 1))
      cat <<YAML
spec:
  taints:
    - key: google.com/tpu
      value: present
      effect: NoSchedule
YAML
      emit_status "$name" "$ip"
    done
  done
done
