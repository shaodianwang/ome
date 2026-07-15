//! Capacity rollup — the cross-resource analytics behind `/v1/capacity` that the
//! frontend can't do itself (it requires joining Nodes × Pods, and fanning out
//! across clusters). Domain logic, not an endpoint: it's run by the background
//! refresh task on informer changes and cached on `AppState`; the endpoint only
//! serves the snapshot.
//!
//! "Allocatable" comes from each Node; "allocated" is summed from the accelerator
//! requests of the Pods scheduled on it. Accelerators are tracked **per kind** —
//! GPUs and TPUs never share a total (different units). Results roll up fleet-wide
//! per kind and are grouped by cluster, island, NVLink/ICI domain, and instance
//! type; every group carries its accelerator `kind`.

use std::{collections::BTreeMap, sync::Arc};

use kube::Resource;
use ome_client::k8s_openapi::api::core::v1::{Node, Pod};

use crate::protocol::{
    first_label, node_accel, sum_accel, AccelDemand, AccelLost, CapacityReport, GpuView,
    GroupCapacity, NodeCounts, PlacementUnit, Topology,
};

/// Accelerator kinds tracked separately (a pod/node never mixes them in practice).
const KINDS: [&str; 2] = ["gpu", "tpu"];

/// GKE labels each TPU node with the carve-able slice it belongs to: the key
/// `cloud.google.com/gke-tpu-partition-<shape>-id` encodes the slice shape, its
/// value the slice instance. A fixed GKE API contract — like the `google.com/tpu`
/// and `node.kubernetes.io/instance-type` keys read elsewhere — so it's matched
/// directly off informer data rather than configured (it can't vary the way the
/// island/NVLink label keys do across providers). Non-GKE nodes carry no such
/// label and contribute no slices.
const TPU_PARTITION_PREFIX: &str = "cloud.google.com/gke-tpu-partition-";
const TPU_PARTITION_SUFFIX: &str = "-id";

/// One cluster's cached Nodes + Pods, the input to aggregation.
pub struct ClusterData {
    pub name: String,
    pub nodes: Vec<Arc<Node>>,
    pub pods: Vec<Arc<Pod>>,
}

/// Accelerator count claimed by non-terminal Pods, keyed by node name. Kind-
/// agnostic: a node's pods request the same accelerator kind the node offers, so
/// the per-node total lines up with that node's kind in [`build`].
pub fn accel_allocated_by_node(pods: &[Arc<Pod>]) -> BTreeMap<String, i64> {
    let mut by_node: BTreeMap<String, i64> = BTreeMap::new();
    for pod in pods {
        // Skip pods that have released their resources.
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
        if matches!(phase, Some("Succeeded" | "Failed")) {
            continue;
        }
        let Some(spec) = pod.spec.as_ref() else {
            continue;
        };
        let Some(node) = spec.node_name.as_ref() else {
            continue;
        };
        let count: i64 = spec
            .containers
            .iter()
            .filter_map(|c| c.resources.as_ref())
            .filter_map(|r| r.requests.as_ref())
            .map(|req| sum_accel(req, "gpu") + sum_accel(req, "tpu"))
            .sum();
        if count > 0 {
            *by_node.entry(node.clone()).or_insert(0) += count;
        }
    }
    by_node
}

/// Bucket key: a group name plus the accelerator kind (kinds are never merged).
type Key = (String, &'static str);

/// Build the fleet capacity report across all clusters.
pub fn build(clusters: &[ClusterData], topo: &Topology) -> CapacityReport {
    let mut counts = NodeCounts::default();
    let mut kinds: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut by_cluster: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut islands: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut nvlink: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut instance: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    // Demand: Pending accelerator pods not yet placed (kind -> accel, pods).
    let mut pending: BTreeMap<&'static str, (i64, i64)> = BTreeMap::new();
    // Lost capacity: accelerators on NotReady/cordoned nodes (kind -> accel, nodes).
    let mut unavail: BTreeMap<&'static str, (i64, usize)> = BTreeMap::new();

    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);
        accumulate_pending(&cluster.pods, &mut pending);

        for node in &cluster.nodes {
            let name = node.meta().name.clone().unwrap_or_default();
            let status = node.status.as_ref();
            let labels = node.meta().labels.clone().unwrap_or_default();

            counts.total += 1;
            let ready = node_ready(node);
            if ready {
                counts.ready += 1;
            }
            let unschedulable = node_unschedulable(node);
            if !unschedulable {
                counts.schedulable += 1;
            }

            let Some((kind, allocatable)) = status
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue; // non-accelerator node: counted in totals, skipped in rollups
            };
            counts.accelerator_nodes += 1;
            // Accelerators on a NotReady or cordoned node exist but can't be scheduled.
            if !ready || unschedulable {
                let e = unavail.entry(kind).or_insert((0, 0));
                e.0 += allocatable;
                e.1 += 1;
            }

            let stat = GpuView::new(allocatable, allocated.get(&name).copied().unwrap_or(0));
            accumulate(&mut kinds, (kind.to_owned(), kind), &stat);
            accumulate(&mut by_cluster, (cluster.name.clone(), kind), &stat);
            accumulate(
                &mut islands,
                (label_or(&labels, &topo.island_labels), kind),
                &stat,
            );
            accumulate(
                &mut nvlink,
                (label_or(&labels, &topo.nvlink_labels), kind),
                &stat,
            );
            accumulate(
                &mut instance,
                (
                    labels
                        .get("node.kubernetes.io/instance-type")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                    kind,
                ),
                &stat,
            );
        }
    }

    CapacityReport {
        nodes: counts,
        accelerators: into_groups(kinds),
        by_cluster: into_groups(by_cluster),
        by_island: into_groups(islands),
        by_nvlink_domain: into_groups(nvlink),
        by_instance_type: into_groups(instance),
        pending: pending
            .into_iter()
            .map(|(kind, (accelerators, pods))| AccelDemand {
                kind: kind.to_owned(),
                accelerators,
                pods,
            })
            .collect(),
        unavailable: unavail
            .into_iter()
            .map(|(kind, (accelerators, nodes))| AccelLost {
                kind: kind.to_owned(),
                accelerators,
                nodes: nodes as i64,
            })
            .collect(),
        // TPU: each node's partition-id label names the slice it belongs to;
        // the slice shape buckets them. GPU: the node itself is a unit (bucketed by
        // GPU count), and — where NVLink labels exist — so is the whole domain.
        tpu_slices: placement_units(clusters, "tpu", |cluster, _name, labels| {
            labels.keys().find_map(|k| {
                k.strip_prefix(TPU_PARTITION_PREFIX)
                    .and_then(|s| s.strip_suffix(TPU_PARTITION_SUFFIX))
                    .map(|shape| {
                        (
                            format!("{cluster}|{shape}|{}", labels[k]),
                            Some(shape.to_owned()),
                        )
                    })
            })
        }),
        gpu_nodes: placement_units(clusters, "gpu", |cluster, name, _labels| {
            Some((format!("{cluster}|{name}"), None))
        }),
        gpu_domains: placement_units(clusters, "gpu", |cluster, _name, labels| {
            first_label(labels, &topo.nvlink_labels).map(|dom| (format!("{cluster}|{dom}"), None))
        }),
    }
}

/// Whether a node's `Ready` condition is True.
fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// Whether a node is cordoned (`spec.unschedulable`).
fn node_unschedulable(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false)
}

/// Carve-able placement units of `want_kind`, bucketed by size, counting how many
/// whole units are placeable (every chip free and every node schedulable) vs
/// partially used — the fragmentation a raw free-chip count hides.
///
/// `unit_of(cluster, node, labels)` assigns a node to a unit, returning `(unique
/// unit key, optional size label)`; a node it maps to `None` is skipped. A `None`
/// size label buckets the unit by its own chip count (GPU nodes / NVLink domains,
/// which have no fixed shape); TPU passes the slice shape. The unit key must be
/// cluster-unique so a slice/partition id reused across clusters isn't merged.
fn placement_units(
    clusters: &[ClusterData],
    want_kind: &str,
    unit_of: impl Fn(&str, &str, &BTreeMap<String, String>) -> Option<(String, Option<String>)>,
) -> Vec<PlacementUnit> {
    // unit key -> (chips, free chips, all-free-and-schedulable, size label).
    let mut units: BTreeMap<String, (i64, i64, bool, Option<String>)> = BTreeMap::new();
    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);
        for node in &cluster.nodes {
            let Some((kind, chips)) = node
                .status
                .as_ref()
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue;
            };
            if kind != want_kind {
                continue;
            }
            let name = node.meta().name.clone().unwrap_or_default();
            let labels = node.meta().labels.clone().unwrap_or_default();
            let Some((key, label)) = unit_of(&cluster.name, &name, &labels) else {
                continue;
            };
            let used = allocated.get(&name).copied().unwrap_or(0);
            let ok = used == 0 && node_ready(node) && !node_unschedulable(node);
            let e = units.entry(key).or_insert((0, 0, true, label.clone()));
            e.0 += chips;
            e.1 += chips - used;
            e.2 &= ok;
            if e.3.is_none() {
                e.3 = label;
            }
        }
    }

    // Bucket units by size label (explicit shape, else the unit's chip count).
    let mut by_size: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
    for (_key, (chips, free, ok, label)) in units {
        let bucket = label.unwrap_or_else(|| chips.to_string());
        let e = by_size.entry(bucket).or_insert((0, 0, 0));
        e.0 = e.0.max(chips); // chips per unit (uniform within a size)
        e.1 += 1; // total
        if ok && free == chips {
            e.2 += 1; // wholly available
        }
    }
    let mut out: Vec<PlacementUnit> = by_size
        .into_iter()
        .map(|(label, (chips, total, available))| PlacementUnit {
            label,
            chips,
            total,
            available,
        })
        .collect();
    // Biggest units first — the ones fragmentation strands soonest.
    out.sort_by(|a, b| b.chips.cmp(&a.chips).then(a.label.cmp(&b.label)));
    out
}

/// Sum accelerator requests of Pending, not-yet-scheduled pods, per kind — demand
/// waiting on capacity. A Pending pod that already has a node (e.g. pulling an
/// image) isn't waiting on capacity, so it's excluded.
fn accumulate_pending(pods: &[Arc<Pod>], out: &mut BTreeMap<&'static str, (i64, i64)>) {
    for pod in pods {
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
        if phase != Some("Pending") {
            continue;
        }
        let Some(spec) = pod.spec.as_ref() else {
            continue;
        };
        if spec.node_name.is_some() {
            continue;
        }
        for kind in KINDS {
            let req: i64 = spec
                .containers
                .iter()
                .filter_map(|c| c.resources.as_ref())
                .filter_map(|r| r.requests.as_ref())
                .map(|r| sum_accel(r, kind))
                .sum();
            if req > 0 {
                let e = out.entry(kind).or_insert((0, 0));
                e.0 += req;
                e.1 += 1;
            }
        }
    }
}

/// The first present island/domain label value, or "unassigned".
fn label_or(labels: &BTreeMap<String, String>, keys: &[String]) -> String {
    first_label(labels, keys).unwrap_or_else(|| "unassigned".to_owned())
}

/// Add a node's accelerator to a (name, kind) bucket.
fn accumulate(map: &mut BTreeMap<Key, (usize, GpuView)>, key: Key, stat: &GpuView) {
    let entry = map.entry(key).or_insert((0, GpuView::default()));
    entry.0 += 1;
    entry.1.add(stat);
}

/// Flatten a bucket map into rows sorted by free descending (most headroom first).
fn into_groups(map: BTreeMap<Key, (usize, GpuView)>) -> Vec<GroupCapacity> {
    let mut groups: Vec<GroupCapacity> = map
        .into_iter()
        .map(|((name, kind), (nodes, gpu))| GroupCapacity {
            name,
            kind: kind.to_owned(),
            nodes,
            allocatable: gpu.allocatable,
            allocated: gpu.allocated,
            free: gpu.free,
        })
        .collect();
    groups.sort_by(|a, b| b.free.cmp(&a.free).then(a.name.cmp(&b.name)));
    groups
}
