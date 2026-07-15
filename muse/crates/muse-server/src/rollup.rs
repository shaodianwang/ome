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
    first_label, node_accel, sum_accel, AccelDemand, AccelLost, CapacityReport, Fragment, GpuView,
    GroupCapacity, NodeCounts, PlacementUnits, Topology,
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
        // TPU: a host carries a partition-id label for *every* slice shape it's
        // part of (its own 2x2x1 and the 2x2x2 it shares with a neighbour), so emit
        // one unit per label — bucketed by shape. GPU: the node itself is a unit
        // (bucketed by GPU count), and — where NVLink labels exist — so is the
        // whole domain (bucketed by domain GPU-count).
        tpu_slices: placement_units(clusters, "tpu", |cluster, _name, labels| {
            labels
                .iter()
                .filter_map(|(k, id)| {
                    let shape = k
                        .strip_prefix(TPU_PARTITION_PREFIX)?
                        .strip_suffix(TPU_PARTITION_SUFFIX)?;
                    Some((
                        format!("{cluster}|{shape}|{id}"),
                        id.clone(),
                        Some(shape.to_owned()),
                    ))
                })
                .collect()
        }),
        gpu_nodes: placement_units(clusters, "gpu", |cluster, name, _labels| {
            vec![(format!("{cluster}|{name}"), name.to_owned(), None)]
        }),
        gpu_domains: placement_units(clusters, "gpu", |cluster, _name, labels| {
            first_label(labels, &topo.nvlink_labels)
                .map(|dom| vec![(format!("{cluster}|{dom}"), dom.clone(), None)])
                .unwrap_or_default()
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

/// The most-stranded fragmented units surfaced per size (the operator drill-down).
const MAX_FRAGMENTS: usize = 12;

/// Carve-able placement units of `want_kind`, bucketed by size, split by occupancy
/// (free / fragmented / full / unhealthy) with the chips stranded inside fragmented
/// units — the fragmentation a single "free" number hides.
///
/// `unit_of(cluster, node, labels)` returns *every* unit a node belongs to as
/// `(unique key, display id, optional size label)` — a list, because one node can
/// belong to several (a TPU host is part of both its 2x2x1 and 2x2x2 slice). A
/// `None` size label buckets by the unit's own chip count (GPU nodes / domains,
/// which have no fixed shape); TPU passes the slice shape. The key is cluster-scoped
/// so an id reused across clusters isn't merged.
fn placement_units(
    clusters: &[ClusterData],
    want_kind: &str,
    unit_of: impl Fn(&str, &str, &BTreeMap<String, String>) -> Vec<(String, String, Option<String>)>,
) -> Vec<PlacementUnits> {
    // A carve-able unit's running tally across its member nodes.
    struct Acc {
        chips: i64,
        free: i64,
        healthy: bool,
        label: Option<String>,
        cluster: String,
        id: String,
    }
    let mut units: BTreeMap<String, Acc> = BTreeMap::new();
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
            let used = allocated.get(&name).copied().unwrap_or(0);
            let healthy = node_ready(node) && !node_unschedulable(node);
            for (key, id, label) in unit_of(&cluster.name, &name, &labels) {
                let e = units.entry(key).or_insert_with(|| Acc {
                    chips: 0,
                    free: 0,
                    healthy: true,
                    label,
                    cluster: cluster.name.clone(),
                    id,
                });
                e.chips += chips;
                e.free += chips - used;
                e.healthy &= healthy;
            }
        }
    }

    // One accumulator per size bucket, splitting its units by occupancy.
    struct Bucket {
        chips: i64,
        free: i64,
        fragmented: i64,
        full: i64,
        unhealthy: i64,
        stranded: i64,
        frags: Vec<Fragment>,
    }
    let mut by_size: BTreeMap<String, Bucket> = BTreeMap::new();
    for (_key, u) in units {
        let bucket = u.label.clone().unwrap_or_else(|| u.chips.to_string());
        let b = by_size.entry(bucket).or_insert_with(|| Bucket {
            chips: 0,
            free: 0,
            fragmented: 0,
            full: 0,
            unhealthy: 0,
            stranded: 0,
            frags: Vec::new(),
        });
        b.chips = b.chips.max(u.chips);
        if !u.healthy {
            b.unhealthy += 1;
        } else if u.free >= u.chips {
            b.free += 1;
        } else if u.free <= 0 {
            b.full += 1;
        } else {
            b.fragmented += 1;
            b.stranded += u.free;
            b.frags.push(Fragment {
                id: u.id,
                cluster: u.cluster,
                free: u.free,
                chips: u.chips,
            });
        }
    }

    let mut out: Vec<PlacementUnits> = by_size
        .into_iter()
        .map(|(label, mut b)| {
            b.frags.sort_by(|a, c| c.free.cmp(&a.free)); // most stranded first
            b.frags.truncate(MAX_FRAGMENTS);
            PlacementUnits {
                label,
                chips: b.chips,
                total: b.free + b.fragmented + b.full + b.unhealthy,
                free: b.free,
                fragmented: b.fragmented,
                full: b.full,
                unhealthy: b.unhealthy,
                stranded_chips: b.stranded,
                fragments: b.frags,
            }
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
