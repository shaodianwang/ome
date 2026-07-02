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
    first_label, node_accel, sum_accel, CapacityReport, GpuView, GroupCapacity, NodeCounts,
    Topology,
};

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

    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);

        for node in &cluster.nodes {
            let name = node.meta().name.clone().unwrap_or_default();
            let status = node.status.as_ref();
            let labels = node.meta().labels.clone().unwrap_or_default();

            counts.total += 1;
            let ready = status
                .and_then(|s| s.conditions.as_ref())
                .map(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
                .unwrap_or(false);
            if ready {
                counts.ready += 1;
            }
            if !node
                .spec
                .as_ref()
                .and_then(|s| s.unschedulable)
                .unwrap_or(false)
            {
                counts.schedulable += 1;
            }

            let Some((kind, allocatable)) = status
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue; // non-accelerator node: counted in totals, skipped in rollups
            };
            counts.accelerator_nodes += 1;

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
