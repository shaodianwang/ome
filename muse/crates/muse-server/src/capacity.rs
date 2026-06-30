//! Cluster capacity aggregation — the cross-resource analytics the frontend
//! can't do itself (it requires joining Nodes × Pods, and fanning out across
//! clusters).
//!
//! "Allocatable" comes from each Node; "allocated" is summed from the GPU
//! requests of the Pods scheduled on it. Results roll up fleet-wide and are
//! grouped by cluster, RDMA island, NVLink domain, and instance type.

use std::{collections::BTreeMap, sync::Arc};

use kube::Resource;
use ome_client::k8s_openapi::api::core::v1::{Node, Pod};
use serde::Serialize;

use crate::views::{first_label, sum_gpu, GpuView, Topology};

/// One cluster's cached Nodes + Pods, the input to aggregation.
pub struct ClusterData {
    pub name: String,
    pub nodes: Vec<Arc<Node>>,
    pub pods: Vec<Arc<Pod>>,
}

/// Fleet-wide capacity report.
#[derive(Serialize)]
pub struct CapacityReport {
    pub nodes: NodeCounts,
    pub gpu: GpuView,
    #[serde(rename = "byCluster")]
    pub by_cluster: Vec<GroupCapacity>,
    #[serde(rename = "byIsland")]
    pub by_island: Vec<GroupCapacity>,
    #[serde(rename = "byNvlinkDomain")]
    pub by_nvlink_domain: Vec<GroupCapacity>,
    #[serde(rename = "byInstanceType")]
    pub by_instance_type: Vec<GroupCapacity>,
}

#[derive(Serialize, Default)]
pub struct NodeCounts {
    pub total: usize,
    pub ready: usize,
    pub schedulable: usize,
    #[serde(rename = "gpuNodes")]
    pub gpu_nodes: usize,
}

/// GPU capacity for one group (a cluster, island, NVLink domain, or instance type).
#[derive(Serialize)]
pub struct GroupCapacity {
    pub name: String,
    pub nodes: usize,
    pub gpu: GpuView,
}

/// GPU count claimed by non-terminal Pods, keyed by node name.
pub fn gpu_allocated_by_node(pods: &[Arc<Pod>]) -> BTreeMap<String, i64> {
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
        let gpus: i64 = spec
            .containers
            .iter()
            .filter_map(|c| c.resources.as_ref())
            .filter_map(|r| r.requests.as_ref())
            .map(sum_gpu)
            .sum();
        if gpus > 0 {
            *by_node.entry(node.clone()).or_insert(0) += gpus;
        }
    }
    by_node
}

/// Build the fleet capacity report across all clusters.
pub fn build(clusters: &[ClusterData], topo: &Topology) -> CapacityReport {
    let mut counts = NodeCounts::default();
    let mut total = GpuView::default();
    let mut by_cluster: Vec<GroupCapacity> = Vec::new();
    let mut islands: BTreeMap<String, (usize, GpuView)> = BTreeMap::new();
    let mut nvlink_domains: BTreeMap<String, (usize, GpuView)> = BTreeMap::new();
    let mut instance_types: BTreeMap<String, (usize, GpuView)> = BTreeMap::new();

    for cluster in clusters {
        let allocated = gpu_allocated_by_node(&cluster.pods);
        let mut cluster_nodes = 0usize;
        let mut cluster_gpu = GpuView::default();

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

            let allocatable = status
                .and_then(|s| s.allocatable.as_ref())
                .map_or(0, sum_gpu);
            if allocatable == 0 {
                continue; // non-GPU node: counted in totals above, skip GPU rollups
            }
            counts.gpu_nodes += 1;

            let gpu = GpuView::new(allocatable, allocated.get(&name).copied().unwrap_or(0));
            total.add(&gpu);
            cluster_nodes += 1;
            cluster_gpu.add(&gpu);

            accumulate(
                &mut islands,
                first_label(&labels, &topo.island_labels),
                &gpu,
            );
            accumulate(
                &mut nvlink_domains,
                first_label(&labels, &topo.nvlink_labels),
                &gpu,
            );
            accumulate(
                &mut instance_types,
                labels.get("node.kubernetes.io/instance-type").cloned(),
                &gpu,
            );
        }

        by_cluster.push(GroupCapacity {
            name: cluster.name.clone(),
            nodes: cluster_nodes,
            gpu: cluster_gpu,
        });
    }

    by_cluster.sort_by(|a, b| b.gpu.free.cmp(&a.gpu.free).then(a.name.cmp(&b.name)));
    CapacityReport {
        nodes: counts,
        gpu: total,
        by_cluster,
        by_island: into_groups(islands),
        by_nvlink_domain: into_groups(nvlink_domains),
        by_instance_type: into_groups(instance_types),
    }
}

/// Add a node's GPU to a grouping bucket (keyed by a label value, "unassigned"
/// if no candidate label was present).
fn accumulate(map: &mut BTreeMap<String, (usize, GpuView)>, key: Option<String>, gpu: &GpuView) {
    let name = key.unwrap_or_else(|| "unassigned".to_owned());
    let entry = map.entry(name).or_insert((0, GpuView::default()));
    entry.0 += 1;
    entry.1.add(gpu);
}

/// Flatten a grouping map into rows sorted by free GPUs descending (most
/// available capacity first).
fn into_groups(map: BTreeMap<String, (usize, GpuView)>) -> Vec<GroupCapacity> {
    let mut groups: Vec<GroupCapacity> = map
        .into_iter()
        .map(|(name, (nodes, gpu))| GroupCapacity { name, nodes, gpu })
        .collect();
    groups.sort_by(|a, b| b.gpu.free.cmp(&a.gpu.free).then(a.name.cmp(&b.name)));
    groups
}
