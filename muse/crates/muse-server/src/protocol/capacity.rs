//! Capacity wire types: the accelerator-accounting helpers shared by the node
//! and pod projections, and the `GET /v1/capacity` response built by
//! `rollup::build`.

use std::collections::BTreeMap;

use ome_client::k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use serde::Serialize;

/// Accelerator accounting: schedulable total vs. claimed by pods (a count of
/// chips of one kind — GPUs or TPUs).
#[derive(Serialize, Clone, Copy, Default)]
pub struct GpuView {
    pub allocatable: i64,
    pub allocated: i64,
    pub free: i64,
}

impl GpuView {
    pub fn new(allocatable: i64, allocated: i64) -> Self {
        Self {
            allocatable,
            allocated,
            free: (allocatable - allocated).max(0),
        }
    }
    pub fn add(&mut self, other: &GpuView) {
        self.allocatable += other.allocatable;
        self.allocated += other.allocated;
        self.free += other.free;
    }
}

/// The accelerator "kind" of a resource key, or `None` if it isn't an
/// accelerator. Pattern-based, so it's vendor-neutral: `nvidia.com/gpu`,
/// `amd.com/gpu` → `"gpu"`; `google.com/tpu` → `"tpu"`. GPUs and TPUs are tracked
/// separately — different units, never summed into one number.
fn accel_kind(key: &str) -> Option<&'static str> {
    match key.rsplit('/').next().unwrap_or(key) {
        "gpu" => Some("gpu"),
        "tpu" => Some("tpu"),
        _ => None,
    }
}

/// Sum the integer counts of accelerators of `kind` in a resource map.
pub fn sum_accel(resources: &BTreeMap<String, Quantity>, kind: &str) -> i64 {
    resources
        .iter()
        .filter(|(k, _)| accel_kind(k) == Some(kind))
        .filter_map(|(_, q)| q.0.parse::<i64>().ok())
        .sum()
}

/// A node's (or pod's) accelerator: its kind and total count — e.g. `("tpu", 4)`
/// or `("gpu", 8)`. `None` if it exposes no accelerator; if more than one kind is
/// present (unusual) the greater count wins.
pub fn node_accel(resources: &BTreeMap<String, Quantity>) -> Option<(&'static str, i64)> {
    let gpu = sum_accel(resources, "gpu");
    let tpu = sum_accel(resources, "tpu");
    match (gpu, tpu) {
        (0, 0) => None,
        (g, t) if t > g => Some(("tpu", t)),
        (g, _) => Some(("gpu", g)),
    }
}

/// A node's accelerator for the API: kind plus schedulable/claimed/free chips.
#[derive(Serialize)]
pub struct Accel {
    pub kind: &'static str,
    pub allocatable: i64,
    pub allocated: i64,
    pub free: i64,
}

/// The node labels muse groups capacity by: the RDMA island (outer level — the
/// network fabric) and the NVLink domain (inner level — e.g. a GB300 NVL72 rack,
/// which sits within one island). Each is a list of candidate label keys tried in
/// order (first present wins), so one config covers clusters/providers that label
/// topology differently.
#[derive(Clone)]
pub struct Topology {
    pub island_labels: Vec<String>,
    pub nvlink_labels: Vec<String>,
}

/// First present, non-empty value among candidate label keys (tried in order).
pub fn first_label(labels: &BTreeMap<String, String>, keys: &[String]) -> Option<String> {
    keys.iter()
        .find_map(|k| labels.get(k).filter(|v| !v.is_empty()).cloned())
}

/// Fleet-wide capacity report.
#[derive(Serialize)]
pub struct CapacityReport {
    pub nodes: NodeCounts,
    /// Schedulable accelerator totals — Ready, uncordoned Nodes only — one entry
    /// per kind (e.g. `gpu`, `tpu`). Physical inventory excluded here is reported
    /// in `unavailable`.
    pub accelerators: Vec<GroupCapacity>,
    #[serde(rename = "byCluster")]
    pub by_cluster: Vec<GroupCapacity>,
    #[serde(rename = "byIsland")]
    pub by_island: Vec<GroupCapacity>,
    #[serde(rename = "byNvlinkDomain")]
    pub by_nvlink_domain: Vec<GroupCapacity>,
    #[serde(rename = "byInstanceType")]
    pub by_instance_type: Vec<GroupCapacity>,
    /// Unscheduled accelerator demand — Pending pods not yet placed, by kind.
    pub pending: Vec<AccelDemand>,
    /// Accelerators on NotReady/cordoned nodes — present but unschedulable, by kind.
    pub unavailable: Vec<AccelLost>,
    /// TPU carve-able slices grouped by shape — how many whole slices are free now
    /// vs partially used. Empty on clusters without GKE TPU partition labels.
    #[serde(rename = "tpuSlices")]
    pub tpu_slices: Vec<PlacementUnits>,
    /// TPU subblocks — one entry per physical subblock (from the NVLink/ICI label),
    /// for the heatmap grid. Empty without that label.
    #[serde(rename = "tpuSubblocks")]
    pub tpu_subblocks: Vec<UnitBox>,
    /// GPU whole-node availability grouped by GPUs-per-node — how many entire nodes
    /// are free (for a job needing a full box) vs partially used.
    #[serde(rename = "gpuNodes")]
    pub gpu_nodes: Vec<PlacementUnits>,
    /// GPU racks — one entry per NVLink domain (from the NVLink label), for the
    /// heatmap grid. Empty without that label.
    #[serde(rename = "gpuRacks")]
    pub gpu_racks: Vec<UnitBox>,
    /// Top accelerator consumers on this cluster (by namespace), most chips first.
    pub consumers: Vec<Consumer>,
    /// Top accelerator consumers by owning InferenceService (pods carrying the
    /// operator's ownership label), most chips first. Pods without the label —
    /// non-OME workloads — appear only in the namespace list above.
    #[serde(rename = "consumersByDeployment")]
    pub consumers_by_deployment: Vec<DeploymentConsumer>,
    /// InferenceServices with unscheduled accelerator pods, ranked by missing
    /// accelerators. Requested is the sum of scheduled and missing demand.
    #[serde(rename = "pendingByDeployment")]
    pub pending_by_deployment: Vec<PendingDeployment>,
}

/// One carve-able domain (a TPU subblock or a GPU NVLink rack) for the heatmap:
/// its id, cluster, and chip occupancy. The console derives the state — free /
/// partial / full / unhealthy — from `free`, `chips`, and `healthy`.
#[derive(Serialize)]
pub struct UnitBox {
    pub id: String,
    pub cluster: String,
    pub chips: i64,
    pub free: i64,
    pub healthy: bool,
}

/// A top accelerator consumer on a cluster: a namespace, the accelerator kind, and
/// how many chips its scheduled pods hold.
#[derive(Serialize)]
pub struct Consumer {
    pub namespace: String,
    pub kind: String,
    pub accelerators: i64,
    pub pods: i64,
}

/// A top accelerator consumer attributed to one InferenceService. Carries its
/// cluster so fleet-wide rows stay clickable identities, not merged aggregates.
#[derive(Serialize)]
pub struct DeploymentConsumer {
    pub cluster: String,
    pub namespace: String,
    pub name: String,
    pub kind: String,
    pub accelerators: i64,
    pub pods: i64,
}

/// Accelerator demand for one InferenceService that has Pending, unbound pods.
#[derive(Serialize)]
pub struct PendingDeployment {
    pub cluster: String,
    pub namespace: String,
    pub name: String,
    pub kind: String,
    #[serde(rename = "requestedAccelerators")]
    pub requested_accelerators: i64,
    #[serde(rename = "requestedPods")]
    pub requested_pods: i64,
    #[serde(rename = "allocatedAccelerators")]
    pub allocated_accelerators: i64,
    #[serde(rename = "allocatedPods")]
    pub allocated_pods: i64,
    #[serde(rename = "missingAccelerators")]
    pub missing_accelerators: i64,
    #[serde(rename = "missingPods")]
    pub missing_pods: i64,
}

/// The occupancy breakdown of the carve-able units at one size — a TPU slice shape,
/// a GPU node's GPU-count, or an NVLink domain's GPU-count. Every unit falls in
/// exactly one bucket, so `total == free + fragmented + full + unhealthy`. This is
/// what makes fragmentation legible: `free` is what a whole-unit job can land on
/// (researcher), `fragmented` are the defrag targets (operator), and
/// `stranded_chips` is the free capacity trapped inside them (product).
#[derive(Serialize)]
pub struct PlacementUnits {
    /// Size label: a TPU shape ("2x2x2"), or the unit's chip/GPU count ("8", "72").
    pub label: String,
    /// Chips (TPU) or GPUs per unit of this size.
    pub chips: i64,
    pub total: i64,
    /// Wholly free and schedulable — placeable for a job needing the whole unit.
    pub free: i64,
    /// Partially used (some chips busy, some free), all nodes schedulable.
    pub fragmented: i64,
    /// Fully allocated.
    pub full: i64,
    /// At least one node NotReady/cordoned — can't be scheduled regardless of usage.
    pub unhealthy: i64,
    /// Free chips trapped inside fragmented units — free, but can't serve a
    /// whole-unit job of this size (the recoverable-by-defrag waste).
    #[serde(rename = "strandedChips")]
    pub stranded_chips: i64,
    /// The most-stranded fragmented units, named — the operator's defrag shortlist.
    pub fragments: Vec<Fragment>,
}

/// One fragmented carve-able unit, named for a drill-down: a TPU slice/partition id
/// or an NVLink-domain/node name, its cluster, and how many of its chips are free.
#[derive(Serialize)]
pub struct Fragment {
    pub id: String,
    pub cluster: String,
    pub free: i64,
    pub chips: i64,
}

/// Unscheduled accelerator demand for one kind (Pending, not-yet-placed pods).
#[derive(Serialize)]
pub struct AccelDemand {
    pub kind: String,
    pub accelerators: i64,
    pub pods: i64,
}

/// Accelerators that exist but can't be scheduled (on NotReady / cordoned nodes).
#[derive(Serialize)]
pub struct AccelLost {
    pub kind: String,
    pub accelerators: i64,
    pub nodes: i64,
}

#[derive(Serialize, Default)]
pub struct NodeCounts {
    pub total: usize,
    pub ready: usize,
    /// Nodes that are both Ready and uncordoned.
    pub schedulable: usize,
    #[serde(rename = "acceleratorNodes")]
    pub accelerator_nodes: usize,
}

/// Schedulable accelerator capacity for one group (a kind total, cluster, island,
/// NVLink/ICI domain, or instance type), tagged with the accelerator `kind`.
/// NotReady and cordoned Nodes are excluded from all four values and instead
/// reported in [`CapacityReport::unavailable`].
#[derive(Serialize)]
pub struct GroupCapacity {
    pub name: String,
    pub kind: String,
    pub nodes: usize,
    pub allocatable: i64,
    pub allocated: i64,
    pub free: i64,
}
