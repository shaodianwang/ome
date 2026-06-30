//! Response DTOs — clean, stable projections of the raw k8s/CRD objects.
//!
//! The console gets a documented contract decoupled from CRD/Node internals (and
//! a far smaller payload than the raw objects). The full object is still available
//! per-object via `GET /v1/{kind}/{name}?raw=true`.

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use kube::Resource;
use ome_client::{
    k8s_openapi::{
        api::core::v1::Node,
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
    },
    InferenceService,
};
use serde::Serialize;

/// GPU accounting: schedulable total vs. claimed by pods.
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

/// True for accelerator GPU resource keys, vendor-neutrally (`nvidia.com/gpu`,
/// `amd.com/gpu`, ...).
pub fn is_gpu(key: &str) -> bool {
    key == "gpu" || key.ends_with("/gpu")
}

/// Sum the integer GPU quantities in a resource map (across vendors).
pub fn sum_gpu(resources: &BTreeMap<String, Quantity>) -> i64 {
    resources
        .iter()
        .filter(|(k, _)| is_gpu(k))
        .filter_map(|(_, q)| q.0.parse::<i64>().ok())
        .sum()
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

/// Generic overview row for any kind: identity + age.
#[derive(Serialize)]
pub struct ResourceSummary {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl ResourceSummary {
    pub fn of<K: Resource>(obj: &K) -> Self {
        let meta = obj.meta();
        Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            age: age(meta),
        }
    }
}

/// InferenceService overview: the model/runtime it binds, mode, and readiness.
#[derive(Serialize)]
pub struct InferenceServiceView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "deploymentMode")]
    pub deployment_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl InferenceServiceView {
    pub fn of(isvc: &InferenceService) -> Self {
        let meta = isvc.meta();
        let ready = isvc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conds| {
                conds
                    .iter()
                    .find(|c| c.type_ == "Ready")
                    .map(|c| c.status == "True")
            });
        let deployment_mode = isvc
            .spec
            .deployment_mode
            .as_ref()
            .and_then(|m| serde_json::to_value(m).ok())
            .and_then(|v| v.as_str().map(String::from));
        Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            model: isvc.spec.model.as_ref().map(|m| m.name.clone()),
            runtime: isvc.spec.runtime.as_ref().map(|r| r.name.clone()),
            deployment_mode,
            ready,
            age: age(meta),
        }
    }
}

/// Node overview shaped for capacity views: schedulability, instance/island, GPU
/// accounting, and a vendor-neutral device map.
#[derive(Serialize)]
pub struct NodeView {
    pub name: String,
    pub ready: bool,
    pub schedulable: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename = "instanceType")]
    pub instance_type: Option<String>,
    /// RDMA island the node belongs to (value of the configured island label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub island: Option<String>,
    /// NVLink domain the node belongs to (value of the configured NVLink label) —
    /// e.g. a GB300 NVL72 rack. Sits within one island.
    #[serde(skip_serializing_if = "Option::is_none", rename = "nvlinkDomain")]
    pub nvlink_domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    /// GPU schedulable/claimed/free (absent for non-GPU nodes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pods: Option<String>,
    /// Device/accelerator resources from capacity (everything that isn't
    /// cpu/memory/ephemeral-storage/pods/hugepages) — e.g. `nvidia.com/gpu`,
    /// `rdma/ib`, `google.com/tpu`.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub devices: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "kubeletVersion")]
    pub kubelet_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl NodeView {
    /// `gpu_allocated` is the GPU count claimed by pods on this node (computed from
    /// the Pod cache); `topo` selects which labels name the island / NVLink domain.
    pub fn of(node: &Node, gpu_allocated: i64, topo: &Topology) -> Self {
        let meta = node.meta();
        let labels = meta.labels.clone().unwrap_or_default();
        let status = node.status.as_ref();
        let capacity = status.and_then(|s| s.capacity.as_ref());
        let cap = |key: &str| capacity.and_then(|c| c.get(key)).map(|q| q.0.clone());
        let ready = status
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false);
        let devices = capacity
            .map(|c| {
                c.iter()
                    .filter(|(k, q)| !is_mundane(k) && q.0 != "0")
                    .map(|(k, q)| (k.clone(), q.0.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let gpu_allocatable = status
            .and_then(|s| s.allocatable.as_ref())
            .map(sum_gpu)
            .unwrap_or(0);
        let gpu = (gpu_allocatable > 0).then(|| GpuView::new(gpu_allocatable, gpu_allocated));
        Self {
            name: meta.name.clone().unwrap_or_default(),
            ready,
            schedulable: !node
                .spec
                .as_ref()
                .and_then(|s| s.unschedulable)
                .unwrap_or(false),
            instance_type: nonempty(labels.get("node.kubernetes.io/instance-type")),
            island: first_label(&labels, &topo.island_labels),
            nvlink_domain: first_label(&labels, &topo.nvlink_labels),
            zone: nonempty(labels.get("topology.kubernetes.io/zone")),
            gpu,
            cpu: cap("cpu"),
            memory: cap("memory").and_then(|m| humanize_mem(&m)),
            pods: cap("pods"),
            devices,
            kubelet_version: status
                .and_then(|s| s.node_info.as_ref())
                .map(|i| i.kubelet_version.clone()),
            age: age(meta),
        }
    }
}

/// A label value, treating absent and empty-string as the same (None).
fn nonempty(value: Option<&String>) -> Option<String> {
    value.filter(|s| !s.is_empty()).cloned()
}

/// Standard non-accelerator capacity keys, excluded from the device map.
fn is_mundane(key: &str) -> bool {
    matches!(key, "cpu" | "memory" | "ephemeral-storage" | "pods") || key.starts_with("hugepages-")
}

/// Humanize a k8s memory quantity (e.g. "1002007424Ki") to GiB/TiB.
fn humanize_mem(quantity: &str) -> Option<String> {
    const KI: f64 = 1024.0;
    let (num, mult) = [
        ("Ki", KI),
        ("Mi", KI * KI),
        ("Gi", KI * KI * KI),
        ("Ti", KI * KI * KI * KI),
        ("Pi", KI * KI * KI * KI * KI),
    ]
    .into_iter()
    .find_map(|(suffix, mult)| quantity.trim().strip_suffix(suffix).map(|n| (n, mult)))
    .unwrap_or((quantity.trim(), 1.0));
    let bytes = num.trim().parse::<f64>().ok()? * mult;
    let gib = bytes / (KI * KI * KI);
    Some(if gib >= 1024.0 {
        format!("{:.1}TiB", gib / 1024.0)
    } else {
        format!("{gib:.0}GiB")
    })
}

/// Humanized object age from its creation timestamp. k8s_openapi's `Time` wraps a
/// `jiff::Timestamp`; compare its epoch seconds against wall-clock seconds.
fn age(meta: &ObjectMeta) -> Option<String> {
    let created = meta.creation_timestamp.as_ref()?.0.as_second();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    let secs = (now - created).max(0);
    Some(if secs >= 86_400 {
        format!("{}d", secs / 86_400)
    } else if secs >= 3_600 {
        format!("{}h", secs / 3_600)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    })
}
