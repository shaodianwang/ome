//! The wire protocol: every request query param and response body the console
//! exchanges with muse-server, in one place.
//!
//! Response bodies are clean, stable projections of the raw k8s/CRD objects — a
//! documented contract decoupled from CRD/Node internals (and a far smaller
//! payload); the full object is still available per-object via
//! `GET /v1/{kind}/{name}?raw=true`. The projection builders (`…::of`) live here
//! too; the modules that produce these responses (capacity, feed, resources)
//! reference these types.

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use kube::Resource;
use ome_client::{
    inferenceservices::{InferenceServiceStatus, InferenceServiceStatusPlacement},
    k8s_openapi::{
        api::core::v1::Node,
        apimachinery::pkg::{
            api::resource::Quantity,
            apis::meta::v1::{Condition, ObjectMeta},
        },
    },
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceService,
    ServingRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
pub fn accel_kind(key: &str) -> Option<&'static str> {
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

/// The storage backend from a storage URI scheme: `manifold://…` → "manifold",
/// `pvc://…` → "pvc", `hf://…` → "hf". None when the URI carries no scheme.
fn source_scheme(uri: &str) -> Option<String> {
    uri.split_once("://")
        .map(|(scheme, _)| scheme.to_lowercase())
        .filter(|s| !s.is_empty())
}

/// Model-registry overview: the auto-parsed metadata + lifecycle for a BaseModel
/// or ClusterBaseModel (both share this shape), plus where its weights come from.
#[derive(Serialize)]
pub struct BaseModelView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "parameterSize")]
    pub parameter_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "maxTokens")]
    pub max_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub capabilities: Vec<String>,
    /// Storage backend scheme (e.g. "manifold", "pvc", "hf", "oci") — where the
    /// weights are fetched from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "storageUri")]
    pub storage_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    /// Lifecycle state from status (e.g. Ready, Importing, Failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl BaseModelView {
    pub fn of(m: &BaseModel) -> Self {
        Self {
            name: m.meta().name.clone().unwrap_or_default(),
            namespace: m.meta().namespace.clone(),
            display_name: m.spec.display_name.clone(),
            architecture: m.spec.model_architecture.clone(),
            format: m.spec.model_format.as_ref().map(|f| f.name.clone()),
            parameter_size: m.spec.model_parameter_size.clone(),
            quantization: m.spec.quantization.clone(),
            max_tokens: m.spec.max_tokens,
            vendor: m.spec.vendor.clone(),
            version: m.spec.version.clone(),
            capabilities: m.spec.model_capabilities.clone().unwrap_or_default(),
            source: source_scheme(&m.spec.storage.storage_uri),
            storage_uri: some_nonempty(&m.spec.storage.storage_uri),
            disabled: m.spec.disabled,
            state: m.status.as_ref().map(|s| state_string(&s.state)),
            lifecycle: m.status.as_ref().and_then(|s| s.lifecycle.clone()),
            age: age(m.meta()),
        }
    }

    pub fn of_cluster(m: &ClusterBaseModel) -> Self {
        Self {
            name: m.meta().name.clone().unwrap_or_default(),
            namespace: m.meta().namespace.clone(),
            display_name: m.spec.display_name.clone(),
            architecture: m.spec.model_architecture.clone(),
            format: m.spec.model_format.as_ref().map(|f| f.name.clone()),
            parameter_size: m.spec.model_parameter_size.clone(),
            quantization: m.spec.quantization.clone(),
            max_tokens: m.spec.max_tokens,
            vendor: m.spec.vendor.clone(),
            version: m.spec.version.clone(),
            capabilities: m.spec.model_capabilities.clone().unwrap_or_default(),
            source: source_scheme(&m.spec.storage.storage_uri),
            storage_uri: some_nonempty(&m.spec.storage.storage_uri),
            disabled: m.spec.disabled,
            state: m.status.as_ref().map(|s| state_string(&s.state)),
            lifecycle: m.status.as_ref().and_then(|s| s.lifecycle.clone()),
            age: age(m.meta()),
        }
    }
}

/// Full model detail for the console drawer: the overview plus status conditions,
/// the parsed model configuration, and per-node readiness counts (PerNode models).
#[derive(Serialize)]
pub struct BaseModelDetail {
    #[serde(flatten)]
    pub view: BaseModelView,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "modelConfiguration")]
    pub model_configuration: Option<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "nodesReady")]
    pub nodes_ready: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "nodesFailed")]
    pub nodes_failed: Option<usize>,
}

impl BaseModelDetail {
    pub fn of(m: &BaseModel) -> Self {
        Self {
            view: BaseModelView::of(m),
            conditions: conditions_of(m.status.as_ref().and_then(|s| s.conditions.as_deref())),
            model_configuration: m.spec.model_configuration.clone().filter(|c| !c.is_empty()),
            nodes_ready: m
                .status
                .as_ref()
                .and_then(|s| s.nodes_ready.as_ref())
                .map(|v| v.len()),
            nodes_failed: m
                .status
                .as_ref()
                .and_then(|s| s.nodes_failed.as_ref())
                .map(|v| v.len()),
        }
    }

    pub fn of_cluster(m: &ClusterBaseModel) -> Self {
        Self {
            view: BaseModelView::of_cluster(m),
            conditions: conditions_of(m.status.as_ref().and_then(|s| s.conditions.as_deref())),
            model_configuration: m.spec.model_configuration.clone().filter(|c| !c.is_empty()),
            nodes_ready: m
                .status
                .as_ref()
                .and_then(|s| s.nodes_ready.as_ref())
                .map(|v| v.len()),
            nodes_failed: m
                .status
                .as_ref()
                .and_then(|s| s.nodes_failed.as_ref())
                .map(|v| v.len()),
        }
    }
}

/// The `image` of an opaque pod-template container map.
fn image_of(c: &BTreeMap<String, Value>) -> Option<String> {
    c.get("image").and_then(Value::as_str).map(str::to_owned)
}

/// A component's primary serving container from its serialized config: the
/// single-node `runner`/`containers`, or a multi-node `leader`/`worker`'s. Walking
/// the serialized JSON lets one path cover engine/decoder/router across both the
/// namespaced and cluster-scoped runtime types.
fn container_in(cfg: &Value) -> Option<Container> {
    [Some(cfg), cfg.get("leader"), cfg.get("worker")]
        .into_iter()
        .flatten()
        .find_map(|node| {
            let c = node.get("runner").filter(|v| v.is_object()).or_else(|| {
                node.get("containers")
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
            })?;
            c.as_object()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        })
}

/// The serving container of an optional component config (see [`container_in`]).
fn config_container<T: Serialize>(cfg: &Option<T>) -> Option<Container> {
    container_in(&serde_json::to_value(cfg.as_ref()?).ok()?)
}

/// Drop empty strings and duplicates, preserving first-seen order.
fn uniq_nonempty(items: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    items
        .into_iter()
        .filter(|s| !s.is_empty() && seen.insert(s.clone()))
        .collect()
}

/// A worker-scalable serving component (engine or decoder) and its node topology.
#[derive(Serialize)]
pub struct RuntimeComponent {
    /// "engine" | "decoder".
    pub role: &'static str,
    /// Multi-node (runs a worker pod group) vs single-node.
    #[serde(rename = "multiNode")]
    pub multi_node: bool,
    /// Worker-group node count, when multi-node and declared.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<i64>,
}

/// Build the node-topology component list from each config's presence + worker
/// size. `*_worker` is `Some(size?)` when that component runs a multi-node worker
/// group (the inner value is its node count). The engine is always present; a
/// decoder means prefill/decode disaggregation, and the two scale independently.
/// The router is intentionally excluded — it's a routing layer, not a worker group.
fn runtime_components(
    engine_worker: Option<Option<i64>>,
    has_decoder: bool,
    decoder_worker: Option<Option<i64>>,
) -> Vec<RuntimeComponent> {
    let mut v = vec![RuntimeComponent {
        role: "engine",
        multi_node: engine_worker.is_some(),
        nodes: engine_worker.flatten(),
    }];
    if has_decoder {
        v.push(RuntimeComponent {
            role: "decoder",
            multi_node: decoder_worker.is_some(),
            nodes: decoder_worker.flatten(),
        });
    }
    v
}

/// ServingRuntime overview: the serving-engine image, what models it can serve
/// (formats / architectures / quantizations), the
/// wire protocols, and its components (engine + optional decoder/router), each with
/// its own node topology. ServingRuntime and ClusterServingRuntime share this shape.
#[derive(Serialize)]
pub struct RuntimeView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub architectures: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub formats: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub quantizations: Vec<String>,
    /// Wire protocols served (e.g. openAI, cohere).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub protocols: Vec<String>,
    /// The runtime's worker-scalable components (engine, and — when disaggregated —
    /// decoder), each with its own node topology.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub components: Vec<RuntimeComponent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl RuntimeView {
    pub fn of(r: &ServingRuntime) -> Self {
        let s = &r.spec;
        let f = s.supported_model_formats.as_deref().unwrap_or_default();
        Self {
            name: r.meta().name.clone().unwrap_or_default(),
            namespace: r.meta().namespace.clone(),
            image: config_container(&s.engine_config)
                .or_else(|| s.containers.as_ref().and_then(|v| v.first()).cloned())
                .as_ref()
                .and_then(image_of),
            architectures: uniq_nonempty(
                f.iter()
                    .filter_map(|x| x.model_architecture.clone())
                    .collect(),
            ),
            formats: uniq_nonempty(f.iter().map(|x| x.model_format.name.clone()).collect()),
            quantizations: uniq_nonempty(f.iter().filter_map(|x| x.quantization.clone()).collect()),
            protocols: s.protocol_versions.clone().unwrap_or_default(),
            components: runtime_components(
                s.engine_config
                    .as_ref()
                    .and_then(|e| e.worker.as_ref())
                    .map(|w| w.size),
                s.decoder_config.is_some(),
                s.decoder_config
                    .as_ref()
                    .and_then(|d| d.worker.as_ref())
                    .map(|w| w.size),
            ),
            disabled: s.disabled,
            age: age(r.meta()),
        }
    }

    pub fn of_cluster(r: &ClusterServingRuntime) -> Self {
        let s = &r.spec;
        let f = s.supported_model_formats.as_deref().unwrap_or_default();
        Self {
            name: r.meta().name.clone().unwrap_or_default(),
            namespace: r.meta().namespace.clone(),
            image: config_container(&s.engine_config)
                .or_else(|| s.containers.as_ref().and_then(|v| v.first()).cloned())
                .as_ref()
                .and_then(image_of),
            architectures: uniq_nonempty(
                f.iter()
                    .filter_map(|x| x.model_architecture.clone())
                    .collect(),
            ),
            formats: uniq_nonempty(f.iter().map(|x| x.model_format.name.clone()).collect()),
            quantizations: uniq_nonempty(f.iter().filter_map(|x| x.quantization.clone()).collect()),
            protocols: s.protocol_versions.clone().unwrap_or_default(),
            components: runtime_components(
                s.engine_config
                    .as_ref()
                    .and_then(|e| e.worker.as_ref())
                    .map(|w| w.size),
                s.decoder_config.is_some(),
                s.decoder_config
                    .as_ref()
                    .and_then(|d| d.worker.as_ref())
                    .map(|w| w.size),
            ),
            disabled: s.disabled,
            age: age(r.meta()),
        }
    }
}

/// A serving container's env var (literal values only; refs are kept by name).
#[derive(Serialize)]
pub struct EnvVar {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// A container's resource requests / limits (resource name → quantity).
#[derive(Serialize)]
pub struct RuntimeResources {
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub requests: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub limits: BTreeMap<String, String>,
}

/// One serving component's container config — enough to see how it runs.
#[derive(Serialize)]
pub struct RuntimeContainer {
    pub role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub env: Vec<EnvVar>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<RuntimeResources>,
}

type Container = BTreeMap<String, Value>;

/// A string-array field (e.g. command / args) of an opaque container map.
fn str_list(c: &Container, key: &str) -> Vec<String> {
    c.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Literal env vars of an opaque container map (valueFrom refs kept by name only).
fn env_of(c: &Container) -> Vec<EnvVar> {
    c.get("env")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| {
                    Some(EnvVar {
                        name: e.get("name")?.as_str()?.to_owned(),
                        value: e.get("value").and_then(Value::as_str).map(str::to_owned),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A k8s quantity map (e.g. resources.requests) from an opaque JSON object,
/// stringifying each quantity (they may be JSON strings or numbers).
fn quantity_map(v: Option<&Value>) -> BTreeMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| {
                    let s = val
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| val.is_number().then(|| val.to_string()))?;
                    Some((k.clone(), s))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A container's resource requests/limits, if any are declared.
fn resources_of(c: &Container) -> Option<RuntimeResources> {
    let r = c.get("resources")?;
    let requests = quantity_map(r.get("requests"));
    let limits = quantity_map(r.get("limits"));
    if requests.is_empty() && limits.is_empty() {
        None
    } else {
        Some(RuntimeResources { requests, limits })
    }
}

fn container_detail(role: &'static str, c: &Container) -> RuntimeContainer {
    RuntimeContainer {
        role,
        image: image_of(c),
        command: str_list(c, "command"),
        args: str_list(c, "args"),
        env: env_of(c),
        resources: resources_of(c),
    }
}

/// Full runtime detail: the overview plus each component's serving-container config
/// (image, command, args, env). ServingRuntime and ClusterServingRuntime share it.
#[derive(Serialize)]
pub struct RuntimeDetail {
    #[serde(flatten)]
    pub view: RuntimeView,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub containers: Vec<RuntimeContainer>,
}

impl RuntimeDetail {
    pub fn of(r: &ServingRuntime) -> Self {
        let s = &r.spec;
        let mut containers = Vec::new();
        if let Some(c) = config_container(&s.engine_config)
            .or_else(|| s.containers.as_ref().and_then(|v| v.first()).cloned())
        {
            containers.push(container_detail("engine", &c));
        }
        if let Some(c) = config_container(&s.decoder_config) {
            containers.push(container_detail("decoder", &c));
        }
        if let Some(c) = config_container(&s.router_config) {
            containers.push(container_detail("router", &c));
        }
        Self {
            view: RuntimeView::of(r),
            containers,
        }
    }

    pub fn of_cluster(r: &ClusterServingRuntime) -> Self {
        let s = &r.spec;
        let mut containers = Vec::new();
        if let Some(c) = config_container(&s.engine_config)
            .or_else(|| s.containers.as_ref().and_then(|v| v.first()).cloned())
        {
            containers.push(container_detail("engine", &c));
        }
        if let Some(c) = config_container(&s.decoder_config) {
            containers.push(container_detail("decoder", &c));
        }
        if let Some(c) = config_container(&s.router_config) {
            containers.push(container_detail("router", &c));
        }
        Self {
            view: RuntimeView::of_cluster(r),
            containers,
        }
    }
}

/// Fine-tuned weight (LoRA / adapter / delta) overview: the base model it extends
/// and its type — enough to link it back to a model in the registry drawer.
#[derive(Serialize)]
pub struct FineTunedWeightView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "baseModel")]
    pub base_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "baseModelNamespace")]
    pub base_model_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "modelType")]
    pub model_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl FineTunedWeightView {
    pub fn of(w: &FineTunedWeight) -> Self {
        Self {
            name: w.meta().name.clone().unwrap_or_default(),
            namespace: w.meta().namespace.clone(),
            base_model: some_nonempty(&w.spec.base_model_ref.name),
            base_model_namespace: w.spec.base_model_ref.namespace.clone(),
            model_type: some_nonempty(&w.spec.model_type),
            version: w.spec.version.clone(),
            age: age(w.meta()),
        }
    }
}

/// Serialize an enum (or any value) to its plain string form, treating null and
/// empty string as absent — e.g. a `deploymentMode`/`phase`/`state` enum → "Ready".
fn enum_str<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .filter(|s| !s.is_empty())
}

/// Serialize a status `state` enum to its plain string form (e.g. "Ready").
fn state_string<T: Serialize>(state: &T) -> String {
    enum_str(state).unwrap_or_default()
}

/// Non-empty owned copy of a string, or None.
fn some_nonempty(s: &str) -> Option<String> {
    Some(s.to_owned()).filter(|s| !s.is_empty())
}

/// A status condition, projected to its identity + reason/message.
#[derive(Serialize)]
pub struct ConditionView {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Project a slice of conditions into [`ConditionView`]s (empty reason/message
/// dropped).
fn conditions_of(conds: Option<&[Condition]>) -> Vec<ConditionView> {
    conds
        .unwrap_or_default()
        .iter()
        .map(|c| ConditionView {
            type_: c.type_.clone(),
            status: c.status.clone(),
            reason: some_nonempty(&c.reason),
            message: some_nonempty(&c.message),
        })
        .collect()
}

/// One InferenceService Component (engine / decoder / router): readiness (from
/// the matching `<Comp>Ready` condition), live replica counts, rollout phase, and
/// the revision currently owning traffic.
#[derive(Serialize)]
pub struct ComponentView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "currentReplicas")]
    pub current_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "desiredReplicas")]
    pub desired_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "rolloutPhase")]
    pub rollout_phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Readiness for one Component, read from its `<Comp>Ready` status condition.
fn component_ready(status: &InferenceServiceStatus, component: &str) -> Option<bool> {
    let cond_type = match component {
        "engine" => "EngineReady",
        "decoder" => "DecoderReady",
        "router" => "RouterReady",
        _ => return None,
    };
    status
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == cond_type)
        .map(|c| c.status == "True")
}

/// Project `status.components` into a stable engine → decoder → router ordering.
fn components_of(status: &InferenceServiceStatus) -> Vec<ComponentView> {
    let Some(comps) = status.components.as_ref() else {
        return Vec::new();
    };
    let order = |name: &str| match name {
        "engine" => 0,
        "decoder" => 1,
        "router" => 2,
        _ => 3,
    };
    let mut out: Vec<ComponentView> = comps
        .iter()
        .map(|(name, c)| ComponentView {
            name: name.clone(),
            ready: component_ready(status, name),
            current_replicas: c.autoscaler.as_ref().and_then(|a| a.current_replicas),
            desired_replicas: c.autoscaler.as_ref().and_then(|a| a.desired_replicas),
            rollout_phase: c.rollout_phase.as_ref().and_then(enum_str),
            revision: c
                .latest_rolledout_revision
                .clone()
                .or_else(|| c.latest_ready_revision.clone())
                .filter(|s| !s.is_empty()),
            url: c.url.clone().filter(|s| !s.is_empty()),
        })
        .collect();
    out.sort_by_key(|c| order(&c.name));
    out
}

/// Why the service isn't ready: the `Ready` condition's reason/message when it is
/// not True, else the model-load failure (if any). `(reason, message)`.
fn not_ready_info(status: &InferenceServiceStatus) -> (Option<String>, Option<String>) {
    if let Some(c) = status
        .conditions
        .as_ref()
        .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"))
    {
        if c.status != "True" {
            return (some_nonempty(&c.reason), some_nonempty(&c.message));
        }
    }
    if let Some(f) = status
        .model_status
        .as_ref()
        .and_then(|m| m.last_failure_info.as_ref())
    {
        return (
            f.reason.as_ref().and_then(enum_str),
            f.message.clone().filter(|s| !s.is_empty()),
        );
    }
    (None, None)
}

/// InferenceService overview: the model/runtime it binds, mode, readiness, the
/// servable endpoint, per-Component replicas, and model-load state (with the
/// reason it is not ready, when applicable).
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
    /// Addressable endpoint (`status.url`) once the service is servable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<ComponentView>,
    /// Model-load transition (`UpToDate` / `InProgress` / `BlockedByFailedLoad` / …).
    #[serde(skip_serializing_if = "Option::is_none", rename = "modelState")]
    pub model_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "loadedCopies")]
    pub loaded_copies: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "failedCopies")]
    pub failed_copies: Option<i64>,
    /// When not ready: the coarse reason (e.g. `BaseModelNotReady`, `EngineNotReady`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl InferenceServiceView {
    pub fn of(isvc: &InferenceService) -> Self {
        let meta = isvc.meta();
        let status = isvc.status.as_ref();
        let ready = status
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conds| {
                conds
                    .iter()
                    .find(|c| c.type_ == "Ready")
                    .map(|c| c.status == "True")
            });
        let (reason, message) = status.map(not_ready_info).unwrap_or((None, None));
        let model = status.and_then(|s| s.model_status.as_ref());
        let copies = model.and_then(|m| m.model_copies.as_ref());
        Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            model: isvc.spec.model.as_ref().map(|m| m.name.clone()),
            runtime: isvc.spec.runtime.as_ref().map(|r| r.name.clone()),
            deployment_mode: isvc.spec.deployment_mode.as_ref().and_then(enum_str),
            ready,
            url: status.and_then(|s| s.url.clone()).filter(|s| !s.is_empty()),
            components: status.map(components_of).unwrap_or_default(),
            model_state: model
                .and_then(|m| m.transition_status.as_ref())
                .and_then(enum_str),
            loaded_copies: copies.and_then(|c| c.total_copies),
            failed_copies: copies.map(|c| c.failed_copies).filter(|&n| n > 0),
            reason,
            message,
            age: age(meta),
        }
    }
}

/// One fan-out candidate cluster in an OEP-0018 placement race.
#[derive(Serialize)]
pub struct PlacementCandidateView {
    pub cluster: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

/// Multi-cluster placement: where the ISVC landed, the race phase, the winning
/// endpoint, and the candidate clusters it fanned out to.
#[derive(Serialize)]
pub struct PlacementView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<PlacementCandidateView>,
}

fn placement_of(p: &InferenceServiceStatusPlacement) -> PlacementView {
    PlacementView {
        cluster: p.cluster.clone().filter(|s| !s.is_empty()),
        phase: p.phase.as_ref().and_then(enum_str),
        endpoint: p.endpoint.clone().filter(|s| !s.is_empty()),
        candidates: p
            .candidates
            .as_ref()
            .map(|cs| {
                cs.iter()
                    .map(|c| PlacementCandidateView {
                        cluster: c.cluster.clone(),
                        phase: c.phase.as_ref().and_then(enum_str),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// A canary rollout in flight: which step and the observed traffic weight.
#[derive(Serialize)]
pub struct CanaryView {
    #[serde(rename = "currentStep")]
    pub current_step: i32,
    #[serde(rename = "trafficWeight")]
    pub traffic_weight: i32,
}

/// Full InferenceService detail for the console drawer: the overview plus all
/// status conditions, multi-cluster placement, and any in-flight canary.
#[derive(Serialize)]
pub struct InferenceServiceDetail {
    #[serde(flatten)]
    pub view: InferenceServiceView,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<CanaryView>,
}

impl InferenceServiceDetail {
    pub fn of(isvc: &InferenceService) -> Self {
        let status = isvc.status.as_ref();
        Self {
            view: InferenceServiceView::of(isvc),
            conditions: conditions_of(status.and_then(|s| s.conditions.as_deref())),
            placement: status.and_then(|s| s.placement.as_ref()).map(placement_of),
            canary: status.and_then(|s| s.canary.as_ref()).map(|c| CanaryView {
                current_step: c.current_step,
                traffic_weight: c.observed_traffic_weight,
            }),
        }
    }
}

/// A rollout in progress: an InferenceService whose latest spec hasn't finished
/// rolling out (not `Ready`, or its generation isn't observed yet). Steady-state
/// services are omitted by [`RolloutView::progressing`].
#[derive(Serialize)]
pub struct RolloutView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "deploymentMode")]
    pub deployment_mode: Option<String>,
    pub ready: bool,
    /// `Ready` condition reason (e.g. `RevisionMissing`, `EngineNotReady`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl RolloutView {
    /// `Some` when the service is mid-rollout, `None` when it's steady (Ready and
    /// its spec generation observed).
    pub fn progressing(isvc: &InferenceService) -> Option<Self> {
        let meta = isvc.meta();
        let status = isvc.status.as_ref();
        let ready_cond = status
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"));
        let ready = ready_cond.map(|c| c.status == "True").unwrap_or(false);
        // Spec changed but not yet observed by the controller → still rolling out.
        let spec_pending = matches!(
            (status.and_then(|s| s.observed_generation), meta.generation),
            (Some(observed), Some(generation)) if observed < generation
        );
        if ready && !spec_pending {
            return None;
        }
        Some(Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            deployment_mode: isvc
                .spec
                .deployment_mode
                .as_ref()
                .and_then(|m| serde_json::to_value(m).ok())
                .and_then(|v| v.as_str().map(String::from)),
            ready,
            reason: ready_cond
                .map(|c| c.reason.clone())
                .filter(|s| !s.is_empty()),
            message: ready_cond
                .map(|c| c.message.clone())
                .filter(|s| !s.is_empty()),
            age: age(meta),
        })
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
    /// The node's accelerator: kind + schedulable/claimed/free chips (absent for
    /// non-accelerator nodes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accelerator: Option<Accel>,
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
    /// Problems with the node (NotReady / cordoned / pressure conditions); empty =
    /// healthy + schedulable. Powers the console's node explorer + why-down view.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub issues: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
}

impl NodeView {
    /// `accel_allocated` is the accelerator count claimed by pods on this node
    /// (computed from the Pod cache); `topo` selects which labels name the island /
    /// NVLink domain (or, for TPUs, the physical block / sub-block).
    pub fn of(node: &Node, accel_allocated: i64, topo: &Topology) -> Self {
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
        let accelerator = status
            .and_then(|s| s.allocatable.as_ref())
            .and_then(node_accel)
            .map(|(kind, allocatable)| Accel {
                kind,
                allocatable,
                allocated: accel_allocated,
                free: (allocatable - accel_allocated).max(0),
            });
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
            accelerator,
            cpu: cap("cpu"),
            memory: cap("memory").and_then(|m| humanize_mem(&m)),
            pods: cap("pods"),
            devices,
            kubelet_version: status
                .and_then(|s| s.node_info.as_ref())
                .map(|i| i.kubelet_version.clone()),
            issues: node_issues(node),
            age: age(meta),
        }
    }
}

/// Problems with a node, for the console — NotReady, pressure conditions, or
/// cordoned. Empty means healthy and schedulable. Taints are intentionally
/// excluded: accelerator nodes routinely carry a NoSchedule device taint that
/// isn't a fault, so flagging it would be noise.
fn node_issues(node: &Node) -> Vec<String> {
    let mut out = Vec::new();
    let conds = node.status.as_ref().and_then(|s| s.conditions.as_ref());
    let cond = |t: &str| conds.and_then(|cs| cs.iter().find(|c| c.type_ == t));
    if cond("Ready").map(|c| c.status != "True").unwrap_or(true) {
        out.push("NotReady".to_owned());
    }
    for t in [
        "MemoryPressure",
        "DiskPressure",
        "PIDPressure",
        "NetworkUnavailable",
    ] {
        if cond(t).map(|c| c.status == "True").unwrap_or(false) {
            out.push(t.to_owned());
        }
    }
    if node
        .spec
        .as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false)
    {
        out.push("cordoned".to_owned());
    }
    out
}

/// A scheduling taint on a node (key + effect; value when set).
#[derive(Serialize)]
pub struct TaintView {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub effect: String,
}

/// A pod scheduled on a node, with the accelerators it holds — so an operator can
/// see what a drain would evict.
#[derive(Serialize)]
pub struct NodePod {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accelerators: Option<i64>,
}

/// Full node detail for the console drill-down: the overview plus status
/// conditions, scheduling taints, and the pods currently on it.
#[derive(Serialize)]
pub struct NodeDetail {
    #[serde(flatten)]
    pub view: NodeView,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub taints: Vec<TaintView>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "podsOnNode")]
    pub pods_on_node: Vec<NodePod>,
}

impl NodeDetail {
    /// `pods_on_node` is built by the handler (it holds the Pod cache); the rest is
    /// projected from the Node object.
    pub fn of(
        node: &Node,
        accel_allocated: i64,
        pods_on_node: Vec<NodePod>,
        topo: &Topology,
    ) -> Self {
        Self {
            view: NodeView::of(node, accel_allocated, topo),
            conditions: node_conditions(node),
            taints: node
                .spec
                .as_ref()
                .and_then(|s| s.taints.as_ref())
                .map(|ts| {
                    ts.iter()
                        .map(|t| TaintView {
                            key: t.key.clone(),
                            value: t.value.clone().filter(|v| !v.is_empty()),
                            effect: t.effect.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            pods_on_node,
        }
    }
}

/// Project a node's *notable* status conditions to `ConditionView`: `Ready` plus
/// anything currently `True`. Nodes (esp. with a problem-detector) carry dozens of
/// conditions that sit `False` in steady state — keeping only Ready + active ones
/// drops that noise while still surfacing every fault that's firing.
fn node_conditions(node: &Node) -> Vec<ConditionView> {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter()
                .filter(|c| c.type_ == "Ready" || c.status == "True")
                .map(|c| ConditionView {
                    type_: c.type_.clone(),
                    status: c.status.clone(),
                    reason: c.reason.clone().filter(|s| !s.is_empty()),
                    message: c.message.clone().filter(|s| !s.is_empty()),
                })
                .collect()
        })
        .unwrap_or_default()
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

// ===========================================================================
// Capacity — the `GET /v1/capacity` response, built by `rollup::build`.
// ===========================================================================

/// Fleet-wide capacity report.
#[derive(Serialize)]
pub struct CapacityReport {
    pub nodes: NodeCounts,
    /// Fleet accelerator totals — one entry per kind (e.g. `gpu`, `tpu`).
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
    pub schedulable: usize,
    #[serde(rename = "acceleratorNodes")]
    pub accelerator_nodes: usize,
}

/// Accelerator capacity for one group (a kind total, cluster, island, NVLink/ICI
/// domain, or instance type), tagged with the accelerator `kind`.
#[derive(Serialize)]
pub struct GroupCapacity {
    pub name: String,
    pub kind: String,
    pub nodes: usize,
    pub allocatable: i64,
    pub allocated: i64,
    pub free: i64,
}

// ===========================================================================
// Live updates — activity rows are human-facing history, while invalidations are
// compact cache-refresh hints. Both travel over `/v1/stream` as distinct named
// SSE events so changing activity presentation cannot break live query refresh.
// ===========================================================================

/// One observed change to a watched resource.
#[derive(Clone, Serialize)]
pub struct FeedEvent {
    pub cluster: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub name: String,
    /// `"applied"` (add or update) or `"deleted"`.
    pub action: &'static str,
    /// Wall-clock seconds since the Unix epoch when muse observed the change.
    pub at: u64,
}

/// Resource/query domains whose cached HTTP projections may have changed.
///
/// A single event can cover a burst across several domains. Values are stable
/// protocol names such as `InferenceService`, `Node`, and the derived `Capacity`
/// projection; clients match only the domains used by their query.
#[derive(Clone, Debug, Serialize)]
pub struct InvalidationEvent {
    pub resources: Vec<String>,
}

// ===========================================================================
// Request query params.
// ===========================================================================

/// Query params common to the aggregating endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct ListParams {
    /// Restrict to a single cluster by name.
    pub cluster: Option<String>,
}

/// Query params for `GET /v1/{kind}/{name}`.
#[derive(Debug, Default, Deserialize)]
pub struct ViewParams {
    /// Return the full underlying object instead of the projected view.
    #[serde(default)]
    pub raw: bool,
    /// Restrict the lookup to a single cluster by name.
    pub cluster: Option<String>,
}

/// Query params for the inference test proxy: which cluster the ISVC lives on, and
/// which endpoint path to POST to (engines differ — vLLM `/v1/chat/completions`,
/// sglang `/generate`).
#[derive(Debug, Default, Deserialize)]
pub struct ChatParams {
    pub cluster: Option<String>,
    /// Endpoint path on the model's own URL. Required; it must be an absolute path
    /// (no scheme/host) so it cannot redirect the proxy off the selected base.
    pub path: Option<String>,
    /// Override the target base URL. Needed because `status.url` often points at an
    /// unreachable/internal gateway (OEP-0023) while the external gateway works —
    /// the console lets the user pick. Accepted only when its host is within the
    /// exact status origin or an explicitly configured host/suffix. Empty means use
    /// `status.url`; no parent domain is inferred from either hostname.
    pub base: Option<String>,
}

// ===========================================================================
// Response envelopes.
// ===========================================================================

/// A list response: `{ kind?, count, items }`. Items are pre-projected (each
/// already carries its owning `cluster`), so they're held as raw JSON.
#[derive(Serialize)]
pub struct Listing {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub count: usize,
    pub items: Vec<Value>,
}

impl Listing {
    pub fn new(items: Vec<Value>) -> Self {
        Self {
            kind: None,
            count: items.len(),
            items,
        }
    }
    pub fn of_kind(kind: impl Into<String>, items: Vec<Value>) -> Self {
        Self {
            kind: Some(kind.into()),
            count: items.len(),
            items,
        }
    }
}

/// Per-cluster summary row for `GET /v1/clusters`.
#[derive(Serialize)]
pub struct ClusterSummary {
    pub name: String,
    pub ready: bool,
    pub status: &'static str,
    /// Apiserver Kubernetes version (e.g. "v1.35.6").
    pub version: String,
    /// Cloud region, from the standard `topology.kubernetes.io/region` node label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub nodes: usize,
    #[serde(rename = "acceleratorNodes")]
    pub accelerator_nodes: usize,
    #[serde(rename = "inferenceServices")]
    pub inference_services: usize,
}

/// Console feature flags for `GET /v1/config`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigResp {
    pub chat: bool,
    /// Total budget (seconds) for one proxied chat request. The console bounds its
    /// own request a little above this so a stalled endpoint can't hang the UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_timeout_secs: Option<u64>,
    /// Extra browser-side time beyond the server budget, supplied by deployment
    /// config rather than baked into the console.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_client_grace_secs: Option<u64>,
    /// Whether the metrics proxy (`/v1/metrics/*` → a cluster's Prometheus) is
    /// configured; the console hides its metric panels when false.
    pub metrics: bool,
}

/// Serialize any value to JSON, or `Null` on failure.
pub fn to_val<T: Serialize>(value: T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Serialize a projected view and stamp the owning `cluster` onto it. Every list
/// row and detail object carries which cluster it came from; injecting it here
/// keeps the view types cluster-agnostic. A non-object view is returned unchanged.
pub fn tagged<T: Serialize>(view: T, cluster: &str) -> Value {
    let mut value = to_val(view);
    if let Value::Object(map) = &mut value {
        map.insert("cluster".to_owned(), Value::String(cluster.to_owned()));
    }
    value
}
