//! The wire protocol: every request query param and response body the console
//! exchanges with muse-server, in one place (the capacity, pod, and
//! system-component types live in this module's submodules and are re-exported
//! here, so `crate::protocol` stays the single namespace).
//!
//! Response bodies are clean, stable projections of the raw k8s/CRD objects — a
//! documented contract decoupled from CRD/Node internals (and a far smaller
//! payload); the full object is still available per-object via
//! `GET /v1/{kind}/{name}?raw=true`. The projection builders (`…::of`) live here
//! too; the modules that produce these responses (capacity, feed, resources)
//! reference these types.

mod capacity;
mod pods;
mod system;

pub use capacity::{
    first_label, node_accel, sum_accel, Accel, AccelDemand, AccelLost, CapacityReport, Consumer,
    DeploymentConsumer, Fragment, GpuView, GroupCapacity, NodeCounts, PendingDeployment,
    PlacementUnits, Topology, UnitBox,
};
pub(crate) use pods::{
    isvc_owner, owned_by, pod_accelerators, pod_component, pod_problem_reason,
    INFERENCE_SERVICE_POD_LABEL,
};
pub use pods::{IsvcPodView, IsvcPodsView, PodInspectView};
pub use system::{
    ComponentHealth, SystemComponent, SystemComponentKind, SystemComponentRow, SystemComponents,
    SystemPodView,
};

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use kube::Resource;
use ome_client::{
    inferenceservices::{
        InferenceServiceStatus, InferenceServiceStatusCanary,
        InferenceServiceStatusComponentsRolloutPhase, InferenceServiceStatusPlacement,
        InferenceServiceStatusRolloutCoordinationGroupsPhase,
    },
    k8s_openapi::{
        api::core::v1::Node,
        apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time},
    },
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceService,
    ServingRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
pub(crate) fn enum_str<T: Serialize>(value: &T) -> Option<String> {
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
pub(crate) fn some_nonempty(s: &str) -> Option<String> {
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

impl ConditionView {
    /// Project a condition's fields, dropping empty reason/message.
    pub(crate) fn new(
        type_: &str,
        status: &str,
        reason: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            type_: type_.to_owned(),
            status: status.to_owned(),
            reason: reason.filter(|s| !s.is_empty()),
            message: message.filter(|s| !s.is_empty()),
        }
    }
}

/// Project a slice of conditions into [`ConditionView`]s (empty reason/message
/// dropped).
fn conditions_of(conds: Option<&[Condition]>) -> Vec<ConditionView> {
    conds
        .unwrap_or_default()
        .iter()
        .map(|c| {
            ConditionView::new(
                &c.type_,
                &c.status,
                Some(c.reason.clone()),
                Some(c.message.clone()),
            )
        })
        .collect()
}

/// The `Ready` status condition of an InferenceService, when reported.
fn ready_condition(status: &InferenceServiceStatus) -> Option<&Condition> {
    status
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == "Ready")
}

/// One InferenceService Component (engine / decoder / router): readiness (from
/// the matching `<Comp>Ready` condition), live replica counts, rollout phase, and
/// the revision currently owning traffic.
#[derive(Serialize)]
pub struct ComponentView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
    /// Ready Instances, from `lifecycle.readyReplicas`. Excludes still-
    /// initializing Instances, unlike the autoscaler's `currentReplicas`
    /// scale count below. None for non-OMENative Components.
    #[serde(skip_serializing_if = "Option::is_none", rename = "readyReplicas")]
    pub ready_replicas: Option<i32>,
    /// Total Instances, from `lifecycle.replicas`. None for non-OMENative Components.
    #[serde(skip_serializing_if = "Option::is_none", rename = "totalReplicas")]
    pub total_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "currentReplicas")]
    pub current_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "desiredReplicas")]
    pub desired_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "rolloutPhase")]
    pub rollout_phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Revision the bulk of Instances currently run (`lifecycle.currentRevision`).
    /// Differs from `updateRevision` only while a rollout is converging.
    #[serde(skip_serializing_if = "Option::is_none", rename = "currentRevision")]
    pub current_revision: Option<String>,
    /// Revision the rollout is converging to (`lifecycle.updateRevision`).
    #[serde(skip_serializing_if = "Option::is_none", rename = "updateRevision")]
    pub update_revision: Option<String>,
    /// Instances already on `updateRevision` (`lifecycle.updatedReplicas`).
    #[serde(skip_serializing_if = "Option::is_none", rename = "updatedReplicas")]
    pub updated_replicas: Option<i32>,
    /// Ready Instances on `updateRevision` (`lifecycle.updatedReadyReplicas`) —
    /// the true measure of rollout progress while old-revision pods still count
    /// toward `readyReplicas`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "updatedReadyReplicas"
    )]
    pub updated_ready_replicas: Option<i32>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "latestReadyRevision"
    )]
    pub latest_ready_revision: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "latestRolledoutRevision"
    )]
    pub latest_rolledout_revision: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "previousRolledoutRevision"
    )]
    pub previous_rolledout_revision: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub traffic: Vec<ComponentTrafficView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentTrafficView {
    pub revision_name: String,
    pub percent: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_revision: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
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
        .map(|(name, c)| {
            let lifecycle = c.lifecycle.as_ref();
            ComponentView {
                name: name.clone(),
                ready: component_ready(status, name),
                ready_replicas: lifecycle.and_then(|l| l.ready_replicas),
                total_replicas: lifecycle.and_then(|l| l.replicas),
                current_replicas: c.autoscaler.as_ref().and_then(|a| a.current_replicas),
                desired_replicas: c.autoscaler.as_ref().and_then(|a| a.desired_replicas),
                rollout_phase: c.rollout_phase.as_ref().and_then(enum_str),
                revision: c
                    .latest_rolledout_revision
                    .clone()
                    .or_else(|| c.latest_ready_revision.clone())
                    .filter(|s| !s.is_empty()),
                current_revision: lifecycle
                    .and_then(|l| l.current_revision.clone())
                    .filter(|s| !s.is_empty()),
                update_revision: lifecycle
                    .and_then(|l| l.update_revision.clone())
                    .filter(|s| !s.is_empty()),
                updated_replicas: lifecycle.and_then(|l| l.updated_replicas),
                updated_ready_replicas: lifecycle.and_then(|l| l.updated_ready_replicas),
                latest_ready_revision: c.latest_ready_revision.clone().filter(|s| !s.is_empty()),
                latest_rolledout_revision: c
                    .latest_rolledout_revision
                    .clone()
                    .filter(|s| !s.is_empty()),
                previous_rolledout_revision: c
                    .previous_rolledout_revision
                    .clone()
                    .filter(|s| !s.is_empty()),
                traffic: c
                    .traffic
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|t| ComponentTrafficView {
                        revision_name: t.revision_name.clone(),
                        percent: t.percent,
                        latest_revision: t.latest_revision,
                        tag: t.tag.clone().filter(|s| !s.is_empty()),
                    })
                    .collect(),
                url: c.url.clone().filter(|s| !s.is_empty()),
            }
        })
        .collect();
    out.sort_by_key(|c| order(&c.name));
    out
}

/// Why the service isn't ready: the `Ready` condition's reason/message when it is
/// not True, else the model-load failure (if any). `(reason, message)`.
fn not_ready_info(status: &InferenceServiceStatus) -> (Option<String>, Option<String>) {
    if let Some(c) = ready_condition(status) {
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
    #[serde(skip_serializing_if = "Option::is_none", rename = "modelRef")]
    pub model_ref: Option<ResourceRefView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "runtimeRef")]
    pub runtime_ref: Option<ResourceRefView>,
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
    /// In-flight canary rollout (step, observed traffic weight); absent outside
    /// a canary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<CanaryView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
    /// Accelerator chips currently held by this service's scheduled pods. Filled
    /// by the resource endpoints (which hold the pod caches); absent when zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accelerators: Option<i64>,
    /// The runtime the operator actually resolves for this service (see
    /// [`ResolvedRuntimeRefView`]). Filled by the resource endpoints, which hold
    /// the runtime caches this is resolved against; absent when the runtime name
    /// matches no cached runtime. `runtime`/`runtimeRef` above keep carrying the
    /// spec declaration verbatim.
    #[serde(skip_serializing_if = "Option::is_none", rename = "resolvedRuntimeRef")]
    pub resolved_runtime_ref: Option<ResolvedRuntimeRefView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryView>,
}

impl InferenceServiceView {
    pub fn of(isvc: &InferenceService) -> Self {
        let meta = isvc.meta();
        let status = isvc.status.as_ref();
        let ready = status.and_then(ready_condition).map(|c| c.status == "True");
        let (reason, message) = status.map(not_ready_info).unwrap_or((None, None));
        let model = status.and_then(|s| s.model_status.as_ref());
        let copies = model.and_then(|m| m.model_copies.as_ref());
        let model_ref = isvc.spec.model.as_ref().and_then(|model| {
            resource_ref(
                &model.name,
                model.kind.as_deref().unwrap_or("ClusterBaseModel"),
                meta.namespace.as_deref(),
            )
        });
        let runtime_ref = isvc.spec.runtime.as_ref().and_then(|runtime| {
            resource_ref(
                &runtime.name,
                runtime.kind.as_deref().unwrap_or("ClusterServingRuntime"),
                meta.namespace.as_deref(),
            )
        });
        Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            model: isvc.spec.model.as_ref().map(|m| m.name.clone()),
            model_ref,
            runtime: isvc.spec.runtime.as_ref().map(|r| r.name.clone()),
            runtime_ref,
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
            canary: status.and_then(|s| s.canary.as_ref()).map(CanaryView::of),
            age: age(meta),
            accelerators: None,
            resolved_runtime_ref: None,
            delivery: None,
        }
    }
}

/// A resource reference carried by projected API views. `namespace` is present
/// only for namespaced kinds, so the console can preserve full Kubernetes
/// identity when it follows a model/runtime link.
#[derive(Serialize)]
pub struct ResourceRefView {
    pub kind: &'static str,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// The runtime identity an InferenceService actually uses, resolved the way the
/// operator resolves the name: a namespaced ServingRuntime shadows a
/// ClusterServingRuntime with the same name, and the kind declared on the spec is
/// not consulted. `source` records where the name came from — `"spec"` when
/// declared on the InferenceService, `"pods"` when read from the labels of the
/// service's running pods (the only place an auto-selected runtime is recorded).
#[derive(Serialize)]
pub struct ResolvedRuntimeRefView {
    pub kind: &'static str,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub source: &'static str,
}

fn resource_ref(name: &str, kind: &str, owner_namespace: Option<&str>) -> Option<ResourceRefView> {
    let (kind, namespace) = match kind {
        "BaseModel" => ("BaseModel", owner_namespace.map(str::to_owned)),
        "ClusterBaseModel" => ("ClusterBaseModel", None),
        "ServingRuntime" => ("ServingRuntime", owner_namespace.map(str::to_owned)),
        "ClusterServingRuntime" => ("ClusterServingRuntime", None),
        _ => return None,
    };
    Some(ResourceRefView {
        kind,
        name: name.to_owned(),
        namespace,
    })
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

/// Canary rollout state: which step and for how long, the observed traffic
/// weight, the candidate/stable identity, and the analysis evidence needed to
/// explain promotion or rollback decisions.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryView {
    pub current_step: i32,
    pub traffic_weight: i32,
    /// Analysis checks currently failing; absent when none are.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_checks: Option<i32>,
    /// Humanized time in the current step (from `stepEnteredTime`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_age: Option<String>,
    /// The analysis checks currently failing, projected for compact display
    /// (Fleet chips, attention detail). Empty (omitted) when everything
    /// passes; `metric_results` below carries the full evidence.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failing_checks: Vec<CanaryCheckView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_revision_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolled_back_revision_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_entered_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluation_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_conclusive_evaluation_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analysis_failed_checks: Option<i32>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub metric_results: Vec<CanaryMetricView>,
}

/// One failing canary analysis check: what was measured against what.
#[derive(Serialize)]
pub struct CanaryCheckView {
    pub name: String,
    /// The gate it must satisfy, e.g. "<= 0.5".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize)]
pub struct CanaryMetricView {
    pub name: String,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RolloutCoordinationView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub components: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub composite_phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

fn coordination_of(status: &InferenceServiceStatus) -> Vec<RolloutCoordinationView> {
    status
        .rollout_coordination
        .as_ref()
        .and_then(|r| r.groups.as_deref())
        .unwrap_or_default()
        .iter()
        .map(|g| RolloutCoordinationView {
            name: g.name.clone(),
            phase: g.phase.as_ref().and_then(enum_str),
            policy: g.policy.as_ref().and_then(enum_str),
            components: g.components.clone().unwrap_or_default(),
            current_component: g.current_component.clone().filter(|s| !s.is_empty()),
            previous_component: g.previous_component.clone().filter(|s| !s.is_empty()),
            composite_phase: g.composite_phase.clone().filter(|s| !s.is_empty()),
            last_transition_time: g.last_transition_time.clone().filter(|s| !s.is_empty()),
            message: g.message.clone().filter(|s| !s.is_empty()),
        })
        .collect()
}

impl CanaryView {
    pub(crate) fn of(c: &InferenceServiceStatusCanary) -> Self {
        Self {
            current_step: c.current_step,
            traffic_weight: c.observed_traffic_weight,
            failed_checks: c.analysis_failed_checks.filter(|&n| n > 0),
            step_age: c.step_entered_time.as_deref().and_then(since_status_time),
            failing_checks: failing_checks(c),
            canary_revision_hash: c.canary_revision_hash.clone().filter(|s| !s.is_empty()),
            rolled_back_revision_hash: c
                .rolled_back_revision_hash
                .clone()
                .filter(|s| !s.is_empty()),
            step_entered_time: c.step_entered_time.clone().filter(|s| !s.is_empty()),
            last_evaluation_time: c.last_evaluation_time.clone().filter(|s| !s.is_empty()),
            last_conclusive_evaluation_time: c
                .last_conclusive_evaluation_time
                .clone()
                .filter(|s| !s.is_empty()),
            analysis_failed_checks: c.analysis_failed_checks,
            metric_results: c
                .metric_results
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|m| CanaryMetricView {
                    name: m.name.clone(),
                    passed: m.passed,
                    value: m.value.clone().filter(|s| !s.is_empty()),
                    threshold: m.threshold.clone().filter(|s| !s.is_empty()),
                    operator: m.operator.clone().filter(|s| !s.is_empty()),
                    message: m.message.clone().filter(|s| !s.is_empty()),
                    time: m.time.clone().filter(|s| !s.is_empty()),
                })
                .collect(),
        }
    }
}

/// The failing entries of a canary's analysis results, projected for display.
pub(crate) fn failing_checks(c: &InferenceServiceStatusCanary) -> Vec<CanaryCheckView> {
    c.metric_results
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|r| !r.passed)
        .map(|r| CanaryCheckView {
            name: r.name.clone(),
            threshold: match (r.operator.as_deref(), r.threshold.as_deref()) {
                (Some(op), Some(t)) => Some(format!("{op} {t}")),
                (None, Some(t)) => Some(t.to_owned()),
                _ => None,
            },
            value: r.value.clone().filter(|s| !s.is_empty()),
            message: r.message.clone().filter(|s| !s.is_empty()),
        })
        .collect()
}

/// Full InferenceService detail for the console drawer: the overview (which
/// carries any in-flight canary) plus all status conditions and multi-cluster
/// placement.
#[derive(Serialize)]
pub struct InferenceServiceDetail {
    #[serde(flatten)]
    pub view: InferenceServiceView,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementView>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "rolloutCoordination")]
    pub rollout_coordination: Vec<RolloutCoordinationView>,
}

impl InferenceServiceDetail {
    pub fn of(isvc: &InferenceService) -> Self {
        let status = isvc.status.as_ref();
        Self {
            view: InferenceServiceView::of(isvc),
            conditions: conditions_of(status.and_then(|s| s.conditions.as_deref())),
            placement: status.and_then(|s| s.placement.as_ref()).map(placement_of),
            rollout_coordination: status.map(coordination_of).unwrap_or_default(),
        }
    }
}

/// An active or terminal-attention OME rollout, or a spec whose generation the
/// controller has not observed yet. Serving readiness is carried separately.
#[derive(Serialize)]
pub struct RolloutView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "deploymentMode")]
    pub deployment_mode: Option<String>,
    pub ready: bool,
    /// An OME rollout phase, or MUSE's `Reconciling` fallback while
    /// `status.observedGeneration` trails `metadata.generation`.
    pub phase: String,
    pub state: RolloutState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "progressPercent")]
    pub progress_percent: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "startedAt")]
    pub started_at: Option<String>,
    /// `Ready` condition reason (e.g. `RevisionMissing`, `EngineNotReady`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub components: Vec<ComponentView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<CanaryView>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "rolloutCoordination")]
    pub rollout_coordination: Vec<RolloutCoordinationView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryView>,
}

impl RolloutView {
    /// `Some` for an explicit OME rollout phase or stale observed generation. A
    /// service that is merely NotReady is health-only and does not appear here.
    pub fn noteworthy(isvc: &InferenceService) -> Option<Self> {
        let meta = isvc.meta();
        let status = isvc.status.as_ref()?;
        let ready_cond = ready_condition(status);
        let ready = ready_cond.map(|c| c.status == "True").unwrap_or(false);
        let coordination = coordination_of(status);
        let attention = rollout_attention(status).or_else(|| reconciliation_attention(isvc))?;
        let canary = status.canary.as_ref().map(CanaryView::of);
        let started_at = canary
            .as_ref()
            .and_then(|c| c.step_entered_time.clone())
            .or_else(|| {
                coordination
                    .iter()
                    .filter_map(|g| g.last_transition_time.clone())
                    .max()
            });
        Some(Self {
            name: meta.name.clone().unwrap_or_default(),
            namespace: meta.namespace.clone(),
            deployment_mode: isvc.spec.deployment_mode.as_ref().and_then(enum_str),
            ready,
            phase: attention.phase,
            state: attention.state,
            strategy: rollout_strategy(isvc),
            progress_percent: canary.as_ref().map(|c| c.traffic_weight),
            started_at,
            reason: ready_cond.and_then(|c| some_nonempty(&c.reason)),
            message: ready_cond.and_then(|c| some_nonempty(&c.message)),
            components: components_of(status),
            canary,
            rollout_coordination: coordination,
            delivery: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RolloutState {
    Active,
    Terminal,
}

struct RolloutAttention {
    phase: String,
    state: RolloutState,
    priority: u8,
}

fn component_rollout_attention(
    phase: &InferenceServiceStatusComponentsRolloutPhase,
) -> Option<RolloutAttention> {
    use InferenceServiceStatusComponentsRolloutPhase as Phase;

    let (state, priority) = match phase {
        Phase::Stable => return None,
        Phase::Failed => (RolloutState::Terminal, 100),
        Phase::RolledBack => (RolloutState::Terminal, 90),
        Phase::RollingBack => (RolloutState::Active, 80),
        Phase::Promoting => (RolloutState::Active, 70),
        Phase::Paused => (RolloutState::Active, 60),
        Phase::Canarying => (RolloutState::Active, 50),
        Phase::BlueGreenStandby => (RolloutState::Active, 40),
        Phase::Pending => (RolloutState::Active, 30),
    };
    Some(RolloutAttention {
        phase: state_string(phase),
        state,
        priority,
    })
}

fn coordination_rollout_attention(
    phase: &InferenceServiceStatusRolloutCoordinationGroupsPhase,
) -> Option<RolloutAttention> {
    use InferenceServiceStatusRolloutCoordinationGroupsPhase as Phase;

    let (state, priority) = match phase {
        Phase::Idle => return None,
        Phase::Failed => (RolloutState::Terminal, 100),
        Phase::RollingBack => (RolloutState::Active, 80),
        Phase::Paused => (RolloutState::Active, 60),
        Phase::Draining => (RolloutState::Active, 55),
        Phase::Shifting => (RolloutState::Active, 50),
        Phase::Waiting => (RolloutState::Active, 45),
        Phase::Surging => (RolloutState::Active, 40),
        Phase::ScalingDown => (RolloutState::Active, 35),
    };
    Some(RolloutAttention {
        phase: state_string(phase),
        state,
        priority,
    })
}

fn rollout_attention(status: &InferenceServiceStatus) -> Option<RolloutAttention> {
    let components = status
        .components
        .as_ref()
        .into_iter()
        .flat_map(|components| components.values())
        .filter_map(|component| {
            component
                .rollout_phase
                .as_ref()
                .and_then(component_rollout_attention)
        });
    let coordination = status
        .rollout_coordination
        .as_ref()
        .and_then(|rollout| rollout.groups.as_deref())
        .unwrap_or_default()
        .iter()
        .filter_map(|group| {
            group
                .phase
                .as_ref()
                .and_then(coordination_rollout_attention)
        });
    components
        .chain(coordination)
        .max_by_key(|attention| attention.priority)
}

/// Fall back to the Kubernetes spec/status handshake before OME has declared a
/// rollout phase. This catches a controller that has not observed the latest
/// spec without treating every NotReady service as a rollout.
fn reconciliation_attention(isvc: &InferenceService) -> Option<RolloutAttention> {
    let generation = isvc.meta().generation?;
    let observed = isvc
        .status
        .as_ref()
        .and_then(|status| status.observed_generation)?;
    if observed >= generation {
        return None;
    }
    Some(RolloutAttention {
        phase: "Reconciling".to_owned(),
        state: RolloutState::Active,
        priority: 0,
    })
}

fn rollout_strategy(isvc: &InferenceService) -> Option<String> {
    let groups = isvc.spec.rollout.as_ref()?.groups.as_deref()?;
    let mut strategies = BTreeMap::<&'static str, ()>::new();
    for group in groups {
        if group.canary.is_some() {
            strategies.insert("Canary", ());
        }
        if group.blue_green.is_some() {
            strategies.insert("BlueGreen", ());
        }
        if group.rolling_update.is_some() {
            strategies.insert("RollingUpdate", ());
        }
    }
    match strategies.len() {
        0 => None,
        1 => strategies.into_keys().next().map(str::to_owned),
        _ => Some("Mixed".to_owned()),
    }
}

/// Argo CD delivery state for the Application and exact tracked resource.
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryView {
    pub application: String,
    pub sync_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_sync_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_health_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_health_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_pruning: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciled_at: Option<String>,
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
/// see what a drain would evict — and, for OME serving pods, the InferenceService
/// that owns it.
#[derive(Serialize)]
pub struct NodePod {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accelerators: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
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
                .map(|c| {
                    ConditionView::new(&c.type_, &c.status, c.reason.clone(), c.message.clone())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A label value, treating absent and empty-string as the same (None).
pub(crate) fn nonempty(value: Option<&String>) -> Option<String> {
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

/// Humanized elapsed time since a k8s `Time`. k8s_openapi's `Time` wraps a
/// `jiff::Timestamp`; compare its epoch seconds against wall-clock seconds.
fn since(time: &Time) -> Option<String> {
    Some(humanize_secs(secs_since(time)?))
}

/// Whole seconds elapsed since `time`; clamped at zero for clock skew.
fn secs_since(time: &Time) -> Option<i64> {
    let then = time.0.as_second();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    Some((now - then).max(0))
}

fn humanize_secs(secs: i64) -> String {
    if secs >= 86_400 {
        format!("{}d", secs / 86_400)
    } else if secs >= 3_600 {
        format!("{}h", secs / 3_600)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// Some status fields carry RFC 3339 timestamps as plain strings; parse through
/// the Kubernetes `Time` deserializer so they follow the same clock handling.
fn parse_status_time(s: &str) -> Option<Time> {
    serde_json::from_value(Value::String(s.to_owned())).ok()
}

/// Whole seconds elapsed since an RFC 3339 status timestamp.
pub(crate) fn secs_since_status_time(s: &str) -> Option<i64> {
    secs_since(&parse_status_time(s)?)
}

/// Humanized elapsed time since an RFC 3339 status timestamp.
pub(crate) fn since_status_time(s: &str) -> Option<String> {
    Some(humanize_secs(secs_since_status_time(s)?))
}

/// Humanized object age from its creation timestamp.
fn age(meta: &ObjectMeta) -> Option<String> {
    since(meta.creation_timestamp.as_ref()?)
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
    /// What actually changed ("created", "engine scaled 4 → 8", "became
    /// ready"); absent for deletions. Events with nothing narratable are
    /// dropped before reaching the feed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
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
    /// Namespace of a namespaced resource. Omit only for cluster-scoped kinds or
    /// legacy callers, which retain the old first-name-match behavior.
    pub namespace: Option<String>,
}

/// Query params for the inference test proxy: which cluster the ISVC lives on, and
/// which endpoint path to POST to (engines differ — vLLM `/v1/chat/completions`,
/// sglang `/generate`).
#[derive(Debug, Default, Deserialize)]
pub struct ChatParams {
    pub cluster: Option<String>,
    /// Namespace of the InferenceService. Omit only for legacy callers.
    pub namespace: Option<String>,
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

/// How urgently a triage finding needs a human, ordered most severe first so
/// the derived `Ord` is the triage sort order. Serializes via [`Self::as_str`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Definitively broken.
    Critical,
    /// Degraded / needs a human.
    Warning,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Warning => "warning",
        }
    }
}

impl Serialize for Severity {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// A triage finding's domain, ordered so cluster-level problems outrank workload
/// ones under the derived `Ord`. Serializes via [`Self::as_str`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Category {
    Cluster,
    Deployment,
    Rollout,
    Pods,
    Model,
}

impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Deployment => "deployment",
            Self::Rollout => "rollout",
            Self::Pods => "pods",
            Self::Model => "model",
        }
    }
}

impl Serialize for Category {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// One fleet-triage finding: something an operator should look at now, with
/// enough context to jump straight to the page that diagnoses it.
#[derive(Serialize)]
pub struct AttentionItem {
    pub severity: Severity,
    pub category: Category,
    pub cluster: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Resource kind for click-through (`InferenceService`, `BaseModel`, …).
    pub kind: &'static str,
    pub name: String,
    /// One line of "why this is here".
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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

/// Thresholds where larger values are worse. A value at the warning threshold is
/// warning; a value at the critical threshold is critical.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpperThresholdPolicy {
    pub warning_at: f64,
    pub critical_at: f64,
}

/// Thresholds where smaller values are worse. A value below the warning threshold
/// is warning; a value below the critical threshold is critical.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LowerThresholdPolicy {
    pub warning_below: f64,
    pub critical_below: f64,
}

/// Optional dashboard health policy. Percentage-valued metrics use 0..100;
/// cache ratios use 0..1. Missing categories render without a health tone.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DashboardPolicy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utilization: Option<UpperThresholdPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability: Option<LowerThresholdPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_rate: Option<UpperThresholdPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kv_cache: Option<UpperThresholdPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_cache: Option<LowerThresholdPolicy>,
}

impl DashboardPolicy {
    pub fn is_empty(&self) -> bool {
        self.utilization.is_none()
            && self.availability.is_none()
            && self.error_rate.is_none()
            && self.kv_cache.is_none()
            && self.prefix_cache.is_none()
    }
}

/// One operator-configured window offered by the Deployment Monitoring range
/// selector. The label is carried over the API rather than invented by the UI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricsTimeRangeOption {
    pub id: String,
    pub label: String,
    pub seconds: u64,
    /// Query Gateway evaluation cadence for this window. Kept in deployment
    /// configuration because supported granularity and cost are backend-specific.
    pub step_seconds: u64,
}

/// The Deployment Monitoring range choices and their explicit initial selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricsTimeRanges {
    pub default: String,
    pub options: Vec<MetricsTimeRangeOption>,
}

/// Console feature flags and display policy for `GET /v1/config`.
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
    /// Whether on-demand pod inspection (live status, events, log tails) is
    /// configured; the console hides those affordances when false.
    pub pod_inspect: bool,
    /// Log-tail line cap for pod inspection, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_log_tail_lines: Option<usize>,
    /// Whether live log streaming is configured; hides the stream affordances
    /// when false.
    pub pod_log_stream: bool,
    /// Pods one stream session follows at most, when streaming is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_log_stream_max_pods: Option<usize>,
    /// Whether the Query Gateway client is configured; the console hides
    /// metric panels when false.
    pub metrics: bool,
    /// Whether the catalog contains queries with safe fleet-wide attribution.
    pub fleet_metrics: bool,
    /// Active-view browser refresh cadence.
    pub metrics_refresh_secs: u64,
    /// Optional health classification policy. Omitted when no categories are
    /// configured, which tells the console to render neutral status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dashboard_policy: Option<DashboardPolicy>,
    /// Optional Deployment Monitoring range selector policy. Omitted means the
    /// console shows the full cached query range without a selector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_time_ranges: Option<MetricsTimeRanges>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_refs_preserve_scope_and_namespace() {
        let namespaced =
            resource_ref("shared", "BaseModel", Some("team-a")).expect("BaseModel is supported");
        assert_eq!(namespaced.kind, "BaseModel");
        assert_eq!(namespaced.namespace.as_deref(), Some("team-a"));

        let cluster = resource_ref("shared", "ClusterBaseModel", Some("team-a"))
            .expect("ClusterBaseModel is supported");
        assert_eq!(cluster.kind, "ClusterBaseModel");
        assert!(cluster.namespace.is_none());

        let runtime = resource_ref("vllm", "ServingRuntime", Some("team-a"))
            .expect("ServingRuntime is supported");
        assert_eq!(runtime.namespace.as_deref(), Some("team-a"));

        let cluster_runtime = resource_ref("vllm", "ClusterServingRuntime", Some("team-a"))
            .expect("ClusterServingRuntime is supported");
        assert!(cluster_runtime.namespace.is_none());

        assert!(resource_ref("shared", "OtherModel", Some("team-a")).is_none());
    }

    #[test]
    fn attention_enums_serialize_to_their_wire_strings() {
        for severity in [Severity::Critical, Severity::Warning] {
            assert_eq!(
                to_val(severity),
                Value::String(severity.as_str().to_owned())
            );
        }
        for category in [
            Category::Cluster,
            Category::Deployment,
            Category::Rollout,
            Category::Pods,
            Category::Model,
        ] {
            assert_eq!(
                to_val(category),
                Value::String(category.as_str().to_owned())
            );
        }
    }

    #[test]
    fn console_config_exposes_metrics_refresh_policy() {
        let response = ConfigResp {
            chat: false,
            chat_timeout_secs: None,
            chat_client_grace_secs: None,
            pod_inspect: false,
            pod_log_tail_lines: None,
            pod_log_stream: false,
            pod_log_stream_max_pods: None,
            metrics: true,
            fleet_metrics: false,
            metrics_refresh_secs: 15,
            dashboard_policy: None,
            metrics_time_ranges: None,
        };
        let value = serde_json::to_value(response).expect("config response serializes");
        assert_eq!(value.get("metricsRefreshSecs"), Some(&Value::from(15)));
        assert_eq!(value.get("fleetMetrics"), Some(&Value::Bool(false)));
    }

    #[test]
    fn component_views_split_revisions_and_surface_canary() {
        let status: InferenceServiceStatus = serde_json::from_value(serde_json::json!({
            "components": {
                "engine": {
                    "rolloutPhase": "Canarying",
                    "latestReadyRevision": "svc-engine-rev-new2",
                    "lifecycle": {
                        "replicas": 5,
                        "readyReplicas": 5,
                        "currentRevision": "svc-engine-old1",
                        "updateRevision": "svc-engine-new2",
                        "updatedReplicas": 2,
                        "updatedReadyReplicas": 1
                    }
                }
            },
            "canary": {
                "currentStep": 2,
                "observedTrafficWeight": 25,
                "analysisFailedChecks": 0
            }
        }))
        .expect("status fixture deserializes");

        let comps = components_of(&status);
        assert_eq!(comps.len(), 1);
        let engine = to_val(&comps[0]);
        assert_eq!(
            engine.get("currentRevision"),
            Some(&Value::from("svc-engine-old1"))
        );
        assert_eq!(
            engine.get("updateRevision"),
            Some(&Value::from("svc-engine-new2"))
        );
        assert_eq!(engine.get("updatedReplicas"), Some(&Value::from(2)));
        assert_eq!(engine.get("updatedReadyReplicas"), Some(&Value::from(1)));

        let canary = CanaryView::of(status.canary.as_ref().expect("canary present"));
        assert_eq!(canary.current_step, 2);
        assert_eq!(canary.traffic_weight, 25);
        // Zero failing checks stay off the wire — only a live failure is signal.
        assert!(canary.failed_checks.is_none());

        let failing: InferenceServiceStatusCanary = serde_json::from_value(serde_json::json!({
            "currentStep": 1,
            "observedTrafficWeight": 5,
            "analysisFailedChecks": 2
        }))
        .expect("canary fixture deserializes");
        assert_eq!(CanaryView::of(&failing).failed_checks, Some(2));
    }

    #[test]
    fn canary_view_carries_step_age_and_failing_checks() {
        let canary: InferenceServiceStatusCanary = serde_json::from_value(serde_json::json!({
            "currentStep": 3,
            "observedTrafficWeight": 25,
            "stepEnteredTime": "2020-01-01T00:00:00Z",
            "metricResults": [
                { "name": "error_rate", "passed": false, "operator": "<=",
                  "threshold": "0.5", "value": "2.1", "message": "over budget" },
                { "name": "latency_p99", "passed": true, "threshold": "800ms" }
            ]
        }))
        .expect("canary fixture deserializes");
        let view = CanaryView::of(&canary);
        // Entered years ago → a day-granularity age, never a raw timestamp.
        assert!(view.step_age.as_deref().is_some_and(|a| a.ends_with('d')));
        assert_eq!(view.failing_checks.len(), 1);
        let check = to_val(&view.failing_checks[0]);
        assert_eq!(check.get("name"), Some(&Value::from("error_rate")));
        assert_eq!(check.get("threshold"), Some(&Value::from("<= 0.5")));
        assert_eq!(check.get("value"), Some(&Value::from("2.1")));
        assert_eq!(check.get("message"), Some(&Value::from("over budget")));
    }

    #[test]
    fn rollout_attention_uses_typed_ome_phases() {
        use InferenceServiceStatusComponentsRolloutPhase as ComponentPhase;
        use InferenceServiceStatusRolloutCoordinationGroupsPhase as CoordinationPhase;

        let canary =
            component_rollout_attention(&ComponentPhase::Canarying).expect("Canarying is active");
        assert_eq!(canary.state, RolloutState::Active);
        assert_eq!(canary.phase, "Canarying");

        let rolled_back = component_rollout_attention(&ComponentPhase::RolledBack)
            .expect("RolledBack is terminal");
        assert_eq!(rolled_back.state, RolloutState::Terminal);
        assert_eq!(rolled_back.phase, "RolledBack");

        assert!(component_rollout_attention(&ComponentPhase::Stable).is_none());
        assert!(coordination_rollout_attention(&CoordinationPhase::Idle).is_none());
    }

    #[test]
    fn ready_rolled_back_service_is_not_hidden() {
        let isvc: InferenceService = serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "chat", "namespace": "team-a" },
            "spec": { "rollout": { "groups": [{
                "components": ["engine"],
                "canary": { "steps": [{ "capacity": "25%", "traffic": 25 }] }
            }] } },
            "status": {
                "conditions": [{
                    "type": "Ready", "status": "True", "reason": "Ready", "message": "",
                    "lastTransitionTime": "2026-08-27T21:00:00Z"
                }],
                "components": { "engine": { "rolloutPhase": "RolledBack" } },
                "canary": {
                    "currentStep": 0,
                    "observedTrafficWeight": 0,
                    "canaryRevisionHash": "candidate",
                    "rolledBackRevisionHash": "candidate",
                    "analysisFailedChecks": 2,
                    "metricResults": [{ "name": "success-rate", "passed": false }]
                }
            }
        }))
        .expect("InferenceService JSON is valid");
        let rollout = RolloutView::noteworthy(&isvc).expect("rollback is noteworthy");
        assert!(rollout.ready);
        assert_eq!(rollout.phase, "RolledBack");
        assert_eq!(rollout.state, RolloutState::Terminal);
        assert_eq!(rollout.strategy.as_deref(), Some("Canary"));
        assert_eq!(
            rollout.canary.map(|c| c.analysis_failed_checks),
            Some(Some(2))
        );
    }

    #[test]
    fn not_ready_without_rollout_phase_is_health_only() {
        let isvc: InferenceService = serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "chat", "namespace": "team-a" },
            "spec": {},
            "status": { "conditions": [{
                "type": "Ready", "status": "False",
                "reason": "InsufficientAvailable", "message": "waiting for capacity",
                "lastTransitionTime": "2026-08-27T21:00:00Z"
            }] }
        }))
        .expect("InferenceService JSON is valid");
        assert!(RolloutView::noteworthy(&isvc).is_none());
    }

    #[test]
    fn stale_generation_is_reconciling_without_an_ome_rollout_phase() {
        let stale: InferenceService = serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "chat", "namespace": "team-a", "generation": 4 },
            "spec": {},
            "status": { "observedGeneration": 3 }
        }))
        .expect("InferenceService JSON is valid");
        let rollout = RolloutView::noteworthy(&stale).expect("stale generation is noteworthy");
        assert_eq!(rollout.phase, "Reconciling");
        assert_eq!(rollout.state, RolloutState::Active);

        let current: InferenceService = serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "chat", "namespace": "team-a", "generation": 4 },
            "spec": {},
            "status": { "observedGeneration": 4 }
        }))
        .expect("InferenceService JSON is valid");
        assert!(RolloutView::noteworthy(&current).is_none());

        let unreported: InferenceService = serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "chat", "namespace": "team-a", "generation": 4 },
            "spec": {},
            "status": {}
        }))
        .expect("InferenceService JSON is valid");
        assert!(RolloutView::noteworthy(&unreported).is_none());
    }
}
