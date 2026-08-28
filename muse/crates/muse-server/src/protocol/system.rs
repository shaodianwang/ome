//! System components (OEP-0021.2): an admin view of OME's own control plane.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ConditionView;

/// Which apps/v1 kind a system-component descriptor targets. The variant names
/// are the wire values (`"Deployment"` / `"DaemonSet"`), matching the Kubernetes
/// kind strings so descriptors read naturally.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SystemComponentKind {
    Deployment,
    DaemonSet,
}

impl SystemComponentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deployment => "Deployment",
            Self::DaemonSet => "DaemonSet",
        }
    }
}

/// One system component to watch, supplied entirely by deployment config — there
/// is no baked-in component set, because the namespaces and label selectors that
/// identify "the controller" or "the scheduler" are install- and provider-specific
/// (repo hard blocker: no hardcoded magic values). Absent config ⇒ the System
/// panel is empty, not wrong.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SystemComponent {
    /// Display name, e.g. "Controller manager".
    pub name: String,
    /// Optional grouping for the panel, e.g. "control-plane".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: SystemComponentKind,
    pub namespace: String,
    /// Match the workload by its exact object name. Preferred for control-plane
    /// singletons, and required when the object carries no metadata labels of its
    /// own (e.g. the OME model-agent DaemonSet labels only its pod template). One
    /// of `resource_name` or `selector` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    /// Match the workload by its own metadata labels (all entries must be present).
    /// Used when `resource_name` is absent.
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
    /// Clusters this component is expected on. Empty ⇒ every aggregated cluster.
    /// The expected scope is what makes `missing` meaningful: a component absent
    /// from a cluster it should run on is a real signal, not silence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<String>,
}

impl SystemComponent {
    /// Whether this component is expected on the named cluster.
    pub fn applies_to(&self, cluster: &str) -> bool {
        self.clusters.is_empty() || self.clusters.iter().any(|c| c == cluster)
    }
}

/// The configured component set, parsed from one JSON array. A newtype (not a bare
/// `Vec`) so clap treats `--system-components-json` as a single JSON value rather
/// than a repeatable element flag.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SystemComponents(pub Vec<SystemComponent>);

/// Derived health of a system component, ordered best → worst so that
/// `Iterator::max` implements the "worst-wins" aggregation rule: an operator
/// scanning a roll-up must never see `healthy` while one instance is down.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ComponentHealth {
    /// `ready == desired` and `desired > 0`.
    Healthy,
    /// `desired == 0` (scaled to zero / no eligible nodes) or an uninterpretable
    /// status — honesty over a fabricated verdict.
    Unknown,
    /// `0 < ready < desired` — partially up (often a rollout in flight).
    Degraded,
    /// `desired > 0` and `ready == 0` — exists but nothing is serving.
    Unavailable,
    /// Expected on this cluster but no object matched — not deployed (or a stale
    /// descriptor). The strongest actionable signal.
    Missing,
}

impl ComponentHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Unknown => "unknown",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
            Self::Missing => "missing",
        }
    }
}

/// One pod backing a system component: name, phase, and node — a liveness
/// glance, not the full diagnosis view (that's the deployment pods endpoint).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemPodView {
    pub name: String,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

/// One row of `GET /v1/system`: a component's health on one cluster, plus the
/// detail the console drawer renders (matched object names, workload conditions,
/// and backing pods). The owning `cluster` is stamped on by [`super::tagged`].
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemComponentRow {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: &'static str,
    pub namespace: String,
    pub status: &'static str,
    pub desired: i32,
    pub ready: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The matched workload object name(s) on this cluster. Empty ⇒ `missing`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pods: Vec<SystemPodView>,
}
