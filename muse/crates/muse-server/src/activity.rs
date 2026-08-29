//! Change narration for the activity feed.
//!
//! Each fed kind projects the fields whose changes deserve a feed entry into
//! an [`ActivityFacts`] fingerprint; [`change_note`] phrases the difference
//! between two fingerprints ("engine scaled 4 → 8", "became ready"). No
//! difference means the event was untracked status churn and stays off the
//! feed entirely.

use kube::Resource;
use ome_client::{
    k8s_openapi::api::core::v1::{Node, Pod},
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};

use crate::protocol::enum_str;

/// Longest note we ship; the feed is a headline, not a diff.
const MAX_NOTE_CHARS: usize = 160;

/// Compact fingerprint of one object's feed-worthy fields. Fields a kind does
/// not have simply stay `None`/empty and never produce a note.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ActivityFacts {
    pub generation: Option<i64>,
    pub ready: Option<bool>,
    pub not_ready_reason: Option<String>,
    /// Model download/import lifecycle state (BaseModel kinds).
    pub state: Option<String>,
    pub canary_step: Option<i32>,
    pub canary_traffic: Option<i32>,
    pub components: Vec<ComponentFacts>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComponentFacts {
    pub name: String,
    pub phase: Option<String>,
    pub replicas: Option<i32>,
    pub update_revision: Option<String>,
    pub rolledout_revision: Option<String>,
}

/// Projection of an object into its feed-worthy fingerprint.
pub trait Activity {
    fn facts(&self) -> ActivityFacts;
}

/// Kinds where only spec edits are feed-worthy (or that never reach the feed
/// but share the informer plumbing).
macro_rules! generation_only {
    ($($kind:ty),+ $(,)?) => {
        $(impl Activity for $kind {
            fn facts(&self) -> ActivityFacts {
                ActivityFacts {
                    generation: self.meta().generation,
                    ..ActivityFacts::default()
                }
            }
        })+
    };
}
generation_only!(
    ServingRuntime,
    ClusterServingRuntime,
    FineTunedWeight,
    WorkloadCluster,
    InferenceReplica,
    Node,
    Pod,
);

impl Activity for InferenceService {
    fn facts(&self) -> ActivityFacts {
        let status = self.status.as_ref();
        let ready = status
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"));
        let components = status
            .and_then(|s| s.components.as_ref())
            .map(|comps| {
                comps
                    .iter()
                    .map(|(name, c)| {
                        let lifecycle = c.lifecycle.as_ref();
                        ComponentFacts {
                            name: name.clone(),
                            phase: c.rollout_phase.as_ref().and_then(enum_str),
                            replicas: lifecycle.and_then(|l| l.replicas),
                            update_revision: lifecycle
                                .and_then(|l| l.update_revision.clone())
                                .filter(|s| !s.is_empty()),
                            rolledout_revision: c
                                .latest_rolledout_revision
                                .clone()
                                .filter(|s| !s.is_empty()),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let canary = status.and_then(|s| s.canary.as_ref());
        ActivityFacts {
            generation: self.meta().generation,
            ready: ready.map(|c| c.status == "True"),
            not_ready_reason: ready
                .filter(|c| c.status != "True")
                .map(|c| c.reason.clone())
                .filter(|s| !s.is_empty()),
            state: None,
            canary_step: canary.map(|c| c.current_step),
            canary_traffic: canary.map(|c| c.observed_traffic_weight),
            components,
        }
    }
}

impl Activity for BaseModel {
    fn facts(&self) -> ActivityFacts {
        ActivityFacts {
            generation: self.meta().generation,
            state: self.status.as_ref().and_then(|s| enum_str(&s.state)),
            ..ActivityFacts::default()
        }
    }
}

impl Activity for ClusterBaseModel {
    fn facts(&self) -> ActivityFacts {
        ActivityFacts {
            generation: self.meta().generation,
            state: self.status.as_ref().and_then(|s| enum_str(&s.state)),
            ..ActivityFacts::default()
        }
    }
}

/// The distinguishing tail of a revision name: the `rev-<hash>` segment when
/// present, else the part after the last dash (the revision hash by the
/// controller's naming contract). Full names are noise at feed size.
fn short_revision(revision: &str) -> &str {
    if let Some(i) = revision.find("rev-") {
        return &revision[i..];
    }
    match revision.rfind('-') {
        Some(i) if i > 0 => &revision[i + 1..],
        _ => revision,
    }
}

/// Phrase the difference between two fingerprints, or `None` when nothing
/// tracked changed (untracked status churn — not feed-worthy).
pub fn change_note(old: &ActivityFacts, new: &ActivityFacts) -> Option<String> {
    let mut notes: Vec<String> = Vec::new();
    if new.generation != old.generation {
        notes.push("spec updated".to_owned());
    }
    match (old.ready, new.ready) {
        (Some(false), Some(true)) | (None, Some(true)) => notes.push("became ready".to_owned()),
        (Some(true), Some(false)) | (None, Some(false)) => {
            let was = if old.ready.is_some() {
                "no longer ready"
            } else {
                "not ready"
            };
            notes.push(match &new.not_ready_reason {
                Some(reason) => format!("{was} — {reason}"),
                None => was.to_owned(),
            });
        }
        _ => {}
    }
    if let (Some(a), Some(b)) = (&old.state, &new.state) {
        if a != b {
            notes.push(format!("{a} → {b}"));
        }
    } else if let (None, Some(b)) = (&old.state, &new.state) {
        notes.push(format!("→ {b}"));
    }
    match (old.canary_step, new.canary_step) {
        (Some(a), Some(b)) if a != b => {
            let traffic = new
                .canary_traffic
                .map(|t| format!(" ({t}% traffic)"))
                .unwrap_or_default();
            notes.push(format!("canary step {a} → {b}{traffic}"));
        }
        (Some(a), Some(_)) => {
            // Same step; a traffic move alone is still a canary action.
            if let (Some(t0), Some(t1)) = (old.canary_traffic, new.canary_traffic) {
                if t0 != t1 {
                    notes.push(format!("canary traffic {t0}% → {t1}% (step {a})"));
                }
            }
        }
        (None, Some(b)) => notes.push(format!("canary started (step {b})")),
        _ => {}
    }
    for nc in &new.components {
        let Some(oc) = old.components.iter().find(|c| c.name == nc.name) else {
            continue; // a component appearing is initial wiring, not a change story
        };
        if oc.phase != nc.phase {
            match (&oc.phase, &nc.phase) {
                (Some(a), Some(b)) => notes.push(format!("{}: {a} → {b}", nc.name)),
                (None, Some(b)) => notes.push(format!("{}: {b}", nc.name)),
                _ => {}
            }
        }
        if let (Some(a), Some(b)) = (oc.replicas, nc.replicas) {
            if a != b {
                notes.push(format!("{} scaled {a} → {b}", nc.name));
            }
        }
        if oc.update_revision != nc.update_revision {
            if let Some(rev) = &nc.update_revision {
                notes.push(format!("{} rolling to {}", nc.name, short_revision(rev)));
            }
        }
        if oc.rolledout_revision != nc.rolledout_revision {
            if let Some(rev) = &nc.rolledout_revision {
                notes.push(format!("{} rolled out {}", nc.name, short_revision(rev)));
            }
        }
    }
    if notes.is_empty() {
        return None;
    }
    let mut note = notes.join("; ");
    if note.chars().count() > MAX_NOTE_CHARS {
        note = note.chars().take(MAX_NOTE_CHARS - 1).collect::<String>() + "…";
    }
    Some(note)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn isvc(status: serde_json::Value, generation: i64) -> InferenceService {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "svc", "namespace": "ns", "generation": generation },
            "spec": {},
            "status": status,
        }))
        .expect("fixture deserializes")
    }

    #[test]
    fn untracked_status_churn_is_silent() {
        // Same generation, same tracked fields, different untracked noise
        // (condition timestamps, observedGeneration) → no note.
        let a = isvc(
            serde_json::json!({ "observedGeneration": 3, "components": {
                "engine": { "rolloutPhase": "Stable", "lifecycle": { "replicas": 4 } }
            }}),
            3,
        );
        let b = isvc(
            serde_json::json!({ "observedGeneration": 4, "components": {
                "engine": { "rolloutPhase": "Stable", "lifecycle": { "replicas": 4, "readyReplicas": 3 } }
            }}),
            3,
        );
        assert_eq!(change_note(&a.facts(), &b.facts()), None);
    }

    #[test]
    fn spec_scale_and_rollout_transitions_are_phrased() {
        let old = isvc(
            serde_json::json!({
                "conditions": [{ "type": "Ready", "status": "True", "reason": "Ready",
                                 "message": "", "lastTransitionTime": "2026-08-27T21:00:00Z" }],
                "components": { "engine": {
                    "rolloutPhase": "Stable",
                    "latestRolledoutRevision": "svc-engine-rev-old1",
                    "lifecycle": { "replicas": 4, "updateRevision": "svc-engine-old1" }
                }}
            }),
            3,
        );
        let new = isvc(
            serde_json::json!({
                "conditions": [{ "type": "Ready", "status": "False", "reason": "EngineNotReady",
                                 "message": "", "lastTransitionTime": "2026-08-27T22:00:00Z" }],
                "components": { "engine": {
                    "rolloutPhase": "Canarying",
                    "latestRolledoutRevision": "svc-engine-rev-old1",
                    "lifecycle": { "replicas": 8, "updateRevision": "svc-engine-new2" }
                }}
            }),
            4,
        );
        let note = change_note(&old.facts(), &new.facts()).expect("changes are narrated");
        assert_eq!(
            note,
            "spec updated; no longer ready — EngineNotReady; \
             engine: Stable → Canarying; engine scaled 4 → 8; engine rolling to new2"
        );
    }

    #[test]
    fn rollout_completion_and_canary_progress_are_narrated() {
        let old = isvc(
            serde_json::json!({
                "canary": { "currentStep": 1, "observedTrafficWeight": 5 },
                "components": { "engine": {
                    "latestRolledoutRevision": "svc-engine-rev-old1",
                    "lifecycle": { "replicas": 4 }
                }}
            }),
            4,
        );
        let new = isvc(
            serde_json::json!({
                "canary": { "currentStep": 2, "observedTrafficWeight": 25 },
                "components": { "engine": {
                    "latestRolledoutRevision": "svc-engine-rev-new2",
                    "lifecycle": { "replicas": 4 }
                }}
            }),
            4,
        );
        let note = change_note(&old.facts(), &new.facts()).expect("changes are narrated");
        assert_eq!(
            note,
            "canary step 1 → 2 (25% traffic); engine rolled out rev-new2"
        );
    }

    #[test]
    fn ready_flip_and_model_state_read_naturally() {
        let mut old = ActivityFacts::default();
        let mut new = ActivityFacts::default();
        old.ready = Some(false);
        new.ready = Some(true);
        assert_eq!(change_note(&old, &new).as_deref(), Some("became ready"));

        let old = ActivityFacts {
            state: Some("Importing".to_owned()),
            ..ActivityFacts::default()
        };
        let new = ActivityFacts {
            state: Some("Ready".to_owned()),
            ..ActivityFacts::default()
        };
        assert_eq!(
            change_note(&old, &new).as_deref(),
            Some("Importing → Ready")
        );
    }

    #[test]
    fn long_notes_are_capped() {
        let old = ActivityFacts::default();
        let new = ActivityFacts {
            components: (0..30)
                .map(|i| ComponentFacts {
                    name: format!("component-{i}"),
                    phase: Some("Failed".to_owned()),
                    replicas: None,
                    update_revision: None,
                    rolledout_revision: None,
                })
                .collect(),
            ..ActivityFacts::default()
        };
        // Components only note when previously seen; seed the old side.
        let old = ActivityFacts {
            components: new
                .components
                .iter()
                .map(|c| ComponentFacts {
                    phase: Some("Stable".to_owned()),
                    ..c.clone()
                })
                .collect(),
            ..old
        };
        let note = change_note(&old, &new).expect("changes are narrated");
        assert!(note.chars().count() <= MAX_NOTE_CHARS);
        assert!(note.ends_with('…'));
    }
}
