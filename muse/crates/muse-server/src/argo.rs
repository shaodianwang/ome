//! Read-only Argo CD Application cache for delivery-state joins.

use futures_util::StreamExt;
use kube::{
    core::{ApiResource, DynamicObject, GroupVersionKind},
    runtime::{reflector, watcher, WatchStreamExt},
    Api, Client, Resource,
};
use serde_json::Value;

use crate::{informers::Invalidations, protocol::DeliveryView};

const TRACKING_ANNOTATION: &str = "argocd.argoproj.io/tracking-id";

#[derive(Clone)]
pub struct ArgoState {
    applications: reflector::Store<DynamicObject>,
}

impl ArgoState {
    pub async fn start(namespace: &str, invalidations: Invalidations) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk("argoproj.io", "v1alpha1", "Application"),
            "applications",
        );
        let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &resource);
        let writer = reflector::store::Writer::new(resource);
        let applications = writer.as_reader();
        let stream = reflector(writer, watcher(api, watcher::Config::default())).default_backoff();
        tokio::spawn(async move {
            stream
                .for_each(|event| async {
                    match event {
                        Ok(watcher::Event::Apply(_)
                        | watcher::Event::Delete(_)
                        | watcher::Event::InitDone) => {
                            invalidations.mark("ArgoApplication");
                        }
                        Ok(watcher::Event::Init | watcher::Event::InitApply(_)) => {}
                        Err(error) => {
                            tracing::warn!(%error, "Argo CD Application watch failed; delivery state may be stale");
                        }
                    }
                })
                .await;
        });
        Ok(Self { applications })
    }

    pub fn delivery_for<K: Resource>(
        &self,
        object: &K,
        group: &str,
        kind: &str,
    ) -> Option<DeliveryView> {
        let application = tracking_application(object.meta())?;
        let app = self
            .applications
            .state()
            .into_iter()
            .find(|candidate| candidate.metadata.name.as_deref() == Some(application.as_str()));
        Some(match app {
            Some(app) => delivery_from_application(&app, &application, object.meta(), group, kind),
            None => DeliveryView {
                application,
                sync_status: "Unknown".to_owned(),
                health_status: None,
                desired_revision: None,
                operation_phase: None,
                operation_message: None,
                operation_started_at: None,
                operation_finished_at: None,
                resource_sync_status: None,
                resource_health_status: None,
                resource_health_message: None,
                requires_pruning: None,
                reconciled_at: None,
            },
        })
    }
}

fn tracking_application(
    meta: &ome_client::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
) -> Option<String> {
    meta.annotations
        .as_ref()?
        .get(TRACKING_ANNOTATION)?
        .split_once(':')
        .map(|(application, _)| application.trim().to_owned())
        .filter(|application| !application.is_empty())
}

fn delivery_from_application(
    app: &DynamicObject,
    application: &str,
    object: &ome_client::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    group: &str,
    kind: &str,
) -> DeliveryView {
    let status = app.data.get("status").unwrap_or(&Value::Null);
    let resource = status
        .get("resources")
        .and_then(Value::as_array)
        .and_then(|resources| {
            resources.iter().find(|resource| {
                string_at(resource, "group").unwrap_or_default() == group
                    && string_at(resource, "kind") == Some(kind)
                    && string_at(resource, "name") == object.name.as_deref()
                    && string_at(resource, "namespace").unwrap_or_default()
                        == object.namespace.as_deref().unwrap_or_default()
            })
        });
    DeliveryView {
        application: application.to_owned(),
        sync_status: string_path(status, &["sync", "status"])
            .unwrap_or_else(|| "Unknown".to_owned()),
        health_status: string_path(status, &["health", "status"]),
        desired_revision: string_path(status, &["sync", "revision"]),
        operation_phase: string_path(status, &["operationState", "phase"]),
        operation_message: string_path(status, &["operationState", "message"]),
        operation_started_at: string_path(status, &["operationState", "startedAt"]),
        operation_finished_at: string_path(status, &["operationState", "finishedAt"]),
        resource_sync_status: resource.and_then(|r| string_at(r, "status").map(str::to_owned)),
        resource_health_status: resource.and_then(|r| string_path(r, &["health", "status"])),
        resource_health_message: resource.and_then(|r| string_path(r, &["health", "message"])),
        requires_pruning: resource
            .and_then(|r| r.get("requiresPruning"))
            .and_then(Value::as_bool),
        reconciled_at: string_at(status, "reconciledAt").map(str::to_owned),
    }
}

fn string_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current
        .as_str()
        .map(str::to_owned)
        .filter(|value| !value.is_empty())
}

fn string_at<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field)?.as_str().filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projects_application_and_exact_resource_state() {
        let app: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Application",
            "metadata": { "name": "isvc-member-a", "namespace": "argocd" },
            "status": {
                "sync": { "status": "OutOfSync", "revision": "abcdef" },
                "health": { "status": "Healthy" },
                "operationState": { "phase": "Succeeded", "message": "applied" },
                "resources": [{
                    "group": "ome.io", "kind": "InferenceService",
                    "namespace": "team-a", "name": "chat",
                    "status": "OutOfSync", "health": { "status": "Healthy" }
                }]
            }
        }))
        .expect("Application JSON is valid");
        let metadata = serde_json::from_value(serde_json::json!({
            "name": "chat",
            "namespace": "team-a",
            "annotations": { "argocd.argoproj.io/tracking-id": "isvc-member-a:ome.io/InferenceService:team-a/chat" }
        }))
        .expect("metadata JSON is valid");
        let delivery = delivery_from_application(
            &app,
            "isvc-member-a",
            &metadata,
            "ome.io",
            "InferenceService",
        );
        assert_eq!(delivery.sync_status, "OutOfSync");
        assert_eq!(delivery.resource_sync_status.as_deref(), Some("OutOfSync"));
        assert_eq!(delivery.desired_revision.as_deref(), Some("abcdef"));
    }
}
