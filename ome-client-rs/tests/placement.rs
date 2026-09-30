use ome_client::inferencereplicas::InferenceReplicaPlacementExecution;
use ome_client::inferenceservices::{InferenceServiceSpec, InferenceServiceStatus};
use ome_client::TrafficMap;
use serde_json::Value;

fn cases(group: &str) -> Vec<Value> {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/placement.json")).unwrap();
    fixtures[group].as_array().unwrap().clone()
}

#[test]
fn placement_intent_roundtrips() {
    for case in cases("intents") {
        let want = case["spec"].clone();
        let spec: InferenceServiceSpec = serde_json::from_value(want.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(spec).unwrap(),
            want,
            "{}",
            case["name"]
        );
    }
}

#[test]
fn placement_status_roundtrips() {
    for case in cases("statuses") {
        let want = case["status"].clone();
        let status: InferenceServiceStatus = serde_json::from_value(want.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            want,
            "{}",
            case["name"]
        );
    }
}

#[test]
fn member_execution_roundtrips() {
    for case in cases("memberPolicies") {
        let want = case["policy"].clone();
        let policy: InferenceReplicaPlacementExecution =
            serde_json::from_value(want.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(policy).unwrap(),
            want,
            "{}",
            case["name"]
        );
    }
}

#[test]
fn placement_routing_roundtrips() {
    for case in cases("trafficMaps") {
        let want = case["object"].clone();
        let map: TrafficMap = serde_json::from_value(want.clone()).unwrap();
        assert_eq!(serde_json::to_value(map).unwrap(), want, "{}", case["name"]);
    }
}
