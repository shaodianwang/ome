//! Capacity rollup — the cross-resource analytics behind `/v1/capacity` that the
//! frontend can't do itself (it requires joining Nodes × Pods, and fanning out
//! across clusters). Domain logic, not an endpoint: it's run by the background
//! refresh task on informer changes and cached on `AppState`; the endpoint only
//! serves the snapshot.
//!
//! "Allocatable" comes from Ready, uncordoned Nodes; "allocated" is summed from the
//! accelerator requests of the Pods scheduled on those Nodes. NotReady or cordoned
//! Nodes remain visible as unavailable physical inventory, but contribute no
//! schedulable/free capacity. Accelerators are tracked **per kind** — GPUs and TPUs
//! never share a total (different units). Results roll up fleet-wide per kind and
//! are grouped by cluster, island, NVLink/ICI domain, and instance type; every group
//! carries its accelerator `kind`.

use std::{collections::BTreeMap, sync::Arc};

use kube::Resource;
use ome_client::k8s_openapi::api::core::v1::{Node, Pod};

use crate::protocol::{
    first_label, node_accel, sum_accel, AccelDemand, AccelLost, CapacityReport, Consumer,
    DeploymentConsumer, Fragment, GpuView, GroupCapacity, NodeCounts, PlacementUnits, Topology,
    UnitBox, INFERENCE_SERVICE_POD_LABEL,
};

/// Accelerator kinds tracked separately (a pod/node never mixes them in practice).
const KINDS: [&str; 2] = ["gpu", "tpu"];

/// GKE TPU labels used to select the partition ID for a node's provisioned slice.
const TPU_TOPOLOGY_LABEL: &str = "cloud.google.com/gke-tpu-topology";
const TPU_PARTITION_PREFIX: &str = "cloud.google.com/gke-tpu-partition-";
const TPU_PARTITION_SUFFIX: &str = "-id";

/// One cluster's cached Nodes + Pods, the input to aggregation.
pub struct ClusterData {
    pub name: String,
    pub nodes: Vec<Arc<Node>>,
    pub pods: Vec<Arc<Pod>>,
}

/// User-visible response cardinality limits supplied by deployment config.
#[derive(Clone, Copy, Debug)]
pub struct CapacityLimits {
    pub max_fragments: usize,
    pub max_consumers: usize,
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
pub fn build(clusters: &[ClusterData], topo: &Topology, limits: CapacityLimits) -> CapacityReport {
    let mut counts = NodeCounts::default();
    let mut kinds: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut by_cluster: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut islands: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut nvlink: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    let mut instance: BTreeMap<Key, (usize, GpuView)> = BTreeMap::new();
    // Demand: Pending accelerator pods not yet placed (kind -> accel, pods).
    let mut pending: BTreeMap<&'static str, (i64, i64)> = BTreeMap::new();
    // Lost capacity: accelerators on NotReady/cordoned nodes (kind -> accel, nodes).
    let mut unavail: BTreeMap<&'static str, (i64, usize)> = BTreeMap::new();
    // Top consumers: chips held by scheduled pods, per (namespace, kind) and —
    // for pods carrying the ownership label — per (namespace, InferenceService, kind).
    let mut consumers: BTreeMap<(String, &'static str), (i64, i64)> = BTreeMap::new();
    let mut deployment_consumers: BTreeMap<DeploymentKey, (i64, i64)> = BTreeMap::new();

    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);
        accumulate_pending(&cluster.pods, &mut pending);
        accumulate_consumers(
            &cluster.name,
            &cluster.pods,
            &mut consumers,
            &mut deployment_consumers,
        );

        for node in &cluster.nodes {
            let name = node.meta().name.clone().unwrap_or_default();
            let status = node.status.as_ref();
            let labels = node.meta().labels.clone().unwrap_or_default();

            counts.total += 1;
            let ready = node_ready(node);
            if ready {
                counts.ready += 1;
            }
            let available = node_available(node);
            if available {
                counts.schedulable += 1;
            }

            let Some((kind, allocatable)) = status
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue; // non-accelerator node: counted in totals, skipped in rollups
            };
            counts.accelerator_nodes += 1;
            // Keep unavailable physical inventory visible, but do not let it inflate
            // schedulable totals or the topology groups used for largest-fit results.
            if !available {
                let e = unavail.entry(kind).or_insert((0, 0));
                e.0 += allocatable;
                e.1 += 1;
                continue;
            }

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
        pending: pending
            .into_iter()
            .map(|(kind, (accelerators, pods))| AccelDemand {
                kind: kind.to_owned(),
                accelerators,
                pods,
            })
            .collect(),
        unavailable: unavail
            .into_iter()
            .map(|(kind, (accelerators, nodes))| AccelLost {
                kind: kind.to_owned(),
                accelerators,
                nodes: nodes as i64,
            })
            .collect(),
        // TPU: only the partition matching the provisioned topology is a slice.
        // GPU: the node itself is a unit
        // (bucketed by GPU count), and — where NVLink labels exist — so is the
        // whole domain (bucketed by domain GPU-count).
        tpu_slices: placement_units(
            clusters,
            "tpu",
            limits.max_fragments,
            |cluster, _name, labels| {
                let Some(shape) = labels.get(TPU_TOPOLOGY_LABEL) else {
                    return Vec::new();
                };
                let key = format!("{TPU_PARTITION_PREFIX}{shape}{TPU_PARTITION_SUFFIX}");
                labels
                    .get(&key)
                    .filter(|id| !id.is_empty())
                    .map(|id| {
                        vec![(
                            format!("{cluster}|{shape}|{id}"),
                            id.clone(),
                            Some(shape.clone()),
                        )]
                    })
                    .unwrap_or_default()
            },
        ),
        gpu_nodes: placement_units(
            clusters,
            "gpu",
            limits.max_fragments,
            |cluster, name, _labels| vec![(format!("{cluster}|{name}"), name.to_owned(), None)],
        ),
        // Individual domains (one box each in the console) grouped by the NVLink/ICI
        // label — GPU NVLink racks and TPU subblocks.
        gpu_racks: domain_units(clusters, "gpu", &topo.nvlink_labels),
        tpu_subblocks: domain_units(clusters, "tpu", &topo.nvlink_labels),
        consumers: top_consumers(consumers, limits.max_consumers),
        consumers_by_deployment: top_deployment_consumers(
            deployment_consumers,
            limits.max_consumers,
        ),
    }
}

/// One box per carve-able domain (a GPU NVLink rack or a TPU subblock): group
/// `want_kind` nodes by their `keys` label value, summing chips + free and AND-ing
/// health. Unlike [`placement_units`] this returns the domains individually (not
/// bucketed by size) for the console heatmap — most free first so available
/// capacity leads. A domain's varying chip count is just how many of its hosts are
/// currently present.
fn domain_units(clusters: &[ClusterData], want_kind: &str, keys: &[String]) -> Vec<UnitBox> {
    // cluster-scoped key -> (chips, free, healthy, cluster, id)
    let mut units: BTreeMap<String, (i64, i64, bool, String, String)> = BTreeMap::new();
    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);
        for node in &cluster.nodes {
            let Some((kind, chips)) = node
                .status
                .as_ref()
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue;
            };
            if kind != want_kind {
                continue;
            }
            let labels = node.meta().labels.clone().unwrap_or_default();
            let Some(dom) = first_label(&labels, keys) else {
                continue;
            };
            let name = node.meta().name.clone().unwrap_or_default();
            let used = allocated.get(&name).copied().unwrap_or(0);
            let available = node_available(node);
            let e = units
                .entry(format!("{}|{dom}", cluster.name))
                .or_insert_with(|| (0, 0, true, cluster.name.clone(), dom));
            e.0 += chips;
            if available {
                e.1 += (chips - used).max(0);
            }
            e.2 &= available;
        }
    }
    let mut out: Vec<UnitBox> = units
        .into_values()
        .map(|(chips, free, healthy, cluster, id)| UnitBox {
            id,
            cluster,
            chips,
            free,
            healthy,
        })
        .collect();
    out.sort_by(|a, b| b.free.cmp(&a.free).then(a.id.cmp(&b.id)));
    out
}

/// Whether a node's `Ready` condition is True.
fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// Whether a node is cordoned (`spec.unschedulable`).
fn node_unschedulable(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false)
}

/// Whether a node can contribute capacity to a new placement right now.
fn node_available(node: &Node) -> bool {
    node_ready(node) && !node_unschedulable(node)
}

/// Carve-able placement units of `want_kind`, bucketed by size, split by occupancy
/// (free / fragmented / full / unhealthy) with the chips stranded inside fragmented
/// units — the fragmentation a single "free" number hides.
///
/// `unit_of(cluster, node, labels)` returns units as `(unique key, display id,
/// optional size label)`. A `None` size label buckets by the unit's own chip count
/// (GPU nodes / domains, which have no fixed shape); TPU passes the slice shape.
/// The key is cluster-scoped so an id reused across clusters isn't merged.
fn placement_units(
    clusters: &[ClusterData],
    want_kind: &str,
    max_fragments: usize,
    unit_of: impl Fn(&str, &str, &BTreeMap<String, String>) -> Vec<(String, String, Option<String>)>,
) -> Vec<PlacementUnits> {
    // A carve-able unit's running tally across its member nodes.
    struct Acc {
        chips: i64,
        free: i64,
        healthy: bool,
        label: Option<String>,
        cluster: String,
        id: String,
    }
    let mut units: BTreeMap<String, Acc> = BTreeMap::new();
    for cluster in clusters {
        let allocated = accel_allocated_by_node(&cluster.pods);
        for node in &cluster.nodes {
            let Some((kind, chips)) = node
                .status
                .as_ref()
                .and_then(|s| s.allocatable.as_ref())
                .and_then(node_accel)
            else {
                continue;
            };
            if kind != want_kind {
                continue;
            }
            let name = node.meta().name.clone().unwrap_or_default();
            let labels = node.meta().labels.clone().unwrap_or_default();
            let used = allocated.get(&name).copied().unwrap_or(0);
            let available = node_available(node);
            for (key, id, label) in unit_of(&cluster.name, &name, &labels) {
                let e = units.entry(key).or_insert_with(|| Acc {
                    chips: 0,
                    free: 0,
                    healthy: true,
                    label,
                    cluster: cluster.name.clone(),
                    id,
                });
                e.chips += chips;
                if available {
                    e.free += (chips - used).max(0);
                }
                e.healthy &= available;
            }
        }
    }

    // One accumulator per size bucket, splitting its units by occupancy.
    struct Bucket {
        chips: i64,
        free: i64,
        fragmented: i64,
        full: i64,
        unhealthy: i64,
        stranded: i64,
        frags: Vec<Fragment>,
    }
    let mut by_size: BTreeMap<String, Bucket> = BTreeMap::new();
    for (_key, u) in units {
        let bucket = u.label.clone().unwrap_or_else(|| u.chips.to_string());
        let b = by_size.entry(bucket).or_insert_with(|| Bucket {
            chips: 0,
            free: 0,
            fragmented: 0,
            full: 0,
            unhealthy: 0,
            stranded: 0,
            frags: Vec::new(),
        });
        b.chips = b.chips.max(u.chips);
        if !u.healthy {
            b.unhealthy += 1;
        } else if u.free >= u.chips {
            b.free += 1;
        } else if u.free <= 0 {
            b.full += 1;
        } else {
            b.fragmented += 1;
            b.stranded += u.free;
            b.frags.push(Fragment {
                id: u.id,
                cluster: u.cluster,
                free: u.free,
                chips: u.chips,
            });
        }
    }

    let mut out: Vec<PlacementUnits> = by_size
        .into_iter()
        .map(|(label, mut b)| {
            b.frags.sort_by_key(|f| std::cmp::Reverse(f.free)); // most stranded first
            b.frags.truncate(max_fragments);
            PlacementUnits {
                label,
                chips: b.chips,
                total: b.free + b.fragmented + b.full + b.unhealthy,
                free: b.free,
                fragmented: b.fragmented,
                full: b.full,
                unhealthy: b.unhealthy,
                stranded_chips: b.stranded,
                fragments: b.frags,
            }
        })
        .collect();
    // Biggest units first — the ones fragmentation strands soonest.
    out.sort_by(|a, b| b.chips.cmp(&a.chips).then(a.label.cmp(&b.label)));
    out
}

/// Sum accelerator requests of Pending, not-yet-scheduled pods, per kind — demand
/// waiting on capacity. A Pending pod that already has a node (e.g. pulling an
/// image) isn't waiting on capacity, so it's excluded.
fn accumulate_pending(pods: &[Arc<Pod>], out: &mut BTreeMap<&'static str, (i64, i64)>) {
    for pod in pods {
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
        if phase != Some("Pending") {
            continue;
        }
        let Some(spec) = pod.spec.as_ref() else {
            continue;
        };
        if spec.node_name.is_some() {
            continue;
        }
        for kind in KINDS {
            let req: i64 = spec
                .containers
                .iter()
                .filter_map(|c| c.resources.as_ref())
                .filter_map(|r| r.requests.as_ref())
                .map(|r| sum_accel(r, kind))
                .sum();
            if req > 0 {
                let e = out.entry(kind).or_insert((0, 0));
                e.0 += req;
                e.1 += 1;
            }
        }
    }
}

/// Full identity of one deployment's consumption: (cluster, namespace, service, kind).
type DeploymentKey = (String, String, String, &'static str);

/// Sum accelerator chips held by *scheduled* pods — who is actually occupying the
/// cluster's accelerators — per (namespace, kind), and per owning InferenceService
/// for pods that carry the operator's ownership label. A Pending/unscheduled pod
/// isn't holding capacity yet, so it's excluded (that's [`accumulate_pending`]'s job).
fn accumulate_consumers(
    cluster: &str,
    pods: &[Arc<Pod>],
    out: &mut BTreeMap<(String, &'static str), (i64, i64)>,
    by_deployment: &mut BTreeMap<DeploymentKey, (i64, i64)>,
) {
    for pod in pods {
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
        if matches!(phase, Some("Succeeded" | "Failed")) {
            continue;
        }
        let Some(spec) = pod.spec.as_ref() else {
            continue;
        };
        if spec.node_name.is_none() {
            continue;
        }
        let ns = pod.metadata.namespace.clone().unwrap_or_default();
        let owner = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(INFERENCE_SERVICE_POD_LABEL))
            .filter(|o| !o.is_empty());
        for kind in KINDS {
            let req: i64 = spec
                .containers
                .iter()
                .filter_map(|c| c.resources.as_ref())
                .filter_map(|r| r.requests.as_ref())
                .map(|r| sum_accel(r, kind))
                .sum();
            if req > 0 {
                let e = out.entry((ns.clone(), kind)).or_insert((0, 0));
                e.0 += req;
                e.1 += 1;
                if let Some(owner) = owner {
                    let e = by_deployment
                        .entry((cluster.to_owned(), ns.clone(), owner.clone(), kind))
                        .or_insert((0, 0));
                    e.0 += req;
                    e.1 += 1;
                }
            }
        }
    }
}

/// Flatten the consumer map into the top-N namespaces by chips held.
fn top_consumers(
    map: BTreeMap<(String, &'static str), (i64, i64)>,
    max_consumers: usize,
) -> Vec<Consumer> {
    let mut out: Vec<Consumer> = map
        .into_iter()
        .map(|((namespace, kind), (accelerators, pods))| Consumer {
            namespace,
            kind: kind.to_owned(),
            accelerators,
            pods,
        })
        .collect();
    out.sort_by(|a, b| {
        b.accelerators
            .cmp(&a.accelerators)
            .then(a.namespace.cmp(&b.namespace))
    });
    out.truncate(max_consumers);
    out
}

/// Flatten the per-deployment consumer map into the top-N services by chips held.
fn top_deployment_consumers(
    map: BTreeMap<DeploymentKey, (i64, i64)>,
    max_consumers: usize,
) -> Vec<DeploymentConsumer> {
    let mut out: Vec<DeploymentConsumer> = map
        .into_iter()
        .map(
            |((cluster, namespace, name, kind), (accelerators, pods))| DeploymentConsumer {
                cluster,
                namespace,
                name,
                kind: kind.to_owned(),
                accelerators,
                pods,
            },
        )
        .collect();
    out.sort_by(|a, b| {
        b.accelerators
            .cmp(&a.accelerators)
            .then(a.namespace.cmp(&b.namespace))
            .then(a.name.cmp(&b.name))
    });
    out.truncate(max_consumers);
    out
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use ome_client::k8s_openapi::{
        api::core::v1::{
            Container, Node, NodeCondition, NodeSpec, NodeStatus, Pod, PodSpec, PodStatus,
            ResourceRequirements,
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
    };

    use super::{build, CapacityLimits, ClusterData, TPU_TOPOLOGY_LABEL};
    use crate::protocol::{
        AccelLost, CapacityReport, GroupCapacity, PlacementUnits, Topology, UnitBox,
        INFERENCE_SERVICE_POD_LABEL,
    };

    const ISLAND_LABEL: &str = "capacity.test/island";
    const DOMAIN_LABEL: &str = "capacity.test/domain";

    #[derive(Clone, Copy)]
    enum Availability {
        Healthy,
        NotReady,
        Cordoned,
    }

    struct Placement<'a> {
        island: &'a str,
        domain: &'a str,
        tpu_slice: Option<(&'a str, &'a str)>,
    }

    fn topology() -> Topology {
        Topology {
            island_labels: vec![ISLAND_LABEL.to_owned()],
            nvlink_labels: vec![DOMAIN_LABEL.to_owned()],
        }
    }

    fn limits() -> CapacityLimits {
        CapacityLimits {
            max_fragments: usize::MAX,
            max_consumers: usize::MAX,
        }
    }

    fn accel_resource(kind: &str) -> &'static str {
        match kind {
            "gpu" => "nvidia.com/gpu",
            "tpu" => "google.com/tpu",
            _ => panic!("unsupported test accelerator kind: {kind}"),
        }
    }

    fn node(
        name: &str,
        kind: &str,
        chips: i64,
        availability: Availability,
        placement: Placement<'_>,
    ) -> Arc<Node> {
        let (ready, cordoned) = match availability {
            Availability::Healthy => (true, false),
            Availability::NotReady => (false, false),
            Availability::Cordoned => (true, true),
        };
        let mut labels = BTreeMap::from([
            (ISLAND_LABEL.to_owned(), placement.island.to_owned()),
            (DOMAIN_LABEL.to_owned(), placement.domain.to_owned()),
            (
                "node.kubernetes.io/instance-type".to_owned(),
                format!("{kind}.test"),
            ),
        ]);
        if let Some((shape, id)) = placement.tpu_slice {
            labels.insert(TPU_TOPOLOGY_LABEL.to_owned(), shape.to_owned());
            labels.insert(
                format!("cloud.google.com/gke-tpu-partition-{shape}-id"),
                id.to_owned(),
            );
            if shape == "2x2x2" {
                labels.insert(
                    "cloud.google.com/gke-tpu-partition-2x2x1-id".to_owned(),
                    format!("{name}-2x2x1"),
                );
            }
        }
        let allocatable =
            BTreeMap::from([(accel_resource(kind).to_owned(), Quantity(chips.to_string()))]);
        Arc::new(Node {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                labels: Some(labels),
                ..ObjectMeta::default()
            },
            spec: Some(NodeSpec {
                unschedulable: Some(cordoned),
                ..NodeSpec::default()
            }),
            status: Some(NodeStatus {
                allocatable: Some(allocatable),
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".to_owned(),
                    status: if ready { "True" } else { "False" }.to_owned(),
                    ..NodeCondition::default()
                }]),
                ..NodeStatus::default()
            }),
        })
    }

    fn pod(name: &str, node_name: &str, kind: &str, chips: i64) -> Arc<Pod> {
        pod_in_namespace(name, node_name, kind, chips, "workloads")
    }

    fn pod_in_namespace(
        name: &str,
        node_name: &str,
        kind: &str,
        chips: i64,
        namespace: &str,
    ) -> Arc<Pod> {
        let requests =
            BTreeMap::from([(accel_resource(kind).to_owned(), Quantity(chips.to_string()))]);
        Arc::new(Pod {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                ..ObjectMeta::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node_name.to_owned()),
                containers: vec![Container {
                    name: "worker".to_owned(),
                    resources: Some(ResourceRequirements {
                        requests: Some(requests),
                        ..ResourceRequirements::default()
                    }),
                    ..Container::default()
                }],
                ..PodSpec::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".to_owned()),
                ..PodStatus::default()
            }),
        })
    }

    fn cluster(nodes: Vec<Arc<Node>>, pods: Vec<Arc<Pod>>) -> ClusterData {
        ClusterData {
            name: "cluster-a".to_owned(),
            nodes,
            pods,
        }
    }

    fn group<'a>(groups: &'a [GroupCapacity], name: &str, kind: &str) -> &'a GroupCapacity {
        groups
            .iter()
            .find(|g| g.name == name && g.kind == kind)
            .unwrap_or_else(|| panic!("missing capacity group {name}/{kind}"))
    }

    fn assert_group(
        groups: &[GroupCapacity],
        name: &str,
        kind: &str,
        nodes: usize,
        allocatable: i64,
        allocated: i64,
        free: i64,
    ) {
        let actual = group(groups, name, kind);
        assert_eq!(actual.nodes, nodes);
        assert_eq!(actual.allocatable, allocatable);
        assert_eq!(actual.allocated, allocated);
        assert_eq!(actual.free, free);
    }

    fn lost<'a>(report: &'a CapacityReport, kind: &str) -> &'a AccelLost {
        report
            .unavailable
            .iter()
            .find(|u| u.kind == kind)
            .unwrap_or_else(|| panic!("missing unavailable inventory for {kind}"))
    }

    fn placement<'a>(units: &'a [PlacementUnits], label: &str) -> &'a PlacementUnits {
        units
            .iter()
            .find(|u| u.label == label)
            .unwrap_or_else(|| panic!("missing placement bucket {label}"))
    }

    fn domain<'a>(units: &'a [UnitBox], id: &str) -> &'a UnitBox {
        units
            .iter()
            .find(|u| u.id == id)
            .unwrap_or_else(|| panic!("missing placement domain {id}"))
    }

    fn assert_inventory_only(report: &CapacityReport, ready_nodes: usize) {
        assert_eq!(report.nodes.total, 1);
        assert_eq!(report.nodes.ready, ready_nodes);
        assert_eq!(report.nodes.schedulable, 0);
        assert_eq!(report.nodes.accelerator_nodes, 1);
        assert!(report.accelerators.is_empty());
        assert!(report.by_cluster.is_empty());
        assert!(report.by_island.is_empty());
        assert!(report.by_nvlink_domain.is_empty());
        assert!(report.by_instance_type.is_empty());

        let unavailable = lost(report, "gpu");
        assert_eq!(unavailable.accelerators, 8);
        assert_eq!(unavailable.nodes, 1);

        let gpu_node = placement(&report.gpu_nodes, "8");
        assert_eq!(gpu_node.total, 1);
        assert_eq!(gpu_node.free, 0);
        assert_eq!(gpu_node.fragmented, 0);
        assert_eq!(gpu_node.full, 0);
        assert_eq!(gpu_node.unhealthy, 1);

        let rack = domain(&report.gpu_racks, "domain-a");
        assert_eq!(rack.chips, 8);
        assert_eq!(rack.free, 0);
        assert!(!rack.healthy);
    }

    #[test]
    fn healthy_gpu_contributes_to_schedulable_and_topology_capacity() {
        let report = build(
            &[cluster(
                vec![node(
                    "gpu-healthy",
                    "gpu",
                    8,
                    Availability::Healthy,
                    Placement {
                        island: "island-a",
                        domain: "domain-a",
                        tpu_slice: None,
                    },
                )],
                vec![],
            )],
            &topology(),
            limits(),
        );

        assert_eq!(report.nodes.total, 1);
        assert_eq!(report.nodes.ready, 1);
        assert_eq!(report.nodes.schedulable, 1);
        assert_eq!(report.nodes.accelerator_nodes, 1);
        assert_group(&report.accelerators, "gpu", "gpu", 1, 8, 0, 8);
        assert_group(&report.by_cluster, "cluster-a", "gpu", 1, 8, 0, 8);
        assert_group(&report.by_island, "island-a", "gpu", 1, 8, 0, 8);
        assert_group(&report.by_nvlink_domain, "domain-a", "gpu", 1, 8, 0, 8);
        assert_group(&report.by_instance_type, "gpu.test", "gpu", 1, 8, 0, 8);
        assert!(report.unavailable.is_empty());

        let gpu_node = placement(&report.gpu_nodes, "8");
        assert_eq!(gpu_node.total, 1);
        assert_eq!(gpu_node.free, 1);
        assert_eq!(gpu_node.unhealthy, 0);
        let rack = domain(&report.gpu_racks, "domain-a");
        assert_eq!(rack.chips, 8);
        assert_eq!(rack.free, 8);
        assert!(rack.healthy);
    }

    #[test]
    fn response_cardinality_uses_configured_limits() {
        let nodes = (0..4)
            .map(|index| {
                node(
                    &format!("gpu-{index}"),
                    "gpu",
                    8,
                    Availability::Healthy,
                    Placement {
                        island: "island-a",
                        domain: "domain-a",
                        tpu_slice: None,
                    },
                )
            })
            .collect();
        let pods = (0..4)
            .map(|index| {
                pod_in_namespace(
                    &format!("work-{index}"),
                    &format!("gpu-{index}"),
                    "gpu",
                    index + 1,
                    &format!("tenant-{index}"),
                )
            })
            .collect();
        let report = build(
            &[cluster(nodes, pods)],
            &topology(),
            CapacityLimits {
                max_fragments: 2,
                max_consumers: 2,
            },
        );

        let gpu_nodes = placement(&report.gpu_nodes, "8");
        assert_eq!(gpu_nodes.fragmented, 4);
        assert_eq!(gpu_nodes.fragments.len(), 2);
        assert_eq!(report.consumers.len(), 2);
        assert_eq!(report.consumers[0].namespace, "tenant-3");
        assert_eq!(report.consumers[1].namespace, "tenant-2");
    }

    /// A pod stamped with the operator's ownership label, as the InferenceService
    /// controller creates them.
    fn owned_pod(
        name: &str,
        node_name: &str,
        kind: &str,
        chips: i64,
        namespace: &str,
        isvc: &str,
    ) -> Arc<Pod> {
        let mut pod = (*pod_in_namespace(name, node_name, kind, chips, namespace)).clone();
        pod.metadata.labels = Some(BTreeMap::from([(
            INFERENCE_SERVICE_POD_LABEL.to_owned(),
            isvc.to_owned(),
        )]));
        Arc::new(pod)
    }

    #[test]
    fn consumers_attribute_labelled_pods_to_their_deployment() {
        let nodes = (0..3)
            .map(|index| {
                node(
                    &format!("gpu-{index}"),
                    "gpu",
                    8,
                    Availability::Healthy,
                    Placement {
                        island: "island-a",
                        domain: "domain-a",
                        tpu_slice: None,
                    },
                )
            })
            .collect();
        let pods = vec![
            owned_pod("kimi-engine-0", "gpu-0", "gpu", 4, "sandbox", "kimi"),
            owned_pod("kimi-decoder-0", "gpu-1", "gpu", 2, "sandbox", "kimi"),
            // Unlabelled (non-OME) pod: namespace consumers only.
            pod_in_namespace("batch-0", "gpu-2", "gpu", 8, "sandbox"),
        ];
        let report = build(&[cluster(nodes, pods)], &topology(), limits());

        assert_eq!(report.consumers.len(), 1);
        assert_eq!(report.consumers[0].namespace, "sandbox");
        assert_eq!(report.consumers[0].accelerators, 14);
        assert_eq!(report.consumers_by_deployment.len(), 1);
        let by_deployment = &report.consumers_by_deployment[0];
        assert_eq!(by_deployment.cluster, "cluster-a");
        assert_eq!(by_deployment.namespace, "sandbox");
        assert_eq!(by_deployment.name, "kimi");
        assert_eq!(by_deployment.kind, "gpu");
        assert_eq!(by_deployment.accelerators, 6);
        assert_eq!(by_deployment.pods, 2);
    }

    #[test]
    fn not_ready_gpu_is_reported_only_as_unavailable_inventory() {
        let report = build(
            &[cluster(
                vec![node(
                    "gpu-not-ready",
                    "gpu",
                    8,
                    Availability::NotReady,
                    Placement {
                        island: "island-a",
                        domain: "domain-a",
                        tpu_slice: None,
                    },
                )],
                vec![],
            )],
            &topology(),
            limits(),
        );

        assert_inventory_only(&report, 0);
    }

    #[test]
    fn cordoned_gpu_is_reported_only_as_unavailable_inventory() {
        let report = build(
            &[cluster(
                vec![node(
                    "gpu-cordoned",
                    "gpu",
                    8,
                    Availability::Cordoned,
                    Placement {
                        island: "island-a",
                        domain: "domain-a",
                        tpu_slice: None,
                    },
                )],
                vec![],
            )],
            &topology(),
            limits(),
        );

        assert_inventory_only(&report, 1);
    }

    #[test]
    fn mixed_health_and_allocations_exclude_unavailable_gpu_and_tpu_capacity() {
        let nodes = vec![
            node(
                "gpu-partial",
                "gpu",
                8,
                Availability::Healthy,
                Placement {
                    island: "island-a",
                    domain: "domain-a",
                    tpu_slice: None,
                },
            ),
            node(
                "gpu-full",
                "gpu",
                8,
                Availability::Healthy,
                Placement {
                    island: "island-b",
                    domain: "domain-full",
                    tpu_slice: None,
                },
            ),
            node(
                "gpu-not-ready",
                "gpu",
                8,
                Availability::NotReady,
                Placement {
                    island: "island-a",
                    domain: "domain-a",
                    tpu_slice: None,
                },
            ),
            node(
                "gpu-cordoned",
                "gpu",
                4,
                Availability::Cordoned,
                Placement {
                    island: "island-b",
                    domain: "domain-cordoned",
                    tpu_slice: None,
                },
            ),
            node(
                "tpu-partial",
                "tpu",
                4,
                Availability::Healthy,
                Placement {
                    island: "island-tpu",
                    domain: "domain-tpu",
                    tpu_slice: Some(("2x2x2", "slice-a")),
                },
            ),
            node(
                "tpu-not-ready",
                "tpu",
                4,
                Availability::NotReady,
                Placement {
                    island: "island-tpu",
                    domain: "domain-tpu",
                    tpu_slice: Some(("2x2x2", "slice-a")),
                },
            ),
        ];
        let pods = vec![
            pod("gpu-partial-work", "gpu-partial", "gpu", 3),
            pod("gpu-full-work", "gpu-full", "gpu", 8),
            pod("gpu-down-work", "gpu-not-ready", "gpu", 2),
            pod("gpu-cordoned-work", "gpu-cordoned", "gpu", 1),
            pod("tpu-partial-work", "tpu-partial", "tpu", 1),
            pod("tpu-down-work", "tpu-not-ready", "tpu", 2),
        ];
        let report = build(&[cluster(nodes, pods)], &topology(), limits());

        assert_eq!(report.nodes.total, 6);
        assert_eq!(report.nodes.ready, 4);
        assert_eq!(report.nodes.schedulable, 3);
        assert_eq!(report.nodes.accelerator_nodes, 6);
        assert_group(&report.accelerators, "gpu", "gpu", 2, 16, 11, 5);
        assert_group(&report.accelerators, "tpu", "tpu", 1, 4, 1, 3);
        assert_group(&report.by_cluster, "cluster-a", "gpu", 2, 16, 11, 5);
        assert_group(&report.by_cluster, "cluster-a", "tpu", 1, 4, 1, 3);
        assert_group(&report.by_island, "island-a", "gpu", 1, 8, 3, 5);
        assert_group(&report.by_island, "island-b", "gpu", 1, 8, 8, 0);
        assert_group(&report.by_island, "island-tpu", "tpu", 1, 4, 1, 3);
        assert_group(&report.by_nvlink_domain, "domain-a", "gpu", 1, 8, 3, 5);
        assert_group(&report.by_nvlink_domain, "domain-full", "gpu", 1, 8, 8, 0);
        assert_group(&report.by_nvlink_domain, "domain-tpu", "tpu", 1, 4, 1, 3);
        assert_group(&report.by_instance_type, "gpu.test", "gpu", 2, 16, 11, 5);
        assert_group(&report.by_instance_type, "tpu.test", "tpu", 1, 4, 1, 3);
        assert!(report
            .by_nvlink_domain
            .iter()
            .all(|g| g.name != "domain-cordoned"));

        let unavailable_gpu = lost(&report, "gpu");
        assert_eq!(unavailable_gpu.accelerators, 12);
        assert_eq!(unavailable_gpu.nodes, 2);
        let unavailable_tpu = lost(&report, "tpu");
        assert_eq!(unavailable_tpu.accelerators, 4);
        assert_eq!(unavailable_tpu.nodes, 1);

        let gpu_eight = placement(&report.gpu_nodes, "8");
        assert_eq!(gpu_eight.total, 3);
        assert_eq!(gpu_eight.free, 0);
        assert_eq!(gpu_eight.fragmented, 1);
        assert_eq!(gpu_eight.full, 1);
        assert_eq!(gpu_eight.unhealthy, 1);
        assert_eq!(gpu_eight.stranded_chips, 5);
        let gpu_four = placement(&report.gpu_nodes, "4");
        assert_eq!(gpu_four.total, 1);
        assert_eq!(gpu_four.unhealthy, 1);

        let mixed_gpu_domain = domain(&report.gpu_racks, "domain-a");
        assert_eq!(mixed_gpu_domain.chips, 16);
        assert_eq!(mixed_gpu_domain.free, 5);
        assert!(!mixed_gpu_domain.healthy);
        let cordoned_gpu_domain = domain(&report.gpu_racks, "domain-cordoned");
        assert_eq!(cordoned_gpu_domain.chips, 4);
        assert_eq!(cordoned_gpu_domain.free, 0);
        assert!(!cordoned_gpu_domain.healthy);

        assert_eq!(report.tpu_slices.len(), 1);
        let tpu_slice = placement(&report.tpu_slices, "2x2x2");
        assert_eq!(tpu_slice.chips, 8);
        assert_eq!(tpu_slice.total, 1);
        assert_eq!(tpu_slice.unhealthy, 1);
        assert_eq!(tpu_slice.stranded_chips, 0);
        let tpu_domain = domain(&report.tpu_subblocks, "domain-tpu");
        assert_eq!(tpu_domain.chips, 8);
        assert_eq!(tpu_domain.free, 3);
        assert!(!tpu_domain.healthy);
    }
}
