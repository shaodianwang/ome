// Package placement implements the OEP-0018 control-plane fan-out controller:
// it places an InferenceService onto a workload cluster and mirrors its status.
package placement

const (
	// ClusterSelectorAnnotation holds a label-selector string (e.g.
	// "gpu=gb300,provider=cw") matched against WorkloadCluster labels to pick
	// candidate clusters. Empty/absent -> all Ready clusters are candidates.
	ClusterSelectorAnnotation = "ome.io/cluster-selector"

	// LocalQueueAnnotation names the Kueue LocalQueue the derived workload's
	// pods join on the target cluster. Defaults to DefaultLocalQueue.
	LocalQueueAnnotation = "ome.io/local-queue"
	DefaultLocalQueue    = "default"

	// PlacementOriginLabel marks a derived ISVC as created by this control
	// plane (for tracking + future GC). Value: the source ISVC's origin id.
	PlacementOriginLabel = "ome.io/placement-origin"
	// PlacementOriginUIDAnnotation records the source ISVC UID on the derived ISVC.
	PlacementOriginUIDAnnotation = "ome.io/placement-origin-uid"

	// KueueServingAnnotation is stamped on the derived pod templates so the
	// target cluster's Kueue gates + holds quota for the serving pods. (Kueue
	// vocabulary stays out of the user-facing API; the control plane sets
	// these on the derived object.)
	KueueServingAnnotation = "kueue.x-k8s.io/pod-group-serving"

	// PlacementFinalizer lets the controller delete the derived ISVC before the
	// source ISVC is removed.
	PlacementFinalizer = "ome.io/placement"
)
