package endpoint

// PlacementEndpointControllerName names the endpoint publisher — explicit so it
// doesn't collide with the placement controller (both watch InferenceService).
const PlacementEndpointControllerName = "placement-endpoint"

const (
	// ManagedByLabel marks the HTTPRoute + ExternalName Service the endpoint
	// publisher creates, so they are discoverable (kubectl get -l ...) and the
	// reconciler can recognize what it owns. Stamped on every published resource.
	ManagedByLabel = "ome.io/managed-by"
	// ManagedByValue is the ManagedByLabel value for the endpoint publisher.
	ManagedByValue = "placement-endpoint"

	// PlacementClusterLabel records which WorkloadCluster the published backend
	// currently points at (the placement winner). Updated on re-placement; purely
	// observational.
	PlacementClusterLabel = "ome.io/placement-cluster"

	// EndpointFinalizer keeps a placed InferenceService around long enough for the
	// publisher to tear down its global-traffic backend before the object is
	// removed. Distinct from the placement controller's own finalizer so the two
	// controllers' teardown order is independent.
	EndpointFinalizer = "ome.io/placement-endpoint"
)
