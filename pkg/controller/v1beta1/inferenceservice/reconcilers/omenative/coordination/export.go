package coordination

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ObservePerRevisionPods is the exported entry point to the per-revision pod-count
// observation the coordination layer uses, so sibling rollout executors (e.g.
// canary) can reuse the exact same accounting instead of re-implementing it.
// Returns TWO maps keyed by component → revision hash: total pods, and the READY
// (serving) subset. Capacity gates that shift traffic must use the ready map.
//
// useIndex MUST match the reader (see observePerRevisionPods): the canary capacity
// gate passes the LIVE API reader, which has no Pod field index, so it MUST pass
// useIndex=false.
func ObservePerRevisionPods(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, useIndex bool) (total, ready map[v1beta1.ComponentType]map[string]int32, err error) {
	return observePerRevisionPods(ctx, reads, isvc, components, useIndex)
}

// LatestRevisionHash returns the latest (target) revision hash for a component,
// exported for reuse by sibling rollout executors.
func LatestRevisionHash(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	return latestRevisionHash(isvc, c)
}

// GCOrphanedPerRevisionServices deletes per-revision Service pairs whose revision
// hash has no live pods (liveHashes is the per-revision pod-count map). Exported
// so sibling rollout executors (e.g. canary) that ensure per-revision Services
// outside the coordination loop can prune orphans the same way.
func GCOrphanedPerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, liveHashes map[string]int32) error {
	return gcOrphanedPerRevisionServices(ctx, c, isvc, component, liveHashes)
}
