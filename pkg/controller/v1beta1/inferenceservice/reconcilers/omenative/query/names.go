package query

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// PodName returns the stable pod name for one ordinal of one Runner
// of one Instance — <isvc>-<comp>-<inst-idx>-<runner>-<ordinal>. Stable
// across reconciles so Create can dedup desired-vs-existing by name
// alone.
func PodName(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-%d-%s-%d", isvc, component, instanceIdx, runner, ordinal)
}

// HeadlessServiceName returns the per-Component headless service name —
// <isvc>-<comp>-headless. Used by every op to look up the service that
// publishes the pods it's about to mutate, and by status_aggregate to
// scan EndpointSlices for ready endpoints.
func HeadlessServiceName(isvc string, component v1beta1.ComponentType) string {
	return fmt.Sprintf("%s-%s-headless", isvc, component)
}

// InstanceSubdomain returns the per-Instance DNS subdomain stamped
// into the OME_INSTANCE_SUBDOMAIN env var. Format is
// <isvc>-<component>-<instance-idx>; today this is metadata-only — the
// Pod's Spec.Subdomain still points at HeadlessServiceName — but the
// env var carries the per-instance form so workload code can address
// its own Instance independently of the shared Service name.
func InstanceSubdomain(isvc string, component v1beta1.ComponentType, instanceIdx int32) string {
	return fmt.Sprintf("%s-%s-%d", isvc, component, instanceIdx)
}
