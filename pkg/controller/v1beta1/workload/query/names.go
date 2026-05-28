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

// StableServiceName returns the per-Component stable (non-headless,
// non-revision-scoped) Service name — <isvc>-<comp>. Matches the
// Service-name convention RawDeployment / MultiNode use (see
// constants.{Engine,Decoder,Router}ServiceName) so the existing ingress
// strategies — which point HTTPRoute / Ingress backendRefs at
// <isvc>-<comp> — work for OMENative without a per-mode strategy.
//
// The Service selects all OMENative pods for the Component regardless
// of revision; it is the cross-revision aggregate ingress target. The
// per-revision Services (PerRevisionServiceName) remain available for
// the OEP-0011 weighted-routing path.
func StableServiceName(isvc string, component v1beta1.ComponentType) string {
	return fmt.Sprintf("%s-%s", isvc, component)
}

// PerRevisionServiceName returns the per-revision *routed* Service name
// for one (component, revisionHash) tuple. Format:
// `<isvc>-<component>-rev-<hash>`.
//
// Mirrors the legacy coordination.PerRevisionServiceName helper at the
// byte level so existing Service names line up across the dispatch
// path. Lives in workload/query/ because workload/ops/delete.go reads
// per-revision Service names when computing drain targets and must not
// import the omenative coordination package (which would close a cycle
// with the workload extraction). The coordination package retains its
// own copy until the per-revision Service builders themselves move to
// workload/ in a later slice.
func PerRevisionServiceName(isvc string, component v1beta1.ComponentType, revisionHash string) string {
	return fmt.Sprintf("%s-%s-rev-%s", isvc, component, revisionHash)
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

// PodGroupName returns the deterministic name of the scheduler-plugins
// PodGroup that gates one multi-pod Instance — <isvc>-<component>-<idx>.
// Matches the InstanceSubdomain shape on purpose: operators inspecting
// `kubectl get pg <name>` can pivot directly to
// `kubectl get pods -l <subdomain>` without re-deriving the prefix.
// Single-pod Instances do NOT get a PodGroup; callers must check
// inst.TotalPods() > 1 before invoking the builder.
func PodGroupName(isvc string, component v1beta1.ComponentType, instanceIdx int32) string {
	return fmt.Sprintf("%s-%s-%d", isvc, component, instanceIdx)
}
