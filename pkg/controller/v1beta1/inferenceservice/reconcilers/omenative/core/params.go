// Package core is the ISVC-side carrier types and adapter glue for the
// OMENative reconciler. After the workload-package extraction the bulk
// of the planning + expectations logic lives in
// pkg/controller/v1beta1/workload/; this package re-exports the
// workload types as ISVC-adapter-facing aliases and provides the
// ReconcileParams shim the legacy dispatch layer still passes around.
//
// The aliases let pre-extraction call sites
// (omenative.{ComponentPlan,InstancePlan,...,BuildPlan,NewExpectations})
// keep compiling unchanged while the bodies live in workload/.
// Adapter-aware callers should depend on workload directly; this
// package will shrink to the per-Source projection helpers in commit 9.
package core

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// Re-exported workload value types. Type aliases (`type A = B`) so
// values are interchangeable at compile time — call sites that hold
// onto a `core.ComponentPlan` are holding the exact same memory as
// `workload.ComponentPlan`.
type (
	ComponentPlan    = workload.ComponentPlan
	InstancePlan     = workload.InstancePlan
	RunnerPlan       = workload.RunnerPlan
	MigrationOverlay = workload.MigrationOverlay
	Expectations     = workload.Expectations
)

// Re-exported constructors. `var` rather than `func` re-exports so
// callers that took a value pointer (e.g., `f := core.NewExpectations`)
// keep the same observable identity.
var (
	NewExpectations     = workload.NewExpectations
	DefaultExpectations = workload.DefaultExpectations
)

// AllocateSurgeIndex is the ISVC-adapter-facing wrapper around
// workload.AllocateSurgeIndex. It centralizes the
// ISVC.Status.Components[c].OMENative.InstanceStatuses projection so
// callers don't repeat the nil-guard dance. Converts at the boundary
// via internalsource.InstanceStatusSliceToWorkload — slice 3 of the
// workload-extraction refactor flipped workload.AllocateSurgeIndex to
// take workload-owned types so the workload package itself stays free
// of pkg/apis/ome/v1beta1.
func AllocateSurgeIndex(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) int32 {
	return workload.AllocateSurgeIndex(internalsource.InstanceStatusSliceToWorkload(extractInstanceStatuses(isvc, component)))
}

// extractInstanceStatuses pulls the OMENative InstanceStatuses slice
// from the ISVC for one Component. Returns nil when the parent map /
// Component entry / OMENative subtree is missing — matches the
// behavior callers relied on before the workload extraction.
func extractInstanceStatuses(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	return cs.OMENative.InstanceStatuses
}

// BuildPlan is the ISVC-adapter-facing wrapper around
// workload.BuildPlan. Projects ReconcileParams to the workload
// (DesiredSpec, ObservedState) shape, then delegates.
//
// Adapter callers in commit 9 will skip this wrapper and call
// workload.BuildPlan directly with their own projection; this shim
// keeps the legacy dispatch chain compiling during the per-commit
// extraction.
func BuildPlan(p ReconcileParams) (ComponentPlan, error) {
	if p.ComponentExt == nil {
		return ComponentPlan{}, errNilComponentExt(p.Component)
	}

	desired := workload.WorkloadDesiredSpec{
		Replicas: replicasFromComponentExt(p.ComponentExt),
		MultiPod: p.MultiPod,
		Runners:  runnersForDesired(p.MultiPod, int32(p.WorkerSize)),
	}
	if p.ComponentExt.OMENative != nil {
		desired.Lifecycle = *p.ComponentExt.OMENative
	}

	observed := workload.WorkloadObservedState{
		InstanceStatuses: internalsource.InstanceStatusSliceToWorkload(extractInstanceStatuses(p.ISVC, p.Component)),
	}

	return workload.BuildPlan(p.Component, desired, observed)
}

// replicasFromComponentExt projects the ComponentExtensionSpec
// MinReplicas pointer into the workload spec's Replicas int32. nil or
// zero means "default to 1" — workload.BuildPlan handles the actual
// defaulting, so any value <= 0 here turns into 0 (caller-clear
// "unset") and the workload layer defaults.
func replicasFromComponentExt(c *v1beta1.ComponentExtensionSpec) int32 {
	if c == nil || c.MinReplicas == nil || *c.MinReplicas <= 0 {
		return 0
	}
	return int32(*c.MinReplicas)
}

// runnersForDesired returns the workload.Runner set for the desired
// shape. Single-pod: one default runner of size 1. Multi-pod:
// leader+worker pair.
func runnersForDesired(multiPod bool, workerSize int32) []workload.Runner {
	if !multiPod {
		return []workload.Runner{{Name: "default", Size: 1}}
	}
	return []workload.Runner{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: workerSize},
	}
}

// errNilComponentExt centralizes the nil-ComponentExt guard that
// workload.BuildPlan no longer needs (its caller always projects a
// non-nil Lifecycle from a non-nil ComponentExt). The ISVC adapter
// raises the error before the workload call.
func errNilComponentExt(component v1beta1.ComponentType) error {
	return fmt.Errorf("nil ComponentExtensionSpec for component=%s", component)
}

// ReconcileParams is the input bag the OMENative reconciler reads on each
// invocation. Constructed at the dispatch site in
// components/{engine,decoder,router}.go from the values reconcileDeployment
// already has in scope.
//
// The Expectations field's type comes from the workload package via the
// re-export above so the ISVC adapter and the future IR adapter can
// share the same cache instance.
type ReconcileParams struct {
	// ISVC is the parent InferenceService being reconciled.
	ISVC *v1beta1.InferenceService

	// ObjectMeta is the rendered per-Component metadata (labels,
	// annotations, owner refs).
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered pod template for the Component's leader /
	// single-pod runner.
	PodSpec *v1.PodSpec

	// Component identifies which of router / engine / decoder is being
	// reconciled.
	Component v1beta1.ComponentType

	// ComponentExt is the merged ComponentExtensionSpec, including the
	// `omenative` sub-block populated by the mutating webhook defaulter.
	ComponentExt *v1beta1.ComponentExtensionSpec

	// WorkerSize is Worker.Size for multi-pod Instances. Zero when the
	// Component is single-pod or has no Worker block (e.g., Router).
	WorkerSize int

	// WorkerPodSpec is the rendered pod template for the Worker runner.
	// Nil when WorkerSize is 0.
	WorkerPodSpec *v1.PodSpec

	// MultiPod indicates the Component declares Leader or Worker (Size > 0),
	// i.e., each Instance materializes more than one pod. Router is always
	// single-pod (MultiPod=false). Set at the dispatch site since
	// EngineSpec/DecoderSpec/RouterSpec aren't carried through this bag.
	MultiPod bool

	// GangSchedulingAvailable is a cluster-discovery boolean threaded
	// through from the InferenceServiceReconciler — true when the
	// scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup` CRD is
	// installed at controller startup. The OMENative reconciler reads
	// this to decide whether to call podgroup.EnsurePodGroup for each
	// multi-pod Instance. When false AND the plan has a multi-pod
	// Instance, the reconciler still creates pods but stamps the
	// `GangSchedulingUnavailable=True` Component condition (per
	// OEP-0007's soft-fail contract: gang scheduling is recommended,
	// not required). Tests / single-pod Components / pre-wiring
	// callers leave it false.
	GangSchedulingAvailable bool

	// Client is the controller-runtime client used to materialize pods,
	// patch status, and observe cluster state. Set at the dispatch site
	// from BaseComponentFields.Client.
	Client client.Client

	// APIReader is an optional live API reader, typically mgr.GetAPIReader().
	// When set it is used for the correctness-critical reads in revision
	// bookkeeping where stale cache contents could cause spurious
	// collisions or duplicate Revision numbers. When nil the cached Client
	// is used and the caller accepts the cache-lag exposure. Callers wire
	// this in once the controller manager exposes a live reader; tests
	// leave it nil since the fake client is a live source.
	APIReader client.Reader

	// Expectations is the create/delete bookkeeping cache the OMENative
	// reconciler uses to avoid re-issuing batches before the controller-
	// runtime watch has confirmed prior writes. Production wires the
	// same instance into the Pod event handler (watches.go) so observed
	// create / delete events release expectations immediately. When nil
	// the package-level DefaultExpectations singleton is used.
	Expectations *Expectations

	// Recorder emits K8s Events against the parent InferenceService at
	// lifecycle transitions (InstanceCreated, InPlaceUpdateStarted,
	// MigrationCompleted, etc. — see events.go for the full reason
	// set). nil-safe: when unset, recordEventf is a no-op so tests
	// don't need to plumb a recorder.
	Recorder record.EventRecorder
}

// Reader returns the live API reader when APIReader is set; otherwise the
// cached Client. Callers use this for revision bookkeeping and other
// reads that must not observe a stale cache.
func (p *ReconcileParams) Reader() client.Reader {
	if p.APIReader != nil {
		return p.APIReader
	}
	return p.Client
}

// ExpectationsCache returns the Expectations cache to use this reconcile —
// the per-reconciler instance threaded through ReconcileParams when
// set, or the package-level DefaultExpectations singleton as a
// fallback for tests and pre-wiring callers.
func (p *ReconcileParams) ExpectationsCache() *Expectations {
	if p.Expectations != nil {
		return p.Expectations
	}
	return DefaultExpectations
}
