package canary

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// DispatchDeps is the controller-side input to the canary executor, mirroring
// coordination.ReconcileInputs. The controller calls Dispatch once per reconcile
// when spec.rollout.canary is set (mutually exclusive with coordination).
type DispatchDeps struct {
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	Now      time.Time
}

// Dispatch runs the canary step machine for an InferenceService, mutating
// isvc.Status in-memory (phase, traffic, step) and returning a requeue hint. The
// step machine is ISVC-level; it is driven through the externally-routed
// (primary) Component — the router for PD, otherwise the single Component — whose
// per-revision Services carry the external traffic weight. Per-component pod
// capacity is applied separately by the controller via EffectivePartition.
//
// No-op (requeue 0) when no canary plan is set.
func Dispatch(ctx context.Context, d DispatchDeps) (time.Duration, error) {
	if d.ISVC.Spec.GetCanary() == nil {
		return 0, nil
	}
	primary := primaryComponent(d.ISVC)
	if primary == "" {
		return 0, nil
	}
	// Observe per-revision pods for EVERY configured component (not just the
	// primary): the canary stages capacity on all of them, so the gate must wait
	// on all of them before shifting traffic.
	perRev, err := coordination.ObservePerRevisionPods(ctx, d.Reader, d.ISVC, configuredComponents(d.ISVC))
	if err != nil {
		return 0, err
	}
	pods := perRev[primary]

	canaryHash := canaryTargetHash(ctx, d.Reader, d.ISVC, primary)

	// Ensure the per-revision Services the canary's traffic status references for
	// the primary, then GC orphans. The canary path skips coordination (the usual
	// producer), so without this the status traffic weights point at Service names
	// that are never created; the downstream weighted-route consumer targets these
	// Services. Mirrors coordination's ensure+GC loop.
	multiPod := isvcutils.IsMultiPodComponent(d.ISVC, primary)
	for hash := range pods {
		if hash == "" {
			continue
		}
		if _, err := coordination.EnsurePerRevisionServices(ctx, d.Client, d.ISVC, primary, hash, multiPod); err != nil {
			return 0, err
		}
	}
	if err := coordination.GCOrphanedPerRevisionServices(ctx, d.Client, d.ISVC, primary, pods); err != nil {
		return 0, err
	}

	now := d.Now
	if now.IsZero() {
		now = time.Now()
	}

	res, err := Reconcile(ctx, ReconcileInputs{
		Client:                 d.Client,
		ISVC:                   d.ISVC,
		Component:              primary,
		CanaryRevisionHash:     canaryHash,
		StableRevisionHash:     otherRevision(pods, canaryHash),
		DesiredReplicas:        componentReplicas(d.ISVC, primary),
		PerRevisionPods:        pods,
		SecondaryCapacityReady: secondaryCapacityReady(d.ISVC, perRev, canaryHash, primary),
		Now:                    now,
	})
	if err != nil {
		return 0, err
	}

	// Canary rollback is deferred: the partition mechanism cannot revert instances
	// already rolled to the canary, so honoring ome.io/rollout-rollback wedges the
	// rollout at RollingBack. Treat it as a guarded no-op — surface a warning so
	// the operator knows the verb had no effect (only while a canary is actually
	// in progress, hence after Reconcile on res.Active), and let the canary
	// proceed per its plan. To roll back, revert the component spec.
	if d.Recorder != nil && res.Active && isRollbackRequested(d.ISVC) {
		d.Recorder.Event(d.ISVC, "Warning", "CanaryRollbackUnsupported",
			"ome.io/rollout-rollback is not yet supported for canary rollouts and was ignored; revert the component spec to roll back")
	}

	recordDispatch(d.Recorder, d.ISVC, primary, res)
	return res.RequeueAfter, nil
}

// configuredComponents lists every present Component in primary order
// (router > engine > decoder). The canary stages capacity on all of them, so the
// capacity gate observes all of them even though the step machine and traffic are
// driven through the primary.
func configuredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if isvc.Spec.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	if isvc.Spec.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	return out
}

// secondaryCapacityReady reports whether every NON-primary Component's canary
// pods have reached that Component's current-step newCount. The step machine and
// external traffic run through the primary, but traffic must not shift until
// EVERY Component's canary capacity is up — otherwise a Ready router (primary)
// can advance the step while the engine/decoder canary pods behind it are still
// coming up. True when there are no secondaries (single-component canary). The
// step index mirrors EffectivePartition so the gate matches the staged partition.
func secondaryCapacityReady(isvc *v1beta1.InferenceService, perRev map[v1beta1.ComponentType]map[string]int32, canaryHash string, primary v1beta1.ComponentType) bool {
	plan := isvc.Spec.GetCanary()
	if plan == nil || len(plan.Steps) == 0 || canaryHash == "" {
		return true
	}
	idx := int32(0)
	if isvc.Status.Canary != nil {
		idx = isvc.Status.Canary.CurrentStep
	}
	if int(idx) >= len(plan.Steps) {
		idx = int32(len(plan.Steps) - 1)
	}
	step := plan.Steps[idx]
	for _, c := range configuredComponents(isvc) {
		if c == primary {
			continue
		}
		if perRev[c][canaryHash] < resolveStepNewCount(step, componentReplicas(isvc, c)) {
			return false
		}
	}
	return true
}

// canaryTargetHash returns the revision hash the canary is rolling toward for a
// Component. It reads the authoritative InferenceReplica status — the workload
// object whose RollingUpdate.Partition the canary drives — rather than the
// ISVC's aggregated lifecycle status. In the IR-managed path the projector
// refreshes that ISVC aggregate only later in the same reconcile, after Dispatch
// runs, so at dispatch time it still names the pre-roll (stable) revision;
// reading it would point the canary at the wrong revision and wedge the capacity
// gate on an empty pod bucket. Falls back to the ISVC lifecycle aggregate for
// the legacy path, where no IR exists and the in-reconcile status aggregation
// has already run by the time the canary dispatches.
func canaryTargetHash(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if reads != nil {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, c)}
		if err := reads.Get(ctx, key, ir); err == nil {
			if name := ir.Status.UpdateRevision; name != "" {
				return query.RevisionHashFromControllerRevisionName(name)
			}
			if name := ir.Status.CurrentRevision; name != "" {
				return query.RevisionHashFromControllerRevisionName(name)
			}
		}
	}
	return coordination.LatestRevisionHash(isvc, c)
}

// primaryComponent is the externally-routed Component the canary drives its step
// machine + traffic weight through: router (PD) > engine > decoder.
func primaryComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	switch {
	case isvc.Spec.Router != nil:
		return v1beta1.RouterComponent
	case isvc.Spec.Engine != nil:
		return v1beta1.EngineComponent
	case isvc.Spec.Decoder != nil:
		return v1beta1.DecoderComponent
	}
	return ""
}

// componentReplicas is the steady replica count for a Component (MinReplicas,
// falling back to MaxReplicas, else 0).
func componentReplicas(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) int32 {
	var ext *v1beta1.ComponentExtensionSpec
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			ext = &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			ext = &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			ext = &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	if ext == nil {
		return 0
	}
	if ext.MinReplicas != nil {
		return int32(*ext.MinReplicas)
	}
	return int32(ext.MaxReplicas)
}

// otherRevision returns the non-canary revision hash with live pods (the stable
// revision). Empty when only the canary revision is present.
func otherRevision(pods map[string]int32, canaryHash string) string {
	best, bestN := "", int32(0)
	for h, n := range pods {
		if h == canaryHash || h == "" {
			continue
		}
		if n >= bestN {
			best, bestN = h, n
		}
	}
	return best
}
