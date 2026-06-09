// Package irprojector projects an InferenceService Component (engine /
// decoder / router) into the per-(ISVC, Component) InferenceReplica
// resource the IR controller reconciles.
//
// The projector is the bridge into the IR-driven world: when a
// Component's effective deployment mode is OMENative, the ISVC
// controller hands the desired per-Component spec to
// ensureInferenceReplica. The IR controller then takes over the
// per-Instance lifecycle — Create / Update / Restart / Migrate /
// Delete — owning its own pods, ControllerRevisions, and per-Component
// headless Service.
//
// The package is intentionally narrow:
//
//   - IsIRManagedComponent reads the resolved deployment mode and
//     returns the predicate the dispatch sites in
//     components/{engine,decoder,router}.go branch on. OMENative-mode
//     Components ALWAYS route through the IR-managed path.
//
//   - EnsureInferenceReplica is the CreateOrUpdate driver: build the
//     desired IR Spec from the rendered per-Component inputs, owner-ref
//     the IR back to the parent ISVC, stamp the controller-write
//     annotation the IR webhook gates on, and persist.
//
// The package does NOT:
//   - reach into the IR controller's internals (writes Spec, reads
//     Status only — see ReadInferenceReplica in status.go);
//   - duplicate pod-spec rendering (the dispatch site already computed
//     the PodSpec; ensure passes it through verbatim);
//   - manage IR deletion outside foreground GC (owner-ref cascade
//     handles it on ISVC delete; explicit operator IR deletes are out
//     of scope).
package irprojector

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// IsIRManagedComponent returns true when the given Component should be
// driven through the InferenceReplica path. Only constants.OMENative
// routes through the IR-managed path; every other mode (RawDeployment,
// MultiNode, …) falls through to its own dispatch branch in
// components/{engine,decoder,router}.go.
//
// OMENative-mode Components ALWAYS route through the IR-managed path —
// there is no per-Component opt-in and no cluster-wide opt-out. The IR
// path is the sole OMENative implementation.
func IsIRManagedComponent(deploymentMode constants.DeploymentModeType) bool {
	return deploymentMode == constants.OMENative
}

// isvcGVK identifies the parent ISVC for owner-ref stamping on
// emitted InferenceReplicas. Declared here so the package is
// self-contained and doesn't need a cross-package import.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// Params is the input bag EnsureInferenceReplica reads to build the
// desired IR Spec. The dispatch site in
// components/{engine,decoder,router}.go fills it from the rendered
// per-Component values.
//
// Critical contract: PodSpec / WorkerPodSpec / ObjectMeta are the
// rendered per-pod template values. The projector does not re-render —
// it hands the IR controller the exact same per-pod template the
// dispatch site computed.
type Params struct {
	// ISVC is the parent InferenceService. The projector reads
	// ISVC.Name / Namespace / UID for IR naming + owner-ref, and
	// passes Spec.<component>.Lifecycle into the IR Spec.
	ISVC *v1beta1.InferenceService

	// Component identifies which of engine / decoder / router is
	// being projected. The IR name is <isvc>-<component>; the IR
	// Spec.Component field stamps this verbatim.
	Component v1beta1.ComponentType

	// ComponentExt is the merged ComponentExtensionSpec for this
	// Component. The projector reads MinReplicas (defaults to 1) and
	// Lifecycle. Other fields (autoscaler, KEDA, deployment strategy)
	// are owned by the ISVC controller's other reconcilers — they
	// don't influence the IR Spec.
	ComponentExt *v1beta1.ComponentExtensionSpec

	// ObjectMeta is the rendered per-Component metadata the omenative
	// path would stamp on each pod. Threaded into the IR via the
	// PodTemplateSpec.ObjectMeta on each Runner so the IR-rendered
	// pods inherit the same labels / annotations the legacy path
	// produced.
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered leader / single-pod template the
	// omenative path would have used. Required.
	PodSpec *corev1.PodSpec

	// WorkerPodSpec is the rendered worker template for multi-pod
	// Components. nil for single-pod Components (router; engine /
	// decoder without Worker block).
	WorkerPodSpec *corev1.PodSpec

	// WorkerSize is Worker.Size for multi-pod Components. Zero for
	// single-pod Components.
	WorkerSize int

	// MultiPod indicates the Component declares Leader + Worker
	// (each Instance materializes more than one pod). Set at the
	// dispatch site since EngineSpec / DecoderSpec aren't carried
	// through this bag.
	MultiPod bool

	// ResolvedAutoscaler is the authoritative per-Component
	// ComponentAutoscaler the autoscaler.ResolveComponentAutoscaler
	// helper picked from the ISVC → runtime → default chain. The
	// projector deep-copies this onto ir.Spec.Autoscaler with
	// whole-block replace semantics (operator-side ISVC / runtime
	// edits always win over any drifted IR.spec.autoscaler). nil is
	// accepted and projects to nil ir.Spec.Autoscaler (no spurious
	// empty block); callers should normally pass the resolver's
	// non-nil return value.
	ResolvedAutoscaler *v1beta1.ComponentAutoscaler

	// Client is the controller-runtime client used to CreateOrUpdate
	// the IR. The ISVC controller already holds this.
	Client client.Client
}

// EnsureInferenceReplica computes the desired IR Spec from Params,
// stamps the owner-ref back to the parent ISVC, attaches the
// controller-write annotation the IR webhook gates on, and
// CreateOrUpdates the object. Returns the post-write IR so the
// caller can read IR.Status downstream (status.go uses this).
//
// Idempotent: the second invocation with the same Params produces
// the same Spec and either no-ops or patches only diffed fields.
// The retry.RetryOnConflict wrapper handles concurrent reconciles
// (the ISVC controller and the IR controller can both bump
// ResourceVersion; retry resolves the race).
//
// Errors are wrapped with the offending IR namespace/name for grep-
// ability in operator logs.
func EnsureInferenceReplica(ctx context.Context, p Params) (*v1beta1.InferenceReplica, error) {
	if err := validateParams(p); err != nil {
		return nil, err
	}

	name := InferenceReplicaName(p.ISVC.Name, p.Component)
	key := types.NamespacedName{Namespace: p.ISVC.Namespace, Name: name}

	logger := log.FromContext(ctx).WithValues(
		"isvc", client.ObjectKey{Namespace: p.ISVC.Namespace, Name: p.ISVC.Name},
		"component", p.Component,
		"inferencereplica", key)

	var committed *v1beta1.InferenceReplica
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ir := &v1beta1.InferenceReplica{}
		getErr := p.Client.Get(ctx, key, ir)
		switch {
		case apierrors.IsNotFound(getErr):
			ir = newInferenceReplica(p, name)
			if err := p.Client.Create(ctx, ir); err != nil {
				if apierrors.IsAlreadyExists(err) {
					// Cache lag: the Get said NotFound but the
					// apiserver already has the IR (the projector's
					// own previous Create just landed, or a racing
					// peer wrote it). Log so repeated firings of this
					// branch — a real symptom of cache desync — surface
					// in operator logs at V(1).
					logger.V(1).Info("InferenceReplica racing-create resolved as Conflict; retrying",
						"parentUID", p.ISVC.UID)
					return apierrors.NewConflict(
						schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"},
						name,
						fmt.Errorf("racing create"),
					)
				}
				return fmt.Errorf("create IR %s/%s: %w", p.ISVC.Namespace, name, err)
			}
			committed = ir
			return nil
		case getErr != nil:
			return fmt.Errorf("get IR %s/%s: %w", p.ISVC.Namespace, name, getErr)
		}

		// IR exists — apply the desired spec on top of the live object,
		// preserving ResourceVersion. We don't clobber Spec fields the
		// projector doesn't own (Pacing, Paused — owned by the ISVC
		// controller's coordination layer in future PRs).
		applyDesiredSpec(ir, p, name)
		if err := p.Client.Update(ctx, ir); err != nil {
			return fmt.Errorf("update IR %s/%s: %w", p.ISVC.Namespace, name, err)
		}
		committed = ir
		return nil
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
}

// InferenceReplicaName returns the canonical IR name for an
// (ISVC, Component) pair: <isvc>-<component>. The IR controller's
// revision/service helpers all key off ParentRef.Name so the IR's
// name itself is mostly a routing identifier; we keep the legacy
// per-Component naming so kubectl debugging is intuitive.
func InferenceReplicaName(isvcName string, component v1beta1.ComponentType) string {
	return isvcName + "-" + string(component)
}

// validateParams enforces the contract the projector relies on. The
// dispatch site in components/{engine,decoder,router}.go always
// passes a non-nil ISVC / ComponentExt / PodSpec; the explicit guard
// surfaces wiring bugs as a clear error instead of a nil deref.
func validateParams(p Params) error {
	if p.Client == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil ISVC (component=%s)", p.Component)
	}
	if p.ComponentExt == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil ComponentExtensionSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	if p.PodSpec == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil PodSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	if p.MultiPod && p.WorkerPodSpec == nil {
		return fmt.Errorf("EnsureInferenceReplica: MultiPod=true but nil WorkerPodSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	return nil
}

// newInferenceReplica builds a fresh IR from Params for the Create
// branch. The owner-ref stamps the ISVC as the controller (so
// foreground GC cascades on ISVC delete) and stamps the
// controller-write annotation the IR validating webhook gates on.
//
// Labels are inherited from the rendered per-Component ObjectMeta so
// the IR object itself carries the same legacy OMENative trio every
// downstream consumer expects.
func newInferenceReplica(p Params, name string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.ISVC.Namespace,
			Labels:    copyMap(p.ObjectMeta.Labels),
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(p.ISVC, isvcGVK),
			},
		},
	}
	applyDesiredSpec(ir, p, name)
	return ir
}

// applyDesiredSpec stamps the projected fields onto the IR. Split out
// so the Create + Update branches in EnsureInferenceReplica produce
// the same Spec (the only difference between Create and Update is
// whether the apiserver assigns a UID — and Spec.Replicas, which is
// autoscaler-owned on Update; see desiredReplicas).
//
// Preserves Spec.Pacing and Spec.Paused on Update — those fields are
// written by the ISVC controller's coordination layer in future PRs;
// clobbering them every reconcile would fight the coordination writer.
// (Today they're always nil/false; the preserve guard is defensive
// against future PRs landing the coordination wire-in.)
//
// Preserves Spec.Replicas on Update when the Component is autoscaler-
// managed — the IR's /scale subresource is the HPA / KEDA / external
// scale target, so the autoscaler is the authoritative writer of
// spec.replicas. Re-stamping MinReplicas every reconcile would clobber
// the autoscaler's value and make the ISVC controller and the autoscaler
// fight over the count. See desiredReplicas for the full decision.
func applyDesiredSpec(ir *v1beta1.InferenceReplica, p Params, name string) {
	// Force the metadata fields the projector owns. Annotations get
	// the controller-write stamp; user-set annotations on the IR are
	// not preserved across reconciles (the IR is a controller-only
	// resource — users edit the parent ISVC).
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] = constants.InferenceReplicaControllerWriteAnnotationVal

	// Labels track the per-Component ObjectMeta the omenative path
	// would have stamped on pods — copying them onto the IR itself
	// gives kubectl-side filterability without changing the pod
	// label-set the IR controller emits.
	ir.Labels = copyMap(p.ObjectMeta.Labels)

	// Owner-ref must stamp the live ISVC so foreground GC cascades
	// the IR on ISVC delete. Update overwrites any drifted owner ref
	// (operators who edit the IR's owner refs by hand will see them
	// re-stamped next reconcile — intentional).
	ir.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(p.ISVC, isvcGVK),
	}

	// ParentRef + Component are immutable post-create per the IR
	// webhook. On Update we re-stamp the same values — no-op for a
	// well-formed IR, defense-in-depth if someone manually edited
	// the live object.
	ir.Spec.ParentRef = v1beta1.ParentReference{
		Name: p.ISVC.Name,
	}
	ir.Spec.Component = p.Component
	ir.Spec.Replicas = desiredReplicas(ir, p)
	ir.Spec.Runners = runnersFromParams(p)
	ir.Spec.Lifecycle = lifecycleFromComponentExt(p.ComponentExt)
	// Project the resolved Autoscaler onto the IR. Whole-block replace
	// semantics — operator-side changes to isvc.spec.<comp>.autoscaler
	// always win over any drifted IR.spec.autoscaler. DeepCopy keeps the
	// IR's pointer disjoint from the resolver's return value so a downstream
	// caller mutating the IR cannot corrupt the next reconcile's resolution.
	// nil ResolvedAutoscaler clears ir.Spec.Autoscaler — defensive against a
	// dispatch site that doesn't run the resolver (no spurious empty block
	// lands on the IR).
	ir.Spec.Autoscaler = p.ResolvedAutoscaler.DeepCopy()
}

// desiredReplicas decides the value to stamp on ir.Spec.Replicas,
// mirroring the Raw path's "preserve existing replicas" approach
// (reconcilers/deployment/deployment_reconciler.go: checkDeploymentExist
// copies existingDeployment.Spec.Replicas onto the target so HPA's writes
// survive the reconcile).
//
// The IR exposes a /scale subresource at .spec.replicas
// (inferencereplica_types.go), so for an autoscaler-managed Component the
// HPA / KEDA / external scaler is the authoritative writer of
// spec.replicas. The decision:
//
//   - CREATE (no live IR yet — ir.ResourceVersion == ""): stamp
//     MinReplicas. There is nothing for the autoscaler to have written
//     yet; MinReplicas is the correct initial desired count regardless of
//     autoscaler class.
//
//   - UPDATE + autoscaler-managed (resolved Class != None): PRESERVE the
//     live ir.Spec.Replicas. Re-stamping MinReplicas would clobber the
//     value HPA / KEDA / an external scaler wrote via /scale, making the
//     ISVC controller and the autoscaler fight over the count every
//     reconcile. Defensive fall-back to MinReplicas if the live value is
//     somehow nil/<=0 (a well-formed IR always has it set from create, so
//     this only guards against a hand-edited or partially-migrated IR —
//     we never write a nil into a live scale target).
//
//   - UPDATE + autoscaling OFF (resolved Class == None / nil): the ISVC
//     controller owns the count, so stamp MinReplicas. (Class None is the
//     proportional-policy follower case where OME drives replicas directly;
//     until that coordinator wires in, MinReplicas is the floor.)
//
// ir is the object applyDesiredSpec is mutating — on the Update path it is
// the live IR fetched via Get (so ResourceVersion + the autoscaler-written
// Spec.Replicas are populated); on the Create path it is a freshly built
// object with an empty ResourceVersion.
func desiredReplicas(ir *v1beta1.InferenceReplica, p Params) *int32 {
	creating := ir.ResourceVersion == ""
	if creating || !isAutoscalerManaged(p.ResolvedAutoscaler) {
		return replicasFromComponentExt(p.ComponentExt)
	}
	// Update + autoscaler-managed: preserve the live (autoscaler-written)
	// value. Guard against a malformed live IR carrying nil/<=0 — never
	// write a nil into a live scale target.
	if ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		return ir.Spec.Replicas
	}
	return replicasFromComponentExt(p.ComponentExt)
}

// isAutoscalerManaged reports whether the resolved Component autoscaler
// drives the IR's /scale subresource — i.e. whether an autoscaler (not the
// ISVC controller) is the authoritative writer of spec.replicas.
//
// Every class except None has an external writer of spec.replicas: HPA and
// KEDA are OME-managed scalers that target the /scale subresource, and
// External is an operator-owned scaler that writes /scale directly
// (documented on InferenceReplicaSpec.Replicas / .Autoscaler). Only None
// means "no autoscaler at all" — the ISVC controller (or the proportional
// coordinator, also OME) owns the count. A nil resolved block is treated as
// None (matches autoscaler.autoscalerClass / resolvedClass).
func isAutoscalerManaged(a *v1beta1.ComponentAutoscaler) bool {
	if a == nil {
		return false
	}
	return a.Class != v1beta1.AutoscalerNone
}

// replicasFromComponentExt projects ComponentExtensionSpec.MinReplicas
// into the IR Spec.Replicas pointer. Defaults to 1 when MinReplicas
// is nil OR <= 0 — matches the workload-side projection
// (core/params.go: replicasFromComponentExt) so the rendered Instance
// count is consistent regardless of which adapter built the IR.
func replicasFromComponentExt(c *v1beta1.ComponentExtensionSpec) *int32 {
	if c == nil || c.MinReplicas == nil || *c.MinReplicas <= 0 {
		one := int32(1)
		return &one
	}
	r := int32(*c.MinReplicas)
	return &r
}

// runnersFromParams produces the IR.Spec.Runners projection. Single-
// pod Components emit one Runner of {Name: default, Size: 1};
// multi-pod Components emit {leader, Size=1} + {worker, Size=N}.
// Mirrors the workload.BuildPlan projection (core/params.go:
// runnersForDesired) so the workload dispatcher sees the same Runner
// shape regardless of which adapter built the IR.
//
// The Runner.Template carries the rendered PodSpec PLUS the
// per-Component ObjectMeta (labels + annotations). The IR controller's
// desiredFromIR projection reads back PodSpec + ObjectMeta from
// these templates into WorkloadDesiredSpec.PodSpec /
// PodTemplateObjectMeta — that's how the rendered template flows
// into the workload renderer without re-rendering.
func runnersFromParams(p Params) []v1beta1.Runner {
	tmplMeta := templateObjectMeta(p)
	if !p.MultiPod {
		return []v1beta1.Runner{{
			Name: v1beta1.RunnerNameDefault,
			Size: 1,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: tmplMeta,
				Spec:       *p.PodSpec,
			},
		}}
	}
	out := []v1beta1.Runner{
		{
			Name: v1beta1.RunnerNameLeader,
			Size: 1,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: tmplMeta,
				Spec:       *p.PodSpec,
			},
		},
	}
	if p.WorkerPodSpec != nil {
		out = append(out, v1beta1.Runner{
			Name: v1beta1.RunnerNameWorker,
			Size: int32(p.WorkerSize),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: tmplMeta,
				Spec:       *p.WorkerPodSpec,
			},
		})
	}
	return out
}

// templateObjectMeta builds the PodTemplateSpec.ObjectMeta stamped on
// every Runner. It merges TWO sources:
//
//  1. p.ObjectMeta.{Labels,Annotations} — the rendered per-Component
//     metadata the dispatch site computed via reconcileObjectMeta
//     (which already folds in BaseModel / ServingRuntime / ISVC-level
//     labels + annotations).
//
//  2. p.ComponentExt.{Labels,Annotations} — the user-declared
//     per-Component overrides from spec.<component>.{labels,annotations}.
//     These take precedence (last-write-wins) so an operator bumping
//     spec.engine.annotations to force a rollout sees that key land on
//     the pod template, which in turn flips the revision hash and
//     triggers the ControllerRevision / rollout machinery the IR
//     controller drives. Mirrors the components/{engine,decoder,router}.go
//     processAnnotations / processLabels merge order.
//
// Name / Namespace / GenerateName / CreationTimestamp / ResourceVersion
// / UID are intentionally dropped — those are owner-object fields and
// stamping them on the template would leak the per-Component ObjectMeta
// identity onto every emitted pod.
//
// Defensive against caller bugs: even when the dispatch site forgets
// to merge p.ComponentExt.{Labels,Annotations} into p.ObjectMeta, the
// projector still emits them on the template. Annotation-only ISVC
// edits (the canonical no-image-bump rollout trigger) only work if
// the projector treats ComponentExt as authoritative for these keys.
func templateObjectMeta(p Params) metav1.ObjectMeta {
	labels := copyMap(p.ObjectMeta.Labels)
	annotations := copyMap(p.ObjectMeta.Annotations)
	if p.ComponentExt != nil {
		if len(p.ComponentExt.Labels) > 0 {
			if labels == nil {
				labels = make(map[string]string, len(p.ComponentExt.Labels))
			}
			for k, v := range p.ComponentExt.Labels {
				labels[k] = v
			}
		}
		if len(p.ComponentExt.Annotations) > 0 {
			if annotations == nil {
				annotations = make(map[string]string, len(p.ComponentExt.Annotations))
			}
			for k, v := range p.ComponentExt.Annotations {
				annotations[k] = v
			}
		}
	}
	return metav1.ObjectMeta{
		Labels:      labels,
		Annotations: annotations,
	}
}

// lifecycleFromComponentExt deep-copies the LifecycleSpec when set
// (defensive — the ISVC and IR controllers should not share a
// pointer). nil when ComponentExt has no Lifecycle block.
func lifecycleFromComponentExt(c *v1beta1.ComponentExtensionSpec) *v1beta1.LifecycleSpec {
	if c == nil || c.Lifecycle == nil {
		return nil
	}
	return c.Lifecycle.DeepCopy()
}

// copyMap returns a shallow copy of m. nil input returns nil so the
// caller can distinguish "no labels" from "empty labels" (some
// downstream serializers care about the distinction).
func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
