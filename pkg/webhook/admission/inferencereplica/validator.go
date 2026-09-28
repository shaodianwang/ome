// Package inferencereplica implements the validating admission webhook
// for the InferenceReplica CRD. InferenceReplica is a controller-only
// resource: users only ever write InferenceService, and the ISVC
// controller is the sole writer of InferenceReplica specs.
//
// This webhook enforces the convention by rejecting any Create / Update
// that lacks the ome.io/controller-write=true annotation, and any
// spec-field edits the ISVC controller never makes (spec.runners,
// spec.podTemplate, spec.parentRef, spec.component) from an actor without
// the annotation. RBAC restricting write on inferencereplicas to the OME
// ServiceAccount is the actual security boundary — this webhook is the
// "discouragement" layer that yields clear error messages and stops
// well-intentioned kubectl edits.
//
// Deletion is not gated: operators may kubectl delete an InferenceReplica
// for debugging, and the controller will recreate it on the next ISVC
// reconcile.
package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/sliceprovision"
	"github.com/ome-projects/ome/pkg/validation"
)

var log = logf.Log.WithName("inferencereplica-validation-webhook")

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-inferencereplica,mutating=false,failurePolicy=fail,groups=ome.io,resources=inferencereplicas,versions=v1beta1,name=inferencereplica.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1
// +kubebuilder:object:generate=false
type Validator struct {
	Decoder admission.Decoder
	// TPUSliceProvisioning is the slice provisioning the controller runs,
	// nil when it provisions no slices. Nodes, set with it, reads the nodes
	// an Instance's slice demand is resolved against.
	TPUSliceProvisioning *controllerconfig.TPUSliceProvisioningConfig
	Nodes                client.Reader
}

// hasControllerWriteAnnotation accepts only the literal "true";
// truthy variants (yes/True) reject so hand edits can't sneak through.
func hasControllerWriteAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[constants.InferenceReplicaControllerWriteAnnotationKey] ==
		constants.InferenceReplicaControllerWriteAnnotationVal
}

func denyDirectWrite(op admissionv1.Operation, namespace, name string) admission.Response {
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica is a controller-only resource and rejects direct "+
			"%s on %s/%s: missing %s=%s annotation. Edit the parent "+
			"InferenceService spec instead; the controller will reconcile "+
			"the InferenceReplica.",
		op, namespace, name,
		constants.InferenceReplicaControllerWriteAnnotationKey,
		constants.InferenceReplicaControllerWriteAnnotationVal,
	))
}

// Handle gates writes per the package doc:
//   - Create / Update without ome.io/controller-write=true ⇒ Denied.
//   - Update changing spec.parentRef or spec.component (even from the
//     controller) ⇒ Denied; recreating the IR is required.
//   - Update touching nothing but metadata.finalizers ⇒ Allowed
//     unconditionally (see isFinalizerOnlyUpdate).
//   - Delete is not registered in the kubebuilder verbs list and
//     therefore not reached here; operators may delete to recover.
func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	// Defensive: webhook is registered for Create+Update only, but a
	// future operator misconfig shouldn't surprise us.
	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
	default:
		return admission.Allowed("")
	}

	newObj := &v1beta1.InferenceReplica{}
	if err := v.Decoder.Decode(req, newObj); err != nil {
		log.Error(err, "Failed to decode InferenceReplica",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Immutability check runs unconditionally on every Update, BEFORE
	// the controller-write annotation gate. The annotation gates WHO
	// can write (operator vs. controller), but field immutability is a
	// stronger contract that applies to ALL writers — a misbehaving
	// controller (e.g., stale ISVC controller cache) that stamps the
	// controller-write annotation must NOT also be able to change
	// spec.parentRef.UID to the wrong parent or move the IR between
	// Component slots. This is exactly the failure mode webhook gates
	// exist for.
	var oldObj *v1beta1.InferenceReplica
	if req.Operation == admissionv1.Update {
		oldObj = &v1beta1.InferenceReplica{}
		if err := v.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
			log.Error(err, "Failed to decode old InferenceReplica",
				"namespace", req.Namespace, "name", req.Name)
			return admission.Errored(http.StatusBadRequest, err)
		}

		// Finalizer-only updates are admitted before every gate below.
		// Such a patch cannot violate any invariant this webhook protects
		// (spec immutability, controller-write provenance) — but denying
		// it can manufacture a teardown wedge: this webhook is fail-
		// closed, so a refused finalizer-removal update leaves a
		// Terminating IR stuck behind our own admission gate.
		if isFinalizerOnlyUpdate(oldObj, newObj) {
			return admission.Allowed("finalizer-only update")
		}

		if !reflect.DeepEqual(oldObj.Spec.ParentRef, newObj.Spec.ParentRef) {
			return admission.Denied(fmt.Sprintf(
				"InferenceReplica %s/%s: spec.parentRef is immutable",
				req.Namespace, req.Name))
		}
		if oldObj.Spec.Component != newObj.Spec.Component {
			return admission.Denied(fmt.Sprintf(
				"InferenceReplica %s/%s: spec.component is immutable",
				req.Namespace, req.Name))
		}
	}

	if !hasControllerWriteAnnotation(newObj.Annotations) {
		log.V(1).Info("Rejecting direct write to InferenceReplica",
			"op", req.Operation,
			"namespace", req.Namespace, "name", req.Name,
			"userInfo", req.UserInfo.Username)
		return denyDirectWrite(req.Operation, req.Namespace, req.Name)
	}

	// Validate the projected Autoscaler block shape. The IR
	// webhook is strict about all spec writes by default (controller-
	// write annotation gate above); this is defense-in-depth against a
	// misbehaving controller writing a malformed projection. External
	// writes to spec.autoscaler are deliberately NOT blocked —
	// the /scale subresource only mutates spec.replicas, so a successful
	// write here means either the controller did it or someone forged
	// the annotation. Either way the shape must be valid.
	if err := validateIRAutoscaler(newObj); err != nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: %s",
			req.Namespace, req.Name, err.Error()))
	}

	// Pacing.Partition must be <= effective Replicas, otherwise an
	// over-Partition silently holds every Instance back forever (the
	// rollout engine treats Partition as a per-index threshold and
	// freezes any Instance with index < Partition).
	if err := validateIRPacing(newObj); err != nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: %s",
			req.Namespace, req.Name, err.Error()))
	}

	// Every container/initContainer volumeMount in a rendered Runner pod
	// template must resolve to a volume declared in that same pod spec.
	// A mount that names an undeclared volume is accepted here but
	// REJECTED by the kube-apiserver at pod-create time during reconcile
	// ("spec.containers[0].volumeMounts[0].name: Not found: <name>"),
	// which surfaces only as a buried InferenceReplica reconcile error
	// in the manager log — with no signal at kubectl apply. This gate is
	// defense-in-depth: it turns that silent failure into a clear
	// admission rejection (and also catches plain volumeMount typos in a
	// runtime's pod template).
	if err := validateIRRunnerVolumes(newObj); err != nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: %s",
			req.Namespace, req.Name, err.Error()))
	}

	// The controller withholds the pods of an Instance that cannot fill a
	// provisioned slice, so such runners would leave every Instance they
	// reach without pods. Rejecting them leaves the IR as it was.
	if err := v.validateIRTPUSlices(ctx, oldObj, newObj); err != nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: %s",
			req.Namespace, req.Name, err.Error()))
	}

	return admission.Allowed("")
}

// isFinalizerOnlyUpdate reports whether the update changes nothing
// beyond metadata.finalizers. A finalizer-only patch cannot violate any
// invariant this webhook protects — spec immutability and controller-
// write provenance both concern content the patch leaves untouched —
// while refusing it can only manufacture teardown wedges: the webhook
// is fail-closed, and finalizer add/remove is exactly the write a
// controller must be able to make for deletion to complete.
//
// "Finalizer-only" is judged strictly:
//   - Spec must be deep-equal.
//   - Status must be deep-equal. With the status subresource enabled
//     the apiserver already resets status before validating admission
//     runs on main-resource updates (and /status updates never reach
//     this webhook, whose rule covers only the main resource), so this
//     check is inert today; it keeps the exemption airtight if the
//     subresource wiring ever changes.
//   - ObjectMeta must be deep-equal after overwriting, on a copy of the
//     old metadata, only Finalizers (the field under test) plus the
//     fields the apiserver itself rewrites on every update and clients
//     cannot use to smuggle state: ResourceVersion (storage-assigned,
//     optimistic concurrency enforced by the apiserver), ManagedFields
//     (server-side field tracking, rewritten on every write), and
//     Generation (apiserver-stamped; inert here given spec equality).
//
// Any other metadata delta — labels, annotations (including stripping
// the controller-write annotation), ownerReferences, deletion fields —
// disqualifies the update, which then falls through to the normal
// gates. Combining a finalizer change with any such edit is therefore
// not a bypass.
func isFinalizerOnlyUpdate(oldObj, newObj *v1beta1.InferenceReplica) bool {
	if !reflect.DeepEqual(oldObj.Spec, newObj.Spec) {
		return false
	}
	if !reflect.DeepEqual(oldObj.Status, newObj.Status) {
		return false
	}
	meta := oldObj.ObjectMeta.DeepCopy()
	meta.Finalizers = newObj.Finalizers
	meta.ResourceVersion = newObj.ResourceVersion
	meta.ManagedFields = newObj.ManagedFields
	meta.Generation = newObj.Generation
	return reflect.DeepEqual(*meta, newObj.ObjectMeta)
}

// validateIRAutoscaler runs the shared Autoscaler shape check against
// an IR's projected Autoscaler block. The IR spec carries its own
// Replicas field (not a ComponentExtensionSpec); we pass it as the
// effective MinReplicas floor for the KEDA idle-vs-min check. Calls
// validation.ValidateAutoscaler directly — the
// (*ComponentAutoscaler, *int) signature accepts this shape without
// synthesizing a parent ComponentExtensionSpec. Shared with the
// InferenceService and ServingRuntime webhooks.
func validateIRAutoscaler(ir *v1beta1.InferenceReplica) error {
	if ir == nil {
		return nil
	}
	var minPtr *int
	if ir.Spec.Replicas != nil {
		min := int(*ir.Spec.Replicas)
		minPtr = &min
	}
	return validation.ValidateAutoscaler(ir.Spec.Autoscaler, minPtr)
}

// validateIRPacing checks that Pacing.Partition <= effective Replicas.
// An over-Partition (Partition > Replicas) silently holds every
// Instance back forever — the rollout engine treats Partition as a
// per-index threshold and freezes any Instance with index < Partition,
// so Partition >= Replicas freezes the whole replica set. Matches the
// effective-Replicas default used by convert.desiredFromIR (nil or 0
// → 1).
func validateIRPacing(ir *v1beta1.InferenceReplica) error {
	if ir == nil || ir.Spec.Pacing == nil || ir.Spec.Pacing.Partition == nil {
		return nil
	}
	partition := *ir.Spec.Pacing.Partition
	// Match convert.desiredFromIR's defaulting: nil OR *r == 0 → 1.
	replicas := int32(1)
	if ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		replicas = *ir.Spec.Replicas
	}
	if partition > replicas {
		return fmt.Errorf(
			"spec.pacing.partition (%d) must be <= spec.replicas (%d); "+
				"an over-partition silently holds every Instance back forever",
			partition, replicas)
	}
	return nil
}

// validateIRRunnerVolumes checks every Runner's fully-rendered pod
// template so that each container/initContainer volumeMount references a
// volume declared in that same pod spec. The IR webhook sees the final
// rendered leader+worker templates, so this is the most precise place to
// catch a dangling mount before it reaches the apiserver at pod-create
// time (where it fails as "spec.containers[i].volumeMounts[j].name: Not
// found: <name>" and surfaces only as a buried reconcile error). Names
// the runner, container, and missing volume so the operator can fix the
// runtime spec — including the multi-node hint, since a dshm/shared
// volume must be declared under engineConfig.leader.volumes /
// engineConfig.worker.volumes for the leader+worker templates that mount
// it.
func validateIRRunnerVolumes(ir *v1beta1.InferenceReplica) error {
	if ir == nil {
		return nil
	}
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		spec := &runner.Template.Spec

		declared := make(map[string]struct{}, len(spec.Volumes))
		for j := range spec.Volumes {
			declared[spec.Volumes[j].Name] = struct{}{}
		}

		// initContainers and containers share the pod's volume set, so
		// validate both against the same declared map.
		if err := validateContainerVolumeMounts(
			string(runner.Name), spec.InitContainers, declared); err != nil {
			return err
		}
		if err := validateContainerVolumeMounts(
			string(runner.Name), spec.Containers, declared); err != nil {
			return err
		}
	}
	return nil
}

// validateContainerVolumeMounts verifies every volumeMount in the given
// containers resolves to a name in `declared`. Returns a message naming
// the runner, container, and missing volume on the first violation.
func validateContainerVolumeMounts(
	runnerName string, containers []corev1.Container, declared map[string]struct{},
) error {
	for c := range containers {
		container := &containers[c]
		for m := range container.VolumeMounts {
			name := container.VolumeMounts[m].Name
			if _, ok := declared[name]; !ok {
				return fmt.Errorf(
					"runner %q container %q: volumeMount %q has no matching "+
						"volume in the pod (for a multi-node runtime declare it "+
						"under engineConfig.leader.volumes / "+
						"engineConfig.worker.volumes)",
					runnerName, container.Name, name)
			}
		}
	}
	return nil
}

// validateIRTPUSlices rejects runners whose Instance the controller cannot
// place on a provisioned slice. An update is rejected only when it
// introduces the failure: one that keeps the runners, or whose previous
// runners fail too, is admitted. A failed node read admits the IR; the
// controller repeats the check before it creates pods.
func (v *Validator) validateIRTPUSlices(ctx context.Context, oldObj, newObj *v1beta1.InferenceReplica) error {
	if v.TPUSliceProvisioning == nil || v.Nodes == nil {
		return nil
	}
	if oldObj != nil && reflect.DeepEqual(oldObj.Spec.Runners, newObj.Spec.Runners) {
		return nil
	}
	err := v.invalidSliceDemand(ctx, newObj)
	if err == nil || (oldObj != nil && v.invalidSliceDemand(ctx, oldObj) != nil) {
		return nil
	}
	return err
}

// invalidSliceDemand returns why an Instance of ir cannot run on a
// provisioned slice. It returns nil when it can, when ir does not opt in,
// and when the nodes cannot be read.
func (v *Validator) invalidSliceDemand(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	if !slicesOptedIn(ir) {
		return nil
	}
	_, _, err := sliceprovision.Resolve(ctx, v.Nodes, v.TPUSliceProvisioning, instancePodSpecs(ir))
	if err != nil && !errors.Is(err, sliceprovision.ErrInvalidDemand) {
		log.Error(err, "Admitting InferenceReplica without checking its TPU slice demand",
			"namespace", ir.Namespace, "name", ir.Name)
		return nil
	}
	return err
}

// slicesOptedIn reports whether the controller places ir's pods on
// provisioned slices: the template of its default or leader runner opts in.
func slicesOptedIn(ir *v1beta1.InferenceReplica) bool {
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		if runner.Name == v1beta1.RunnerNameDefault || runner.Name == v1beta1.RunnerNameLeader {
			return runner.Template.Annotations[constants.TPUSliceProvisioningAnnotationKey] == "true"
		}
	}
	return false
}

// instancePodSpecs are the specs of one Instance's pods, one per pod, as
// the controller creates them: one from the default or leader runner's
// template, and Size from the worker runner's.
func instancePodSpecs(ir *v1beta1.InferenceReplica) []*corev1.PodSpec {
	var specs []*corev1.PodSpec
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		n := int32(1)
		if runner.Name == v1beta1.RunnerNameWorker {
			n = runner.Size
		}
		for ; n > 0; n-- {
			specs = append(specs, &runner.Template.Spec)
		}
	}
	return specs
}
