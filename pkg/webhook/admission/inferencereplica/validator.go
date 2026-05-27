// Package inferencereplica implements the validating admission webhook
// for the InferenceReplica CRD. InferenceReplica is a controller-only
// resource (see design doc §"Access model — controller-only resource"):
// users only ever write InferenceService, and the ISVC controller is the
// sole writer of InferenceReplica specs.
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
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

var log = logf.Log.WithName("inferencereplica-validation-webhook")

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-inferencereplica,mutating=false,failurePolicy=fail,groups=ome.io,resources=inferencereplicas,versions=v1beta1,name=inferencereplica.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1
// +kubebuilder:object:generate=false

// Validator is the ValidatingAdmissionWebhook handler for InferenceReplica.
// It rejects user-driven writes by gating on the controller-write
// annotation; the ISVC controller stamps the annotation on every
// Create / Update it issues, so its own writes pass through unchanged.
type Validator struct {
	Decoder admission.Decoder
}

// hasControllerWriteAnnotation returns true when the annotation is set
// to the literal "true". An empty or absent value rejects.
func hasControllerWriteAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[constants.InferenceReplicaControllerWriteAnnotationKey] ==
		constants.InferenceReplicaControllerWriteAnnotationVal
}

// denyDirectWrite is the canonical rejection message. Centralized so
// reviewers can grep for it in audit logs.
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

// Handle implements admission.Handler.
//
// Rejection rules:
//
//   - Create / Update without the controller-write annotation on the
//     INCOMING object → Denied. The ISVC controller stamps the annotation
//     on every write it issues; absence implies a non-controller actor.
//
//   - Update where the spec changed in any way without the annotation on
//     the new object → Denied. This is a defensive duplicate of the rule
//     above (the annotation check happens first) but keeps the failure
//     mode clear if a future change loosens the top-level gate.
//
//   - Status edits via the main resource path do NOT reach this webhook —
//     the status subresource is on a separate admission path. We do not
//     register on /status here. The kubebuilder marker covers the main
//     resource only.
//
// Allowed:
//
//   - Any write carrying ome.io/controller-write=true.
//
//   - Delete (not registered in the kubebuilder verbs list above).
func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	// Only Create and Update reach this handler per the webhook config.
	// Defensive: if a future operator misconfigures the rule list, we
	// still want predictable behavior.
	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
		// fall through
	default:
		return admission.Allowed("")
	}

	newObj := &v1beta1.InferenceReplica{}
	if err := v.Decoder.Decode(req, newObj); err != nil {
		log.Error(err, "Failed to decode InferenceReplica",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Top-level gate: every controller write stamps the annotation.
	// Absence ⇒ a non-controller actor.
	if !hasControllerWriteAnnotation(newObj.Annotations) {
		log.Info("Rejecting direct write to InferenceReplica",
			"op", req.Operation,
			"namespace", req.Namespace, "name", req.Name,
			"userInfo", req.UserInfo.Username)
		return denyDirectWrite(req.Operation, req.Namespace, req.Name)
	}

	// On Update, check that the immutable spec fields (parentRef,
	// component) did not change. The controller never rewrites them
	// either, so changes here indicate a malformed reconcile or a hand
	// edit that somehow carried the annotation through.
	if req.Operation == admissionv1.Update {
		oldObj := &v1beta1.InferenceReplica{}
		if err := v.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
			log.Error(err, "Failed to decode old InferenceReplica",
				"namespace", req.Namespace, "name", req.Name)
			return admission.Errored(http.StatusBadRequest, err)
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

	return admission.Allowed("")
}
