// Package runtimerevision implements an immutability validator on
// apps/v1.ControllerRevisions: any Update against a revision that
// OME wrote (identified by the ome.io/created-by=ome-controller
// annotation) is rejected. Revisions OME didn't create
// (StatefulSet/DaemonSet-owned) pass through unchanged.
package runtimerevision

import (
	"context"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/constants"
)

// +kubebuilder:webhook:verbs=update,path=/validate-apps-v1-controllerrevision,mutating=false,failurePolicy=fail,groups="apps",resources=controllerrevisions,versions=v1,name=controllerrevision-immutability.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1

var log = logf.Log.WithName("controllerrevision-immutability")

type ImmutabilityValidator struct {
	Decoder admission.Decoder
}

func (v *ImmutabilityValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Update {
		return admission.Allowed("")
	}
	oldObj := &appsv1.ControllerRevision{}
	if err := v.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
		log.Error(err, "decode old ControllerRevision")
		return admission.Errored(http.StatusBadRequest, err)
	}
	// Not an OME-written revision → pass through (StatefulSet /
	// DaemonSet manage their own).
	if oldObj.Annotations[constants.RuntimeRevisionCreatedByKey] != constants.RuntimeRevisionCreatedByOMEValue {
		return admission.Allowed("")
	}
	return admission.Denied(fmt.Sprintf(
		"ControllerRevision %s/%s is owned by OME and immutable; create a new revision instead of updating",
		oldObj.Namespace, oldObj.Name))
}
