package servingruntime

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/validation"
)

var log = logf.Log.WithName(constants.ServingRuntimeValidatorWebhookName)

const (
	InvalidPriorityError                        = "same priority assigned for the model format %s"
	InvalidPriorityServingRuntimeError          = "%s in the servingruntimes %s and %s in namespace %s"
	InvalidPriorityClusterServingRuntimeError   = "%s in the clusterservingruntimes %s and %s"
	PriorityIsNotSameServingRuntimeError        = "%s under the servingruntime %s"
	PriorityIsNotSameClusterServingRuntimeError = "%s under the clusterservingruntime %s"
	ChainsawInjectAnnotationNotAllowError       = "chainsaw inject annotation is not allowed"
)

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-clusterservingruntime,mutating=false,failurePolicy=fail,groups=ome.io,resources=clusterservingruntimes,versions=v1beta1,name=clusterservingruntime.ome-webhook-server.validator

type ClusterServingRuntimeValidator struct {
	Client  client.Client
	Decoder admission.Decoder
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-servingruntime,mutating=false,failurePolicy=fail,groups=ome.io,resources=servingruntimes,versions=v1beta1,name=servingruntime.ome-webhook-server.validator

type ServingRuntimeValidator struct {
	Client  client.Client
	Decoder admission.Decoder
}

func (sr *ServingRuntimeValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	servingRuntime := &v1beta1.ServingRuntime{}
	if err := sr.Decoder.Decode(req, servingRuntime); err != nil {
		log.Error(err, "Failed to decode serving runtime", "name", servingRuntime.Name, "namespace", servingRuntime.Namespace)
		return admission.Errored(http.StatusBadRequest, err)
	}

	// OEP-0012: profile validation runs before the disabled-shortcut
	// below so a broken profile can't slip through "because it was
	// disabled anyway".
	if err := validateProfileMarker(servingRuntime.Annotations, servingRuntime.Spec.Disabled); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validateNamespacedRuntimeInheritance(ctx, sr.Client, servingRuntime); err != nil {
		return admission.Denied(err.Error())
	}

	existing := &v1beta1.ServingRuntimeList{}
	if err := sr.Client.List(context.TODO(), existing, client.InNamespace(servingRuntime.Namespace)); err != nil {
		log.Error(err, "Failed to get serving runtime list", "namespace", servingRuntime.Namespace)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if servingRuntime.Spec.IsDisabled() {
		return admission.Allowed("")
	}

	for i := range existing.Items {
		if err := validation.ValidateModelFormatPrioritySame(&servingRuntime.Spec); err != nil {
			return admission.Denied(fmt.Sprintf(PriorityIsNotSameServingRuntimeError, err.Error(), servingRuntime.Name))
		}
		if err := validateServingRuntimeAnnotations(&servingRuntime.Spec); err != nil {
			return admission.Denied(ChainsawInjectAnnotationNotAllowError)
		}
		if err := validateServingRuntimePriority(&servingRuntime.Spec, &existing.Items[i].Spec, servingRuntime.Name, existing.Items[i].Name); err != nil {
			return admission.Denied(fmt.Sprintf(InvalidPriorityServingRuntimeError, err.Error(), existing.Items[i].Name, servingRuntime.Name, servingRuntime.Namespace))
		}
	}
	return admission.Allowed("")
}

func (csr *ClusterServingRuntimeValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	clusterServingRuntime := &v1beta1.ClusterServingRuntime{}
	if err := csr.Decoder.Decode(req, clusterServingRuntime); err != nil {
		log.Error(err, "Failed to decode cluster serving runtime", "name", clusterServingRuntime.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := validateProfileMarker(clusterServingRuntime.Annotations, clusterServingRuntime.Spec.Disabled); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validateClusterRuntimeInheritance(ctx, csr.Client, clusterServingRuntime); err != nil {
		return admission.Denied(err.Error())
	}

	existing := &v1beta1.ClusterServingRuntimeList{}
	if err := csr.Client.List(context.TODO(), existing); err != nil {
		log.Error(err, "Failed to get cluster serving runtime list")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if clusterServingRuntime.Spec.IsDisabled() {
		return admission.Allowed("")
	}

	for i := range existing.Items {
		if err := validation.ValidateModelFormatPrioritySame(&clusterServingRuntime.Spec); err != nil {
			return admission.Denied(fmt.Sprintf(PriorityIsNotSameClusterServingRuntimeError, err.Error(), clusterServingRuntime.Name))
		}
		if err := validateServingRuntimeAnnotations(&clusterServingRuntime.Spec); err != nil {
			return admission.Denied(ChainsawInjectAnnotationNotAllowError)
		}
		// Pass the existing CSR's name from the loop as
		// existingRuntimeName so the `existingRuntimeName ==
		// newRuntimeName` early-exit in validateServingRuntimePriority
		// only short-circuits self-update (the loop iteration that hits
		// the same CSR we're admitting), not every cross-CSR pairing.
		if err := validateServingRuntimePriority(&clusterServingRuntime.Spec, &existing.Items[i].Spec, existing.Items[i].Name, clusterServingRuntime.Name); err != nil {
			return admission.Denied(fmt.Sprintf(InvalidPriorityClusterServingRuntimeError, err.Error(), existing.Items[i].Name, clusterServingRuntime.Name))
		}
	}
	return admission.Allowed("")
}

func areSupportedModelFormatsEqual(m1, m2 v1beta1.SupportedModelFormat) bool {
	return strings.EqualFold(m1.Name, m2.Name) &&
		ptrDerefEqual(m1.Version, m2.Version) &&
		ptrDerefEqual(m1.Quantization, m2.Quantization) &&
		ptrDerefEqual(m1.ModelFramework, m2.ModelFramework) &&
		ptrDerefEqual(m1.ModelFormat, m2.ModelFormat) &&
		ptrDerefEqual(m1.ModelArchitecture, m2.ModelArchitecture)
}

// ptrDerefEqual treats two nil pointers as equal and two non-nil
// pointers as equal iff their dereferenced values match. Mixed nil /
// non-nil ⇒ unequal. Avoids hand-rolling the same triple-clause check
// at every comparison site.
func ptrDerefEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func areModelSizeRangesEqual(range1, range2 *v1beta1.ModelSizeRangeSpec) bool {
	if range1 == nil || range2 == nil {
		return range1 == range2
	}
	return ptrDerefEqual(range1.Min, range2.Min) && ptrDerefEqual(range1.Max, range2.Max)
}

// validateServingRuntimeAnnotations is a placeholder for future
// pod-template annotation validation. Today it accepts everything; the
// caller still wraps the result so an enforcement path is available
// without webhook rewiring.
func validateServingRuntimeAnnotations(*v1beta1.ServingRuntimeSpec) error {
	return nil
}

// validateServingRuntimePriority rejects two runtimes that auto-select on
// the same (model format, size range) at the same priority — the runtime
// selector has no tiebreaker. Disabled runtimes and self-updates are
// skipped (the latter relies on existingRuntimeName == newRuntimeName).
func validateServingRuntimePriority(newSpec, existingSpec *v1beta1.ServingRuntimeSpec, existingRuntimeName, newRuntimeName string) error {
	if existingSpec.IsDisabled() || existingRuntimeName == newRuntimeName {
		return nil
	}
	if !anyProtocolOverlap(existingSpec.ProtocolVersions, newSpec.ProtocolVersions) {
		return nil
	}
	for _, existingModelFormat := range existingSpec.SupportedModelFormats {
		for _, newModelFormat := range newSpec.SupportedModelFormats {
			if !existingModelFormat.IsAutoSelectEnabled() || !newModelFormat.IsAutoSelectEnabled() {
				continue
			}
			if !areSupportedModelFormatsEqual(existingModelFormat, newModelFormat) {
				continue
			}
			if !areModelSizeRangesEqual(existingSpec.ModelSizeRange, newSpec.ModelSizeRange) {
				continue
			}
			if existingModelFormat.Priority != nil && newModelFormat.Priority != nil &&
				*existingModelFormat.Priority == *newModelFormat.Priority {
				return fmt.Errorf(InvalidPriorityError, newModelFormat.Name)
			}
		}
	}
	return nil
}

func anyProtocolOverlap(a, b []constants.InferenceServiceProtocol) bool {
	for _, p := range a {
		if contains(b, p) {
			return true
		}
	}
	return false
}

func contains[T comparable](slice []T, element T) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}
