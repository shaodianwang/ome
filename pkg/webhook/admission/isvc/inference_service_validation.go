package isvc

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	"github.com/ome-projects/ome/pkg/utils/storage"
	"github.com/ome-projects/ome/pkg/validation"
)

var (
	validatorLogger = logf.Log.WithName("inferenceservice-v1beta1-validation-webhook")
)

// InferenceServiceValidator is responsible for validating the InferenceService resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type InferenceServiceValidator struct {
	Client          client.Client
	RuntimeSelector runtimeselector.Selector

	// KnownPassthroughPrefixes lists the ome.io/* pass-through
	// annotation prefixes the active Gateway-implementation translator
	// supports (e.g. ["ome.io/btp."] when Envoy Gateway is loaded).
	// When non-empty, the validator rejects pass-through annotations
	// whose prefix is not in the set with `UnsupportedPassthrough`,
	// surfacing the misconfiguration at admission instead of letting
	// the controller catch it at runtime via the
	// BackendPolicyUnsupportedFields condition.
	//
	// Empty (default) keeps the prior permissive behavior — all
	// documented pass-through prefixes are accepted. Production
	// (cmd/manager/main.go) populates this from the active translator's
	// SupportedPassthroughPrefixes(); test wiring may leave it empty
	// unless explicitly exercising the rejection path.
	KnownPassthroughPrefixes []string
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-inferenceservice,mutating=false,failurePolicy=fail,groups=ome.io,resources=inferenceservices,versions=v1beta1,name=inferenceservice.ome-webhook-server.validator
var _ webhook.CustomValidator = &InferenceServiceValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (v *InferenceServiceValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	isvc, err := convertToInferenceService(obj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
	validatorLogger.Info("validate create", "name", isvc.Name)
	return v.validateInferenceService(ctx, isvc)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (v *InferenceServiceValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	isvc, err := convertToInferenceService(newObj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
	validatorLogger.Info("validate update", "name", isvc.Name)
	return v.validateInferenceService(ctx, isvc)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (v *InferenceServiceValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	isvc, err := convertToInferenceService(obj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
	validatorLogger.Info("validate delete", "name", isvc.Name)
	return nil, nil
}

// GetIntReference returns the pointer for the integer input
func GetIntReference(number int) *int {
	num := number
	return &num
}

func (v *InferenceServiceValidator) validateInferenceService(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	var allWarnings admission.Warnings

	if err := validation.ValidateInferenceServiceName(isvc.Name); err != nil {
		return allWarnings, err
	}

	warnings, err := validation.ValidateAutoscalerConfig(isvc)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, warnings...)

	if err := validation.ValidateAutoscalerTargetUtilizationPercentage(isvc); err != nil {
		return allWarnings, err
	}

	if err := validation.ValidateEngineDecoderConfig(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	if err := validation.ValidateLeaderWorkerPairing(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	if err := validation.ValidateEngineDecoderDeploymentMode(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// OEP-0011: validate spec.traffic (typed core: LB algorithm, hash
	// key, endpoint override).
	if err := validation.ValidateTrafficSpec(isvc.Spec.Traffic); err != nil {
		return allWarnings, err
	}

	// OEP-0011: validate ome.io/* traffic annotations (parsing,
	// Did-you-mean, cross-rules).
	trafficWarnings, err := validation.ValidateTrafficAnnotations(isvc.Annotations, isvc.Spec.Traffic, v.KnownPassthroughPrefixes...)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, trafficWarnings...)

	// OEP-0011: validate spec.traffic.rollout (multi-step plan + mutual
	// exclusivity with the simple canaryTrafficPercent form).
	if err := validation.ValidateRollout(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// OEP-0011: range-check each Component's CanaryTrafficPercent and
	// enforce PD coupling (engine.canaryTrafficPercent ==
	// decoder.canaryTrafficPercent).
	if err := validation.ValidateCanaryPercent(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// Validate that referenced model exists (for new Engine architecture using isvc.Spec.Model)
	if err := v.validateModelExists(ctx, isvc); err != nil {
		return allWarnings, err
	}

	if err := v.validateOverlays(isvc); err != nil {
		return allWarnings, err
	}

	// Validate runtime and model resolution for new architecture
	if isvc.Spec.Engine != nil {
		warnings, err := v.validateRuntimeAndModelResolution(ctx, isvc)
		if err != nil {
			return allWarnings, err
		}
		allWarnings = append(allWarnings, warnings...)
	}
	return allWarnings, nil
}

// Convert runtime.Object into InferenceService
func convertToInferenceService(obj runtime.Object) (*v1beta1.InferenceService, error) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil, fmt.Errorf("expected an InferenceService object but got %T", obj)
	}
	return isvc, nil
}

// validateModelExists validates that the referenced model (BaseModel or ClusterBaseModel) exists
func (v *InferenceServiceValidator) validateModelExists(ctx context.Context, isvc *v1beta1.InferenceService) error {
	// Check new architecture model reference (isvc.Spec.Model)
	if isvc.Spec.Model != nil && isvc.Spec.Model.Name != "" {
		spec, meta, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
		if err != nil {
			return fmt.Errorf("referenced model %q not found in namespace %q: ensure a BaseModel exists in this namespace or a ClusterBaseModel exists cluster-wide with this name",
				isvc.Spec.Model.Name, isvc.Namespace)
		}
		if err := validateClusterBaseModelPVCNamespace(spec, meta, isvc); err != nil {
			return err
		}
		// Each overlay's BaseModel must also be reachable. Cross-namespace
		// PVC rules apply per-overlay since each gets its own mount.
		if isvc.Spec.Model != nil {
			for _, ov := range isvc.Spec.Model.Overlays {
				ovSpec, ovMeta, err := isvcutils.GetBaseModel(v.Client, ov.Name, isvc.Namespace)
				if err != nil {
					return fmt.Errorf("overlay %q not found in namespace %q: %w", ov.Name, isvc.Namespace, err)
				}
				if err := validateClusterBaseModelPVCNamespace(ovSpec, ovMeta, isvc); err != nil {
					return fmt.Errorf("overlay %q: %w", ov.Name, err)
				}
			}
		}
	}

	return nil
}

// validateClusterBaseModelPVCNamespace rejects InferenceServices that
// reference a ClusterBaseModel whose PVC URI points to a namespace other
// than the InferenceService's own namespace. Kubernetes does not allow a
// pod to mount a PVC from a different namespace, so without this check the
// generated ISVC pods would fail at scheduling with a confusing
// "persistentvolumeclaim ... not found" error and no upstream signal.
//
// Skipped for namespaced BaseModel (already same-namespace by construction)
// and for non-PVC URIs.
func validateClusterBaseModelPVCNamespace(spec *v1beta1.BaseModelSpec, meta *metav1.ObjectMeta, isvc *v1beta1.InferenceService) error {
	if meta == nil || meta.Namespace != "" {
		// namespaced BaseModel — same-namespace by construction
		return nil
	}
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return nil
	}
	uri := *spec.Storage.StorageUri
	storageType, err := storage.GetStorageType(uri)
	if err != nil || storageType != storage.StorageTypePVC {
		return nil
	}
	components, err := storage.ParsePVCStorageURI(uri)
	if err != nil || components.Namespace == "" || components.Namespace == isvc.Namespace {
		return nil
	}
	return fmt.Errorf(
		"ClusterBaseModel %q references PVC in namespace %q, but InferenceService is in namespace %q; "+
			"Kubernetes does not allow pods to mount PVCs from another namespace. "+
			"Either move the InferenceService to namespace %q or replicate the PVC into namespace %q.",
		isvc.Spec.Model.Name, components.Namespace, isvc.Namespace, components.Namespace, isvc.Namespace)
}

// validateOverlays rejects overlay sets that would produce a broken
// pod spec: duplicate Name, env-var name collision after sanitization
// (foo-bar and foo_bar both → FOO_BAR), or self-reference.
// Per-overlay existence + runtime compatibility live in
// validateRuntimeAndModelResolution.
func (v *InferenceServiceValidator) validateOverlays(isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil || len(isvc.Spec.Model.Overlays) == 0 {
		return nil
	}
	primary := isvc.Spec.Model.Name
	seenName := map[string]bool{}
	seenEnv := map[string]string{} // sanitized → original name
	for _, ov := range isvc.Spec.Model.Overlays {
		if ov.Name == "" {
			return fmt.Errorf("overlay name cannot be empty")
		}
		if ov.Name == primary {
			return fmt.Errorf("overlay %q must not reference the primary model of the same kind/apiGroup", ov.Name)
		}
		if seenName[ov.Name] {
			return fmt.Errorf("overlay %q is declared more than once", ov.Name)
		}
		seenName[ov.Name] = true

		envName := isvcutils.OverlayEnvVarName(ov.Name)
		if prev, ok := seenEnv[envName]; ok {
			return fmt.Errorf("overlays %q and %q both sanitize to env var %s; rename one (hyphens and underscores collapse to the same form)",
				prev, ov.Name, envName)
		}
		seenEnv[envName] = ov.Name
	}
	return nil
}

// validateRuntimeAndModelResolution validates runtime and model resolution for new architecture
func (v *InferenceServiceValidator) validateRuntimeAndModelResolution(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	var warnings admission.Warnings

	// Only validate new architecture if Engine is specified (focusing on new spec)
	if isvc.Spec.Engine == nil {
		return warnings, nil
	}

	if isvc.Spec.Model == nil {
		if isvc.Spec.Runtime == nil && !hasFullRunnerConfig(isvc.Spec.Engine) {
			return warnings, fmt.Errorf("model reference is required when runtime is not specified and engine does not have complete runner configuration")
		}
		return warnings, nil
	}

	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		return v.resolveModelAndRuntime(ctx, isvc, warnings)
	}

	if !hasFullRunnerConfig(isvc.Spec.Engine) {
		return v.resolveModelAndRuntime(ctx, isvc, warnings)
	}

	return warnings, nil
}

// resolveModelAndRuntime performs actual model and runtime resolution
func (v *InferenceServiceValidator) resolveModelAndRuntime(ctx context.Context, isvc *v1beta1.InferenceService, warnings admission.Warnings) (admission.Warnings, error) {
	// Resolve model using the new architecture approach
	baseModel, _, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return warnings, fmt.Errorf("failed to resolve model %s: %w", isvc.Spec.Model.Name, err)
	}

	// Validate model is not disabled
	if baseModel.Disabled != nil && *baseModel.Disabled {
		return warnings, fmt.Errorf("model %s is disabled", isvc.Spec.Model.Name)
	}

	// Check runtime selection/validation
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		// Validate specified runtime
		if err := v.RuntimeSelector.ValidateRuntime(ctx, isvc.Spec.Runtime.Name, baseModel, isvc); err != nil {
			return warnings, fmt.Errorf("runtime %s does not support model %s: %w",
				isvc.Spec.Runtime.Name, isvc.Spec.Model.Name, err)
		}
		warnings = append(warnings, fmt.Sprintf("Runtime %s is valid for model %s",
			isvc.Spec.Runtime.Name, isvc.Spec.Model.Name))
	} else {
		// Check if runtime can be auto-selected
		selection, err := v.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
		if err != nil {
			return warnings, fmt.Errorf("no supporting runtime found for model %s and engine does not have complete runner configuration: %w", isvc.Spec.Model.Name, err)
		}
		// Success - runtime will be auto-selected
		warnings = append(warnings, fmt.Sprintf("Runtime %s will be auto-selected for model %s",
			selection.Name, isvc.Spec.Model.Name))
	}
	return warnings, nil
}

// hasFullRunnerConfig checks if the engine has complete runner configuration
func hasFullRunnerConfig(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}

	// Check if main runner is defined with required fields
	if engine.Runner != nil && engine.Runner.Image != "" {
		return true
	}

	// Check if both leader and worker runners are defined for multi-node
	if engine.Leader != nil && engine.Worker != nil {
		leaderHasRunner := engine.Leader.Runner != nil && engine.Leader.Runner.Image != ""
		workerHasRunner := engine.Worker.Runner != nil && engine.Worker.Runner.Image != ""
		return leaderHasRunner && workerHasRunner
	}

	return false
}
