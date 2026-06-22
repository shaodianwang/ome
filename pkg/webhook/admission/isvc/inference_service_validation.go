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
	"github.com/ome-projects/ome/pkg/runtimerevision"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	"github.com/ome-projects/ome/pkg/utils/storage"
	"github.com/ome-projects/ome/pkg/validation"
)

var validatorLogger = logf.Log.WithName("inferenceservice-v1beta1-validation-webhook")

// InferenceServiceValidator wires the webhook framework into the
// per-rule validators in pkg/validation. The kubebuilder markers below
// suppress DeepCopy + OpenAPI codegen since this is a stateless
// wired-once helper, not a persisted CR.
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

func (v *InferenceServiceValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	isvc, err := convertToInferenceService(obj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
	if err := validateLegacyAutoscalerFieldsFromCtx(ctx); err != nil {
		return nil, err
	}
	return v.validateInferenceService(ctx, isvc)
}

func (v *InferenceServiceValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	isvc, err := convertToInferenceService(newObj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
	oldIsvc, err := convertToInferenceService(oldObj)
	if err != nil {
		validatorLogger.Error(err, "Unable to convert prior object to InferenceService")
		return nil, err
	}
	if err := validateLegacyAutoscalerFieldsFromCtx(ctx); err != nil {
		return nil, err
	}
	if err := validation.ValidateCoordinationUpdate(&oldIsvc.Spec, &isvc.Spec); err != nil {
		return nil, err
	}
	return v.validateInferenceService(ctx, isvc)
}

// validateLegacyAutoscalerFieldsFromCtx pulls the raw admission request
// out of ctx and runs the defense-in-depth check for the deleted
// scaleTarget / scaleMetric fields. Silently no-ops when the request
// cannot be retrieved (e.g., when the validator is invoked from a unit
// test that doesn't stamp the ctx), so it never blocks tests that only
// feed a typed object.
func validateLegacyAutoscalerFieldsFromCtx(ctx context.Context) error {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil
	}
	return validation.ValidateLegacyAutoscalerFieldsRaw(req.AdmissionRequest.Object.Raw)
}

func (v *InferenceServiceValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	if _, err := convertToInferenceService(obj); err != nil {
		validatorLogger.Error(err, "Unable to convert object to InferenceService")
		return nil, err
	}
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

	autoscalerWarnings, err := validation.ValidateAutoscalerConfig(isvc)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, autoscalerWarnings...)

	// Per-Component Autoscaler block shape validation (OEP-0013):
	// class enum, required fields, IdleReplicaCount vs MinReplicas, plus
	// annotation/block conflict. The per-Component dispatch is shared
	// with the ServingRuntime and InferenceReplica webhooks via
	// validation.ValidateComponentsAutoscalers — see review §4 in
	// .claude/specs/2026-05-30-code-quality-review/07-webhooks.md.
	if err := validation.ValidateComponentsAutoscalers(isvcAutoscalerChecks(isvc)); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateAutoscalerAnnotationConflict(isvc); err != nil {
		return allWarnings, err
	}

	if err := validation.ValidateAutoscalerTargetUtilizationPercentage(isvc); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateEngineDecoderConfig(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateLeaderWorkerPairing(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	// OEP-0007 Phase 3: forward-looking guard that the leader Runner stays
	// at size=1 (today enforced structurally by LeaderSpec having no Size
	// field; see ValidateLeaderSize).
	if err := validation.ValidateLeaderSize(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	// OEP-0007 Phase 3: lean (no-model, no-runtime) multi-pod specs must
	// declare both leader.runner.image and worker.runner.image — the
	// controller has no way to fill them in without a runtime or model.
	if err := validation.ValidateMultiPodLeanRunner(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateEngineDecoderDeploymentMode(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateScaleToZero(isvc); err != nil {
		return allWarnings, err
	}

	// MigrationPolicy bounds: ProgressDeadlineSeconds >= 30,
	// MaxAutoRetries within [0, 10]. Auto-migration is opt-in (nil
	// fields pass through) but configured values must stay within
	// sane bounds so a misconfigured pod can't thrash migrations.
	if err := validation.ValidateMigrationPolicy(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// Traffic block: typed core load-balancing config + ome.io/* annotations.
	if err := validation.ValidateTrafficSpec(isvc.Spec.Traffic); err != nil {
		return allWarnings, err
	}
	trafficWarnings, err := validation.ValidateTrafficAnnotations(isvc.Annotations, isvc.Spec.Traffic, v.KnownPassthroughPrefixes...)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, trafficWarnings...)

	// spec.rollout.canary: validate the progressive-traffic rollout plan.
	if err := validation.ValidateCanary(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// Cross-Component rollout coordination: structural + RatioBalanced
	// tolerance warning.
	if err := validation.ValidateCoordination(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if warning := validation.CoordinationRatioToleranceWarning(&isvc.Spec); warning != "" {
		allWarnings = append(allWarnings, warning)
	}

	// Per-Component LifecycleSpec.UpdateStrategy.RollingUpdate semantic
	// validation: percent strings must be "<n>%" with 0 <= n <= 100;
	// integer values must be >= 0. The CRD XIntOrString marker handles
	// the structural "must be int or string" check; this validator adds
	// the range / format check.
	if err := validation.ValidateLifecycle(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// At least one of spec.model or spec.runtime must be set. When
	// spec.model is omitted (lean path), spec.runtime must name the
	// runtime to use — the controller skips model-fetch and runtime
	// auto-selection in that mode.
	modelSet := isvc.Spec.Model != nil && isvc.Spec.Model.Name != ""
	runtimeSet := isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != ""
	if !modelSet && !runtimeSet {
		return allWarnings, fmt.Errorf("at least one of spec.model or spec.runtime must be set")
	}

	if err := validateRuntimePin(isvc); err != nil {
		return allWarnings, err
	}
	if err := v.validateModelExists(ctx, isvc); err != nil {
		return allWarnings, err
	}
	if err := v.validateOverlays(isvc); err != nil {
		return allWarnings, err
	}

	if isvc.Spec.Engine != nil {
		resWarnings, err := v.validateRuntimeAndModelResolution(ctx, isvc)
		if err != nil {
			return allWarnings, err
		}
		allWarnings = append(allWarnings, resWarnings...)
	}
	return allWarnings, nil
}

func convertToInferenceService(obj runtime.Object) (*v1beta1.InferenceService, error) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil, fmt.Errorf("expected an InferenceService object but got %T", obj)
	}
	return isvc, nil
}

// validateModelExists ensures the referenced BaseModel/ClusterBaseModel
// (and each overlay) is reachable in the ISVC's namespace, and that
// cross-namespace PVC rules pass per-overlay.
func (v *InferenceServiceValidator) validateModelExists(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil
	}
	spec, meta, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return fmt.Errorf("referenced model %q not found in namespace %q: ensure a BaseModel exists in this namespace or a ClusterBaseModel exists cluster-wide with this name",
			isvc.Spec.Model.Name, isvc.Namespace)
	}
	if err := validateClusterBaseModelPVCNamespace(spec, meta, isvc); err != nil {
		return err
	}
	for _, ov := range isvc.Spec.Model.Overlays {
		ovSpec, ovMeta, err := isvcutils.GetBaseModel(v.Client, ov.Name, isvc.Namespace)
		if err != nil {
			return fmt.Errorf("overlay %q not found in namespace %q: %w", ov.Name, isvc.Namespace, err)
		}
		if err := validateClusterBaseModelPVCNamespace(ovSpec, ovMeta, isvc); err != nil {
			return fmt.Errorf("overlay %q: %w", ov.Name, err)
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
	// namespaced BaseModel ⇒ same-namespace by construction; skip
	if meta == nil || meta.Namespace != "" {
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

// validateRuntimeAndModelResolution gates runtime/model resolution on
// the Engine-architecture path. Lean specs (full runner config + no
// model) short-circuit; everything else needs the runtime selector.
func (v *InferenceServiceValidator) validateRuntimeAndModelResolution(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	var warnings admission.Warnings

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

func (v *InferenceServiceValidator) resolveModelAndRuntime(ctx context.Context, isvc *v1beta1.InferenceService, warnings admission.Warnings) (admission.Warnings, error) {
	baseModel, _, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return warnings, fmt.Errorf("failed to resolve model %s: %w", isvc.Spec.Model.Name, err)
	}
	if baseModel.Disabled != nil && *baseModel.Disabled {
		return warnings, fmt.Errorf("model %s is disabled", isvc.Spec.Model.Name)
	}

	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		if err := v.RuntimeSelector.ValidateRuntime(ctx, isvc.Spec.Runtime.Name, baseModel, isvc); err != nil {
			// The operator named this runtime explicitly, so OME should
			// not block on the runtime's *declared* supportedModelFormats:
			// a generic runtime (e.g. sglang) can serve many architectures
			// it never enumerates. Downgrade a pure compatibility mismatch
			// (format / architecture / framework) to an advisory warning
			// and admit — the deliberate choice wins over the declaration.
			//
			// Everything else stays a hard error:
			//   - runtime not found / disabled / malformed model: the
			//     runtime genuinely cannot run, so admitting would only
			//     produce a broken ISVC.
			//   - a sharded (Distribution=Sharded) model with no configured
			//     cache provider: this surfaces as a RuntimeCompatibilityError
			//     too, but it is a real cluster-config blocker — a sharded
			//     model physically cannot load without a cache provider — not
			//     an over-conservative declared-format check. Keep it gating.
			if runtimeselector.IsRuntimeCompatibilityError(err) && !modelRequiresCacheProvider(baseModel) {
				warnings = append(warnings, fmt.Sprintf(
					"runtime %q does not declare support for model %q (%v); proceeding because the runtime was named explicitly",
					isvc.Spec.Runtime.Name, isvc.Spec.Model.Name, err))
				return warnings, nil
			}
			return warnings, fmt.Errorf("runtime %s does not support model %s: %w",
				isvc.Spec.Runtime.Name, isvc.Spec.Model.Name, err)
		}
		// Success is the common case and emits nothing — a bare "is valid"
		// admission Warning showed up as `Warning:` in kubectl and read as
		// if something were wrong.
		return warnings, nil
	}
	selection, err := v.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
	if err != nil {
		return warnings, fmt.Errorf("no supporting runtime found for model %s and engine does not have complete runner configuration: %w", isvc.Spec.Model.Name, err)
	}
	warnings = append(warnings, fmt.Sprintf("Runtime %s will be auto-selected for model %s",
		selection.Name, isvc.Spec.Model.Name))
	return warnings, nil
}

// validateRuntimePin rejects malformed explicit ControllerRevision pins
// at admission. The full existence check is left to reconcile (the
// webhook can't reliably reach across cluster scope), but the cheap
// shape checks happen here:
//
//   - spec.runtime.revision is meaningful only when autoSync is false;
//     setting it with autoSync=true (default) is a confused-user
//     signal worth surfacing immediately.
//   - The revision name must conform to the convention for the named
//     runtime's kind + scope; an obviously-wrong name (e.g., pinning
//     to runtime A but naming a revision of runtime B) is rejected
//     before the controller ever sees it.
func validateRuntimePin(isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Revision == nil || *isvc.Spec.Runtime.Revision == "" {
		return nil
	}
	if isvc.Spec.Runtime.AutoSync == nil || *isvc.Spec.Runtime.AutoSync {
		return fmt.Errorf(
			"spec.runtime.revision requires spec.runtime.autoSync=false; AutoSync=true would silently ignore the pin")
	}
	kind := runtimerevision.KindClusterServingRuntime
	sourceNS := ""
	if isvc.Spec.Runtime.Kind != nil && *isvc.Spec.Runtime.Kind == string(runtimerevision.KindServingRuntime) {
		kind = runtimerevision.KindServingRuntime
		sourceNS = isvc.Namespace
	}
	revName := *isvc.Spec.Runtime.Revision
	if !runtimerevision.MatchesRuntime(revName, kind, sourceNS, isvc.Spec.Runtime.Name) {
		return fmt.Errorf(
			"spec.runtime.revision %q does not match the expected naming convention for runtime %q (%s); "+
				"expected the form %s",
			revName, isvc.Spec.Runtime.Name, kind, expectedPinPattern(kind, sourceNS, isvc.Spec.Runtime.Name))
	}
	return nil
}

func expectedPinPattern(kind runtimerevision.SourceKind, sourceNS, runtimeName string) string {
	switch kind {
	case runtimerevision.KindClusterServingRuntime:
		return fmt.Sprintf("cr-%s-<8 lowercase hex chars>", runtimeName)
	case runtimerevision.KindServingRuntime:
		return fmt.Sprintf("r-%s-%s-<8 lowercase hex chars>", sourceNS, runtimeName)
	default:
		return "<unknown kind>"
	}
}

// isvcAutoscalerChecks projects each declared Component (Engine,
// Decoder, Router) into the per-Component slice consumed by
// validation.ValidateComponentsAutoscalers. Nil Components are skipped
// so an absent Component never produces a spurious "<name>:" error.
func isvcAutoscalerChecks(isvc *v1beta1.InferenceService) []validation.ComponentAutoscalerCheck {
	var checks []validation.ComponentAutoscalerCheck
	if isvc.Spec.Engine != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "engine",
			Autoscaler:  isvc.Spec.Engine.Autoscaler,
			MinReplicas: isvc.Spec.Engine.MinReplicas,
		})
	}
	if isvc.Spec.Decoder != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "decoder",
			Autoscaler:  isvc.Spec.Decoder.Autoscaler,
			MinReplicas: isvc.Spec.Decoder.MinReplicas,
		})
	}
	if isvc.Spec.Router != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "router",
			Autoscaler:  isvc.Spec.Router.Autoscaler,
			MinReplicas: isvc.Spec.Router.MinReplicas,
		})
	}
	return checks
}

// modelRequiresCacheProvider reports whether the model is sharded
// (Distribution=Sharded) and therefore needs a configured cluster cache
// provider to load. A sharded model that names a runtime explicitly must
// still hard-fail when no provider is configured — it physically cannot
// load — so resolveModelAndRuntime keeps gating that case instead of
// downgrading it to an advisory warning. Mirrors the (unexported)
// runtimeselector helper of the same name.
func modelRequiresCacheProvider(model *v1beta1.BaseModelSpec) bool {
	return model != nil &&
		model.Distribution != nil &&
		*model.Distribution == v1beta1.DistributionSharded
}

// hasFullRunnerConfig reports whether the engine has enough runner
// configuration to skip runtime selection: either a top-level Runner
// with image, or matching Leader.Runner + Worker.Runner images (multi-node).
func hasFullRunnerConfig(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	if engine.Runner != nil && engine.Runner.Image != "" {
		return true
	}
	if engine.Leader != nil && engine.Worker != nil {
		leaderHasRunner := engine.Leader.Runner != nil && engine.Leader.Runner.Image != ""
		workerHasRunner := engine.Worker.Runner != nil && engine.Worker.Runner.Image != ""
		return leaderHasRunner && workerHasRunner
	}
	return false
}
