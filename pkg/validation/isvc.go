package validation

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// OEP-0007 multi-node validation reason constants. Operators and the
// test plan (.claude/specs/2026-05-27-multi-node-real-cluster-tests
// §M-E) reference these by name, so they're exported and stable.
const (
	// ReasonInvalidLeaderWorkerPairing is the legacy umbrella reason
	// for any leader/worker shape mismatch. Retained for back-compat
	// with operators scraping the error string; new errors also embed
	// the more-specific reason below.
	ReasonInvalidLeaderWorkerPairing = "InvalidLeaderWorkerPairing"

	// ReasonWorkerSizeMustBePositive rejects worker.size=0 (or nil)
	// when a leader is declared. The "default 0 means single-pod"
	// fallback is ambiguous — force the operator to either omit
	// Worker entirely (single-pod with default Runner) or set Size >= 1.
	ReasonWorkerSizeMustBePositive = "WorkerSizeMustBePositive"

	// ReasonLeaderRequiresWorker rejects leader-only configs. A leader
	// without a worker has no gang to lead and is meaningless for
	// tensor parallelism.
	ReasonLeaderRequiresWorker = "LeaderRequiresWorker"

	// ReasonWorkerRequiresLeader rejects worker-only configs.
	// Symmetric to LeaderRequiresWorker; covered by the same
	// validator function.
	ReasonWorkerRequiresLeader = "WorkerRequiresLeader"

	// ReasonLeaderSizeMustBeOne rejects user attempts to scale the
	// leader Runner above 1. OEP-0007 fixes leader at size=1: the
	// OME_LEADER_ADDRESS contract resolves to a single
	// <isvc>-<comp>-<inst>-leader-0 FQDN, so multiple leaders would
	// produce ambiguous DNS. LeaderSpec has no explicit Size field
	// today (enforced by type); this validator is a forward-looking
	// guard in case the API surface evolves.
	ReasonLeaderSizeMustBeOne = "LeaderSizeMustBeOne"

	// ReasonSequentialMultiPodRequiresOrder rejects Sequential
	// coordination groups containing a multi-pod Component without an
	// Order pin. Order is REQUIRED for Sequential semantics per
	// OEP-0007.1, but easy to forget for multi-pod variants where the
	// operator might assume the controller picks an order.
	ReasonSequentialMultiPodRequiresOrder = "SequentialMultiPodRequiresOrder"

	// ReasonMultiPodLeanModelNeedsRunner rejects lean (no spec.model,
	// no spec.runtime) ISVCs that declare a multi-pod Component
	// without populating both leader.runner.image and
	// worker.runner.image. The lean path is supported per #249, but
	// multi-pod lean specs cannot be filled in by runtime selection
	// (no runtime) or model parsing (no model), so the operator must
	// supply images explicitly.
	ReasonMultiPodLeanModelNeedsRunner = "MultiPodLeanModelNeedsRunner"

	// ReasonSequentialIncompatibleWithRatioBalanced rejects Sequential
	// coordination groups that also declare RatioBalanced pacing. The
	// two concepts are mutually exclusive: Sequential serializes the
	// rollout so only one Component is in flight at a time, leaving the
	// cross-Component ratio that RatioBalanced is supposed to enforce
	// undefined. The runtime ValidateGroupShape catches it as a safety
	// net (see pkg/.../coordination/groups.go:251), this webhook reason
	// surfaces the conflict at admission so operators don't get silent
	// pacing downgrades after the object lands. Reproduces W-1 in
	// .claude/specs/2026-05-25-real-cluster-tests/README.md.
	ReasonSequentialIncompatibleWithRatioBalanced = "SequentialIncompatibleWithRatioBalanced"

	// ReasonZeroBudgetPacingUnstartable rejects PerComponent pacing
	// blocks that set BOTH maxSurge=0 AND maxUnavailable=0. With both
	// budgets at literal zero there is no surge headroom (no pod can be
	// added) AND no drain headroom (no pod can be taken offline), so
	// the rollout has no way to make progress and the Component will
	// never finish reconciling. Defaulted nil values are not zero —
	// the defaulter fills 25% for both — so this reason only fires when
	// the operator has explicitly set both to 0. Mirrored as a runtime
	// safety net in pkg/.../coordination/groups.go:266. Reproduces W-2
	// in .claude/specs/2026-05-25-real-cluster-tests/README.md.
	ReasonZeroBudgetPacingUnstartable = "ZeroBudgetPacingUnstartable"
)

func ValidateInferenceServiceName(name string) error {
	if !IsvcNameRegex.MatchString(name) {
		return fmt.Errorf("invalid InferenceService name %q, must match %q", name, IsvcNameFmt)
	}
	return nil
}

func ValidateEngineDecoderConfig(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Decoder != nil && spec.Engine == nil {
		return fmt.Errorf("decoder cannot be specified without engine")
	}
	return nil
}

// ValidateLeaderWorkerPairing enforces that the Leader / Worker spec
// pair on each Component (Engine, Decoder) is internally consistent:
// either both are absent (single-pod) or both are present with
// Worker.Size > 0 (multi-pod). Rejects orphan-leader, orphan-worker, and
// declared-worker-with-no-positive-size configurations.
//
// Error strings embed both the legacy "InvalidLeaderWorkerPairing"
// reason (for back-compat with operators / tests grepping for it) and
// a more-specific reason from the OEP-0007 reason constants.
func ValidateLeaderWorkerPairing(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine != nil {
		if err := validateLeaderWorkerPairing("engine", spec.Engine.Leader, spec.Engine.Worker); err != nil {
			return err
		}
	}
	if spec.Decoder != nil {
		if err := validateLeaderWorkerPairing("decoder", spec.Decoder.Leader, spec.Decoder.Worker); err != nil {
			return err
		}
	}
	return nil
}

func validateLeaderWorkerPairing(component string, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if leader == nil && worker == nil {
		return nil
	}
	if leader != nil && worker == nil {
		return fmt.Errorf(
			"%s: %s.leader is set but %s.worker is not; multi-pod configurations require both (%s)",
			ReasonInvalidLeaderWorkerPairing, component, component, ReasonLeaderRequiresWorker,
		)
	}
	if leader == nil && worker != nil {
		return fmt.Errorf(
			"%s: %s.worker is set but %s.leader is not; multi-pod configurations require both (%s)",
			ReasonInvalidLeaderWorkerPairing, component, component, ReasonWorkerRequiresLeader,
		)
	}
	if worker.Size == nil || *worker.Size <= 0 {
		return fmt.Errorf(
			"%s: %s.worker is set but worker.size is not a positive integer; multi-pod configurations require worker.size > 0 (%s)",
			ReasonInvalidLeaderWorkerPairing, component, ReasonWorkerSizeMustBePositive,
		)
	}
	return nil
}

// ValidateLeaderSize enforces that no Component sets a leader Runner
// size greater than 1. OEP-0007 fixes the leader at size=1: the
// OME_LEADER_ADDRESS env var that workers (and the leader itself)
// consult resolves to <isvc>-<comp>-<inst>-leader-0; a second leader
// pod would shadow the DNS contract and break tensor-parallel
// initialization.
//
// LeaderSpec has no explicit Size field today, so this validator is
// structurally a no-op against the current API. It exists as a
// forward-looking guard so that any future API evolution adding such
// a field automatically inherits the rejection, with the stable
// reason string (%s) already in place for operators and tests.
//
// Implementation note: when LeaderSpec gains a Size field, replace
// the per-component no-op with a check that returns the formatted
// error using ReasonLeaderSizeMustBeOne.
func ValidateLeaderSize(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	// Engine leader: structurally fixed at size=1 — no field to inspect.
	if err := validateLeaderSize("engine", spec.Engine); err != nil {
		return err
	}
	// Decoder leader: same.
	if err := validateLeaderSize("decoder", spec.Decoder); err != nil {
		return err
	}
	return nil
}

// validateLeaderSize is the per-Component shape check. LeaderSpec has
// no Size field today; the function is a no-op placeholder so the
// public ValidateLeaderSize entry point exists and tests can pin the
// reason constant. See ValidateLeaderSize doc for the upgrade plan.
func validateLeaderSize(component string, spec interface{}) error {
	// No-op against the current API surface. See the doc comment on
	// ValidateLeaderSize.
	_ = component
	_ = spec
	return nil
}

// ValidateSequentialMultiPodOrder enforces that any Sequential
// coordination group containing a multi-pod Component declares an
// explicit Order over its Components. OEP-0007.1 already mandates
// Order for Sequential groups (covered by validateSequentialGroup in
// coordination.go), but this validator adds a louder reason for the
// multi-pod variant where the controller's gang semantics make the
// missing Order especially dangerous: without it, the rollout would
// have no deterministic boundary between gang rollouts.
//
// This is layered on top of (not in place of) the existing
// SequentialRequiresOrder check — the latter already rejects the
// missing-Order case; this validator surfaces the multi-pod-specific
// reason so operators see the relevant OEP context in the error.
//
// Returns nil when:
//   - spec.rolloutCoordination is absent or has no groups
//   - no Sequential group contains a multi-pod Component
//   - every multi-pod-containing Sequential group has Order set
func ValidateSequentialMultiPodOrder(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.RolloutCoordination == nil {
		return nil
	}
	multiPod := multiPodComponents(spec)
	for i, g := range spec.RolloutCoordination.Groups {
		if g.Policy != v1beta1.CoordinationPolicySequential {
			continue
		}
		if len(g.Order) > 0 {
			continue
		}
		// Sequential with no Order — flag if any member Component is multi-pod.
		for _, c := range g.Components {
			if _, ok := multiPod[c]; !ok {
				continue
			}
			return fmt.Errorf(
				"spec.rolloutCoordination.groups[%d]: Sequential policy with multi-pod component %q requires explicit Order; the controller has no default gang-rollout sequencing for multi-pod components (%s)",
				i, c, ReasonSequentialMultiPodRequiresOrder,
			)
		}
	}
	return nil
}

// ValidateMultiPodLeanRunner enforces that a lean (no spec.model AND
// no spec.runtime) InferenceService declaring a multi-pod Component
// (Engine or Decoder) populates both leader.runner.image and
// worker.runner.image. Per #249, lean ISVCs are supported for the
// single-pod path because the operator supplies a single Runner
// image. For multi-pod the controller renders distinct PodSpecs for
// leader and worker — without a runtime or model to fill in defaults,
// both Runner images must be explicit.
//
// Single-pod lean ISVCs continue to use the existing
// hasFullRunnerConfig path (runner.image satisfies the requirement).
// This validator only fires when MultiPod is true.
func ValidateMultiPodLeanRunner(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	modelSet := spec.Model != nil && spec.Model.Name != ""
	runtimeSet := spec.Runtime != nil && spec.Runtime.Name != ""
	if modelSet || runtimeSet {
		return nil
	}
	// Lean path: at least one of model/runtime must end up set per
	// ValidateInferenceService elsewhere; if both are absent the user
	// is in the "spec.engine carries the runner config end-to-end"
	// path. Multi-pod variants in that path need both leader and
	// worker runner images present.
	if err := validateMultiPodLeanRunnerImages(
		"engine",
		engineLeader(spec.Engine), engineWorker(spec.Engine),
	); err != nil {
		return err
	}
	if err := validateMultiPodLeanRunnerImages(
		"decoder",
		decoderLeader(spec.Decoder), decoderWorker(spec.Decoder),
	); err != nil {
		return err
	}
	return nil
}

func validateMultiPodLeanRunnerImages(component string, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if leader == nil || worker == nil {
		// Single-pod or invalid pair (covered by
		// ValidateLeaderWorkerPairing).
		return nil
	}
	leaderImg := ""
	if leader.Runner != nil {
		leaderImg = leader.Runner.Image
	}
	workerImg := ""
	if worker.Runner != nil {
		workerImg = worker.Runner.Image
	}
	if leaderImg != "" && workerImg != "" {
		return nil
	}
	return fmt.Errorf(
		"%s: %s is multi-pod with no spec.model and no spec.runtime, but %s.leader.runner.image (%q) or %s.worker.runner.image (%q) is empty; multi-pod lean specs must declare both runner images",
		ReasonMultiPodLeanModelNeedsRunner,
		component, component, leaderImg, component, workerImg,
	)
}

func engineLeader(e *v1beta1.EngineSpec) *v1beta1.LeaderSpec {
	if e == nil {
		return nil
	}
	return e.Leader
}

func engineWorker(e *v1beta1.EngineSpec) *v1beta1.WorkerSpec {
	if e == nil {
		return nil
	}
	return e.Worker
}

func decoderLeader(d *v1beta1.DecoderSpec) *v1beta1.LeaderSpec {
	if d == nil {
		return nil
	}
	return d.Leader
}

func decoderWorker(d *v1beta1.DecoderSpec) *v1beta1.WorkerSpec {
	if d == nil {
		return nil
	}
	return d.Worker
}

// multiPodComponents returns the set of Components declared as multi-pod
// (leader + worker with worker.size > 0). Mirrors the controller-side
// MultiPod detection in pkg/controller/v1beta1/inferenceservice/components.
func multiPodComponents(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]struct{} {
	out := map[v1beta1.ComponentType]struct{}{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil && spec.Engine.Leader != nil && spec.Engine.Worker != nil &&
		spec.Engine.Worker.Size != nil && *spec.Engine.Worker.Size > 0 {
		out[v1beta1.EngineComponent] = struct{}{}
	}
	if spec.Decoder != nil && spec.Decoder.Leader != nil && spec.Decoder.Worker != nil &&
		spec.Decoder.Worker.Size != nil && *spec.Decoder.Worker.Size > 0 {
		out[v1beta1.DecoderComponent] = struct{}{}
	}
	return out
}

// ValidateEngineDecoderDeploymentMode enforces that when both Engine and
// Decoder are present and either carries ome.io/deploymentMode=OMENative,
// both components carry the same value for that annotation.
func ValidateEngineDecoderDeploymentMode(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine == nil || spec.Decoder == nil {
		return nil
	}
	engineMode := spec.Engine.Annotations[constants.DeploymentMode]
	decoderMode := spec.Decoder.Annotations[constants.DeploymentMode]
	if engineMode != string(constants.OMENative) && decoderMode != string(constants.OMENative) {
		return nil
	}
	if engineMode != decoderMode {
		return fmt.Errorf(
			"InvalidDeploymentModeCombination: engine %s=%q does not match decoder %s=%q; when either is OMENative, both must use the same value",
			constants.DeploymentMode, engineMode,
			constants.DeploymentMode, decoderMode,
		)
	}
	return nil
}

func ValidateReplicaBounds(min, max *int) error {
	if min != nil && *min < 0 {
		return fmt.Errorf("minReplicas must be >= 0")
	}
	if max != nil && *max < 1 {
		return fmt.Errorf("maxReplicas must be >= 1")
	}
	if min != nil && max != nil && *min > *max {
		return fmt.Errorf("minReplicas (%d) must be <= maxReplicas (%d)", *min, *max)
	}
	return nil
}

// ValidateScaleToZero rejects InferenceServices that explicitly set any
// Component's MinReplicas=0 unless autoscalerClass=keda is configured.
//
// Before Serverless mode was removed, MinReplicas=0 auto-promoted the
// engine to Knative-Serving (which natively supports scale-to-zero).
// After removal, MinReplicas=0 falls through to RawDeployment and the
// Deployment sits at zero replicas (broken). Operators needing
// scale-to-zero must opt into KEDA's idleReplicaCount via the
// autoscalerClass annotation.
func ValidateScaleToZero(isvc *v1beta1.InferenceService) error {
	hasZero := false
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.MinReplicas != nil && *isvc.Spec.Engine.MinReplicas == 0 {
		hasZero = true
	}
	if isvc.Spec.Decoder != nil && isvc.Spec.Decoder.MinReplicas != nil && *isvc.Spec.Decoder.MinReplicas == 0 {
		hasZero = true
	}
	if isvc.Spec.Router != nil && isvc.Spec.Router.MinReplicas != nil && *isvc.Spec.Router.MinReplicas == 0 {
		hasZero = true
	}
	if !hasZero {
		return nil
	}
	annotations := isvc.ObjectMeta.Annotations
	if constants.AutoscalerClassType(annotations[constants.AutoscalerClass]) == constants.AutoscalerClassKEDA {
		return nil
	}
	return fmt.Errorf(
		"InvalidScaleToZero: MinReplicas=0 requires %s=%s annotation (Serverless mode no longer auto-promotes scale-to-zero)",
		constants.AutoscalerClass, constants.AutoscalerClassKEDA,
	)
}

// ValidateAutoscalerConfig validates autoscaler-related annotations and spec
// fields. It returns warnings (non-fatal messages) and an error if validation
// fails.
func ValidateAutoscalerConfig(isvc *v1beta1.InferenceService) ([]string, error) {
	annotations := isvc.ObjectMeta.Annotations
	value, ok := annotations[constants.AutoscalerClass]
	class := constants.AutoscalerClassType(value)
	if ok {
		for _, item := range constants.AutoscalerAllowedClassList {
			if class == item {
				switch class {
				case constants.AutoscalerClassHPA:
					if metric, ok := annotations[constants.AutoscalerMetrics]; ok {
						return nil, ValidateHPAMetrics(v1beta1.ScaleMetric(metric))
					}
					return nil, nil
				case constants.AutoscalerClassKEDA:
					return ValidateKEDAConfig(isvc)
				case constants.AutoscalerClassExternal:
					return nil, nil
				default:
					return nil, fmt.Errorf("unknown autoscaler class [%s]", class)
				}
			}
		}
		return nil, fmt.Errorf("[%s] is not a supported autoscaler class type", value)
	}
	return nil, nil
}

func ValidateAutoscalerTargetUtilizationPercentage(isvc *v1beta1.InferenceService) error {
	annotations := isvc.ObjectMeta.Annotations
	if value, ok := annotations[constants.TargetUtilizationPercentage]; ok {
		t, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
		if t < 1 || t > 100 {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
	}
	return nil
}

func ValidateHPAMetrics(metric v1beta1.ScaleMetric) error {
	for _, item := range constants.AutoscalerAllowedMetricsList {
		if item == constants.AutoscalerMetricsType(metric) {
			return nil
		}
	}
	return fmt.Errorf("[%s] is not a supported metric", metric)
}

var kedaValidScalingOperators = []string{
	"GreaterThan",
	"GreaterThanOrEqual",
	"LessThan",
	"LessThanOrEqual",
}

var kedaValidAuthModes = []string{
	"basic",
	"tls",
	"bearer",
	"custom",
}

func ValidateKEDAConfig(isvc *v1beta1.InferenceService) ([]string, error) {
	var warnings []string
	kedaConfig := isvc.Spec.KedaConfig
	annotations := isvc.ObjectMeta.Annotations

	if kedaConfig != nil && kedaConfig.ScalingOperator != "" {
		if err := ValidateKEDAScalingOperator(kedaConfig.ScalingOperator); err != nil {
			return warnings, err
		}
	}

	if operatorAnnotation, ok := annotations[constants.KedaScalingOperator]; ok {
		if err := ValidateKEDAScalingOperator(operatorAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.ScalingThreshold != "" {
		if err := ValidateKEDAScalingThreshold(kedaConfig.ScalingThreshold); err != nil {
			return warnings, err
		}
	}

	if thresholdAnnotation, ok := annotations[constants.KedaScalingThreshold]; ok {
		if err := ValidateKEDAScalingThreshold(thresholdAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.PromServerAddress != "" {
		if err := ValidateKEDAPrometheusServerAddress(kedaConfig.PromServerAddress); err != nil {
			return warnings, err
		}
	}

	if promAddrAnnotation, ok := annotations[constants.KedaPrometheusServerAddress]; ok {
		if err := ValidateKEDAPrometheusServerAddress(promAddrAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.AuthModes != "" {
		if err := ValidateKEDAAuthModes(kedaConfig.AuthModes); err != nil {
			return warnings, err
		}
		if kedaConfig.AuthenticationRef == nil {
			warnings = append(warnings, "KEDA authModes is specified but authenticationRef is not set; authModes will be ignored by KEDA")
		}
	}

	return warnings, nil
}

func ValidateKEDAScalingOperator(operator string) error {
	for _, valid := range kedaValidScalingOperators {
		if operator == valid {
			return nil
		}
	}
	return fmt.Errorf("invalid KEDA scaling operator %q, must be one of: %s", operator, strings.Join(kedaValidScalingOperators, ", "))
}

func ValidateKEDAScalingThreshold(threshold string) error {
	_, err := strconv.ParseFloat(threshold, 64)
	if err != nil {
		return fmt.Errorf("invalid KEDA scaling threshold %q: must be a valid number", threshold)
	}
	return nil
}

func ValidateKEDAPrometheusServerAddress(address string) error {
	parsedURL, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: %v", address, err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: scheme must be http or https", address)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: host is required", address)
	}
	return nil
}

func ValidateKEDAAuthModes(authModes string) error {
	modes := strings.Split(authModes, ",")
	for _, mode := range modes {
		mode = strings.TrimSpace(mode)
		if mode == "" {
			continue
		}
		valid := false
		for _, validMode := range kedaValidAuthModes {
			if mode == validMode {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid KEDA auth mode %q, must be one of: %s", mode, strings.Join(kedaValidAuthModes, ", "))
		}
	}
	return nil
}
