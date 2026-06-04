// Cross-Component rollout coordination validation.
//
// Webhook-level admission rules for spec.rollout.coordination.groups[]:
// per-group structural checks (duplicate components, order coverage,
// policy-specific requirements) and cross-spec checks (Components in
// a group must declare OMENative deployment mode; RollingUpdate
// lockstep on update).
package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// Admission rejection reasons. Operators and the test plan reference
// these by name, so they're exported constants rather than
// fmt.Errorf strings buried in cases.
const (
	ReasonDuplicateComponentInCoordinationGroups  = "DuplicateComponentInCoordinationGroups"
	ReasonInvalidComponentInCoordinationGroup     = "InvalidComponentInCoordinationGroup"
	ReasonCoordinationRequiresOMENative           = "CoordinationRequiresOMENative"
	ReasonRatioToleranceTooHigh                   = "RatioToleranceTooHigh"
	ReasonOrphanCoordinationGroup                 = "OrphanCoordinationGroup"
	ReasonInvalidCoordinationOrder                = "InvalidCoordinationOrder"
	ReasonRollingUpdateRequiresMultipleComponents = "RollingUpdateRequiresMultipleComponents"
	ReasonRollingUpdateLockstepViolation          = "RollingUpdateLockstepViolation"
	ReasonSequentialRequiresOrder                 = "SequentialRequiresOrder"
	ReasonSequentialIncompleteOrder               = "SequentialIncompleteOrder"
	ReasonSequentialRequiresMultipleComponents    = "SequentialRequiresMultipleComponents"
	ReasonSequentialDuplicateOrder                = "SequentialDuplicateOrder"
)

// ratioToleranceWarningThreshold is the upper bound the validator
// considers operator-safe for the RatioBalanced pacing knob. Values
// above this trigger a non-blocking warning since they effectively
// disable ratio enforcement.
const ratioToleranceWarningThreshold int32 = 50

// validComponents is the closed set of Components allowed in a
// coordination group. Matches v1beta1.ComponentType enum.
var validComponents = map[v1beta1.ComponentType]struct{}{
	v1beta1.RouterComponent:  {},
	v1beta1.EngineComponent:  {},
	v1beta1.DecoderComponent: {},
}

// ValidateCoordination runs the spec.rollout.coordination.groups[]
// admission rules. Returns the first error encountered; nil on
// success.
//
// The validator does NOT do cross-update checks (RollingUpdate
// lockstep) here — those need the prior object to compare against.
// ValidateCoordinationUpdate is the entry point for those.
func ValidateCoordination(spec *v1beta1.InferenceServiceSpec) error {
	coord := spec.GetRolloutCoordination()
	if coord == nil {
		return nil
	}
	groups := coord.Groups
	if len(groups) == 0 {
		return nil
	}

	if err := validateGroupShapes(groups); err != nil {
		return err
	}
	if err := validateNoDuplicateMembership(groups); err != nil {
		return err
	}
	if err := validateOrphanGroups(spec, groups); err != nil {
		return err
	}
	if err := validateComponentsAreOMENative(spec, groups); err != nil {
		return err
	}
	if err := validatePacingCompatibility(groups); err != nil {
		return err
	}
	if err := validatePacingNotZeroBudget(groups); err != nil {
		return err
	}
	return nil
}

// ValidateCoordinationUpdate runs the update-time rules that compare
// old vs new spec — primarily RollingUpdate's "all-or-nothing image
// bump" lockstep. The full structural validation should be invoked
// separately on the new spec.
func ValidateCoordinationUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	newCoord := newSpec.GetRolloutCoordination()
	if newCoord == nil {
		return nil
	}
	for _, g := range newCoord.Groups {
		if g.Policy != v1beta1.CoordinationPolicyRollingUpdate {
			continue
		}
		if err := validateRollingUpdateLockstep(oldSpec, newSpec, g); err != nil {
			return err
		}
	}
	return nil
}

// CoordinationRatioToleranceWarning returns a warning string when any
// RatioBalanced group sets a ratio tolerance above the safe
// threshold. Empty when no warning applies.
func CoordinationRatioToleranceWarning(spec *v1beta1.InferenceServiceSpec) string {
	coord := spec.GetRolloutCoordination()
	if coord == nil {
		return ""
	}
	for i, g := range coord.Groups {
		if g.Pacing == nil || g.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
			continue
		}
		if g.Pacing.RatioTolerance == nil {
			continue
		}
		if *g.Pacing.RatioTolerance > ratioToleranceWarningThreshold {
			return fmt.Sprintf(
				"spec.rollout.coordination.groups[%d].pacing.ratioTolerance=%d effectively disables ratio enforcement (>%d) (%s)",
				i, *g.Pacing.RatioTolerance, ratioToleranceWarningThreshold, ReasonRatioToleranceTooHigh)
		}
	}
	return ""
}

func validateGroupShapes(groups []v1beta1.RolloutCoordinationGroup) error {
	for i, g := range groups {
		for _, c := range g.Components {
			if _, ok := validComponents[c]; !ok {
				return fmt.Errorf("spec.rollout.coordination.groups[%d]: component %q is not one of router/engine/decoder (%s)",
					i, c, ReasonInvalidComponentInCoordinationGroup)
			}
		}
		switch g.Policy {
		case v1beta1.CoordinationPolicySequential:
			if err := validateSequentialGroup(i, g); err != nil {
				return err
			}
		case v1beta1.CoordinationPolicyRollingUpdate:
			if len(g.Components) < 2 {
				return fmt.Errorf("spec.rollout.coordination.groups[%d]: RollingUpdate requires at least 2 components (%s)",
					i, ReasonRollingUpdateRequiresMultipleComponents)
			}
		case v1beta1.CoordinationPolicyBlueGreen:
			if err := validateOrderSubset(i, g.Components, g.Order); err != nil {
				return err
			}
		case v1beta1.CoordinationPolicyIndependent:
		default:
			// Empty / unknown policy — CRD enum validation will catch
			// the unknown case; treat empty as a noop here.
		}
	}
	return nil
}

func validateSequentialGroup(idx int, g v1beta1.RolloutCoordinationGroup) error {
	if len(g.Components) < 2 {
		return fmt.Errorf("spec.rollout.coordination.groups[%d]: Sequential requires at least 2 components (%s)",
			idx, ReasonSequentialRequiresMultipleComponents)
	}
	if len(g.Order) == 0 {
		return fmt.Errorf("spec.rollout.coordination.groups[%d]: Sequential requires order (%s)",
			idx, ReasonSequentialRequiresOrder)
	}
	if len(g.Order) != len(g.Components) {
		return fmt.Errorf("spec.rollout.coordination.groups[%d]: Sequential order must cover every component (%s)",
			idx, ReasonSequentialIncompleteOrder)
	}
	return validateOrderSubset(idx, g.Components, g.Order)
}

func validateOrderSubset(idx int, components, order []v1beta1.ComponentType) error {
	if len(order) == 0 {
		return nil
	}
	in := make(map[v1beta1.ComponentType]struct{}, len(components))
	for _, c := range components {
		in[c] = struct{}{}
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(order))
	for _, c := range order {
		if _, ok := in[c]; !ok {
			return fmt.Errorf("spec.rollout.coordination.groups[%d]: order entry %q is not in components (%s)",
				idx, c, ReasonInvalidCoordinationOrder)
		}
		if _, dup := seen[c]; dup {
			return fmt.Errorf("spec.rollout.coordination.groups[%d]: order entry %q appears more than once; each Component must walk the Sequential / BlueGreen order exactly once (%s)",
				idx, c, ReasonSequentialDuplicateOrder)
		}
		seen[c] = struct{}{}
	}
	return nil
}

func validateNoDuplicateMembership(groups []v1beta1.RolloutCoordinationGroup) error {
	seen := map[v1beta1.ComponentType]int{}
	for i, g := range groups {
		for _, c := range g.Components {
			if prev, ok := seen[c]; ok {
				return fmt.Errorf("spec.rollout.coordination.groups: component %q appears in groups[%d] and groups[%d] (%s)",
					c, prev, i, ReasonDuplicateComponentInCoordinationGroups)
			}
			seen[c] = i
		}
	}
	return nil
}

func validateOrphanGroups(spec *v1beta1.InferenceServiceSpec, groups []v1beta1.RolloutCoordinationGroup) error {
	declared := declaredComponents(spec)
	for i, g := range groups {
		anyDeclared := false
		for _, c := range g.Components {
			if _, ok := declared[c]; ok {
				anyDeclared = true
				break
			}
		}
		if !anyDeclared {
			return fmt.Errorf("spec.rollout.coordination.groups[%d]: no component in this group is declared on the InferenceService (%s)",
				i, ReasonOrphanCoordinationGroup)
		}
	}
	return nil
}

func validateComponentsAreOMENative(spec *v1beta1.InferenceServiceSpec, groups []v1beta1.RolloutCoordinationGroup) error {
	modeFor := omenativeDeploymentModeMap(spec)
	for i, g := range groups {
		for _, c := range g.Components {
			declared, ok := modeFor[c]
			if !ok {
				// Component isn't declared at all — handled by orphan check.
				continue
			}
			if declared != string(constants.OMENative) {
				return fmt.Errorf("spec.rollout.coordination.groups[%d]: component %q has deploymentMode=%q; coordination requires OMENative (%s)",
					i, c, declared, ReasonCoordinationRequiresOMENative)
			}
		}
	}
	return nil
}

// validatePacingCompatibility rejects coordination groups whose policy
// is incompatible with the declared pacing.type. The only invalid
// combination today is Sequential + RatioBalanced — Sequential
// serializes the rollout so only one Component is in flight at a time,
// leaving the cross-Component ratio that RatioBalanced is supposed to
// enforce undefined. Mirrors the runtime safety net in
// pkg/.../coordination/groups.go:251 (ValidateGroupShape).
//
// Reproduces W-1 in .claude/specs/2026-05-25-real-cluster-tests/README.md.
func validatePacingCompatibility(groups []v1beta1.RolloutCoordinationGroup) error {
	for i, g := range groups {
		if g.Policy != v1beta1.CoordinationPolicySequential {
			continue
		}
		if g.Pacing == nil {
			continue
		}
		if g.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
			return fmt.Errorf("spec.rollout.coordination.groups[%d]: Sequential cannot use RatioBalanced pacing — only one Component rolls at a time, so the cross-Component ratio is undefined (%s)",
				i, ReasonSequentialIncompatibleWithRatioBalanced)
		}
	}
	return nil
}

// validatePacingNotZeroBudget rejects PerComponent pacing blocks that
// explicitly set BOTH maxSurge=0 AND maxUnavailable=0. With both
// budgets at literal zero there is no surge headroom (no pod can be
// added) AND no drain headroom (no pod can be taken offline), so the
// rollout cannot make progress. Mirrors the runtime safety net in
// pkg/.../coordination/groups.go:266 (ValidateGroupShape).
//
// Only fires when BOTH values are explicitly set to literal 0 — nil
// (defaulted) values resolve to 25% via pacingWithDefaults and are not
// treated as zero here. The runtime defaulter check treats nil
// MaxUnavailable as zero because it post-dates this check; the webhook
// inspects the literal user input.
//
// Reproduces W-2 in .claude/specs/2026-05-25-real-cluster-tests/README.md.
func validatePacingNotZeroBudget(groups []v1beta1.RolloutCoordinationGroup) error {
	for i, g := range groups {
		if g.Pacing == nil {
			continue
		}
		// Only PerComponent pacing uses MaxSurge / MaxUnavailable as
		// per-Component budgets. RatioBalanced has its own ratio
		// tolerance knob. Type defaults to PerComponent when unset,
		// so empty Type counts as PerComponent here too.
		if g.Pacing.Type != "" && g.Pacing.Type != v1beta1.CoordinationPacingPerComponent {
			continue
		}
		if !explicitZeroIntOrString(g.Pacing.MaxSurge) || !explicitZeroIntOrString(g.Pacing.MaxUnavailable) {
			continue
		}
		return fmt.Errorf("spec.rollout.coordination.groups[%d]: maxSurge=0 AND maxUnavailable=0 deadlocks rollouts — set at least one non-zero (%s)",
			i, ReasonZeroBudgetPacingUnstartable)
	}
	return nil
}

// explicitZeroIntOrString reports whether the user explicitly set the
// IntOrString to a zero value (literal 0 or "0" or "0%"). Nil is not
// zero — nil means "use default" and the defaulter fills in 25%.
func explicitZeroIntOrString(v *intstr.IntOrString) bool {
	if v == nil {
		return false
	}
	if v.Type == intstr.Int {
		return v.IntValue() == 0
	}
	return v.StrVal == "0" || v.StrVal == "0%"
}

func validateRollingUpdateLockstep(oldSpec, newSpec *v1beta1.InferenceServiceSpec, g v1beta1.RolloutCoordinationGroup) error {
	oldImages := componentImages(oldSpec, g.Components)
	newImages := componentImages(newSpec, g.Components)
	anyChanged := false
	allChanged := true
	for _, c := range g.Components {
		o, n := oldImages[c], newImages[c]
		if o != n {
			anyChanged = true
		} else {
			allChanged = false
		}
	}
	if anyChanged && !allChanged {
		return fmt.Errorf("spec.rollout.coordination: RollingUpdate group requires all components to bump together (%s)",
			ReasonRollingUpdateLockstepViolation)
	}
	return nil
}

// declaredComponents returns the set of Components actually present on
// the InferenceService spec.
func declaredComponents(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]struct{} {
	out := map[v1beta1.ComponentType]struct{}{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil {
		out[v1beta1.EngineComponent] = struct{}{}
	}
	if spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = struct{}{}
	}
	if spec.Router != nil {
		out[v1beta1.RouterComponent] = struct{}{}
	}
	return out
}

// omenativeDeploymentModeMap returns Component → effective
// deploymentMode value for each Component on the spec. Effective value
// follows the resolution chain: per-Component annotation >
// spec.deploymentMode > "" (empty when neither is set). The shape-derived
// modes (MultiNode, PDDisaggregated) are intentionally NOT considered
// — coordination only cares about the dispatch backend, not the shape.
func omenativeDeploymentModeMap(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil {
		out[v1beta1.EngineComponent] = resolveComponentDeploymentMode(spec.Engine.Annotations, spec.DeploymentMode)
	}
	if spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = resolveComponentDeploymentMode(spec.Decoder.Annotations, spec.DeploymentMode)
	}
	if spec.Router != nil {
		out[v1beta1.RouterComponent] = resolveComponentDeploymentMode(spec.Router.Annotations, spec.DeploymentMode)
	}
	return out
}

// componentImages returns the container image per Component in
// `components`. The Runner field is the preferred image source (the
// OEP-0007 contract), but users may also specify the image via
// PodSpec.Containers[0].Image — both surfaces must produce the same
// lockstep signal so that an image bump via either path is detected.
//
// Precedence: Runner.Image if non-empty, otherwise the first container's
// image in PodSpec. Empty when neither is set or the Component spec is
// absent.
func componentImages(spec *v1beta1.InferenceServiceSpec, components []v1beta1.ComponentType) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	if spec == nil {
		return out
	}
	for _, c := range components {
		var img string
		switch c {
		case v1beta1.EngineComponent:
			if spec.Engine != nil {
				img = engineRunnerOrPodSpecImage(spec.Engine)
			}
		case v1beta1.DecoderComponent:
			if spec.Decoder != nil {
				img = decoderRunnerOrPodSpecImage(spec.Decoder)
			}
		case v1beta1.RouterComponent:
			if spec.Router != nil {
				img = routerRunnerOrPodSpecImage(spec.Router)
			}
		}
		if img != "" {
			out[c] = img
		}
	}
	return out
}

func engineRunnerOrPodSpecImage(e *v1beta1.EngineSpec) string {
	if e.Runner != nil && e.Runner.Image != "" {
		return e.Runner.Image
	}
	if len(e.PodSpec.Containers) > 0 {
		return e.PodSpec.Containers[0].Image
	}
	return ""
}

func decoderRunnerOrPodSpecImage(d *v1beta1.DecoderSpec) string {
	if d.Runner != nil && d.Runner.Image != "" {
		return d.Runner.Image
	}
	if len(d.PodSpec.Containers) > 0 {
		return d.PodSpec.Containers[0].Image
	}
	return ""
}

func routerRunnerOrPodSpecImage(r *v1beta1.RouterSpec) string {
	if r.Runner != nil && r.Runner.Image != "" {
		return r.Runner.Image
	}
	if len(r.PodSpec.Containers) > 0 {
		return r.PodSpec.Containers[0].Image
	}
	return ""
}
