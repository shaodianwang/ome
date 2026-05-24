// Cross-Component rollout coordination validation.
//
// Webhook-level admission rules for spec.rolloutCoordination.groups[]:
// per-group structural checks (duplicate components, order coverage,
// policy-specific requirements) and cross-spec checks (Components in
// a group must declare OMENative deployment mode; PairedMixable
// lockstep on update).
package validation

import (
	"fmt"

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
	ReasonPairedMixableRequiresMultipleComponents = "PairedMixableRequiresMultipleComponents"
	ReasonPairedMixableLockstepViolation          = "PairedMixableLockstepViolation"
	ReasonSequentialRequiresOrder                 = "SequentialRequiresOrder"
	ReasonSequentialIncompleteOrder               = "SequentialIncompleteOrder"
	ReasonSequentialRequiresMultipleComponents    = "SequentialRequiresMultipleComponents"
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

// ValidateCoordination runs the spec.rolloutCoordination.groups[]
// admission rules. Returns the first error encountered; nil on
// success.
//
// The validator does NOT do cross-update checks (PairedMixable
// lockstep) here — those need the prior object to compare against.
// ValidateCoordinationUpdate is the entry point for those.
func ValidateCoordination(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.RolloutCoordination == nil {
		return nil
	}
	groups := spec.RolloutCoordination.Groups
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
	return nil
}

// ValidateCoordinationUpdate runs the update-time rules that compare
// old vs new spec — primarily PairedMixable's "all-or-nothing image
// bump" lockstep. The full structural validation should be invoked
// separately on the new spec.
func ValidateCoordinationUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	if newSpec == nil || newSpec.RolloutCoordination == nil {
		return nil
	}
	for _, g := range newSpec.RolloutCoordination.Groups {
		if g.Policy != v1beta1.CoordinationPolicyPairedMixable {
			continue
		}
		if err := validatePairedMixableLockstep(oldSpec, newSpec, g); err != nil {
			return err
		}
	}
	return nil
}

// CoordinationRatioToleranceWarning returns a warning string when any
// RatioBalanced group sets a ratio tolerance above the safe
// threshold. Empty when no warning applies.
func CoordinationRatioToleranceWarning(spec *v1beta1.InferenceServiceSpec) string {
	if spec == nil || spec.RolloutCoordination == nil {
		return ""
	}
	for i, g := range spec.RolloutCoordination.Groups {
		if g.Pacing == nil || g.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
			continue
		}
		if g.Pacing.RatioTolerance == nil {
			continue
		}
		if *g.Pacing.RatioTolerance > ratioToleranceWarningThreshold {
			return fmt.Sprintf(
				"spec.rolloutCoordination.groups[%d].pacing.ratioTolerance=%d effectively disables ratio enforcement (>%d) (%s)",
				i, *g.Pacing.RatioTolerance, ratioToleranceWarningThreshold, ReasonRatioToleranceTooHigh)
		}
	}
	return ""
}

func validateGroupShapes(groups []v1beta1.RolloutCoordinationGroup) error {
	for i, g := range groups {
		for _, c := range g.Components {
			if _, ok := validComponents[c]; !ok {
				return fmt.Errorf("spec.rolloutCoordination.groups[%d]: component %q is not one of router/engine/decoder (%s)",
					i, c, ReasonInvalidComponentInCoordinationGroup)
			}
		}
		switch g.Policy {
		case v1beta1.CoordinationPolicySequential:
			if err := validateSequentialGroup(i, g); err != nil {
				return err
			}
		case v1beta1.CoordinationPolicyPairedMixable:
			if len(g.Components) < 2 {
				return fmt.Errorf("spec.rolloutCoordination.groups[%d]: PairedMixable requires at least 2 components (%s)",
					i, ReasonPairedMixableRequiresMultipleComponents)
			}
		case v1beta1.CoordinationPolicyAtomic:
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
		return fmt.Errorf("spec.rolloutCoordination.groups[%d]: Sequential requires at least 2 components (%s)",
			idx, ReasonSequentialRequiresMultipleComponents)
	}
	if len(g.Order) == 0 {
		return fmt.Errorf("spec.rolloutCoordination.groups[%d]: Sequential requires order (%s)",
			idx, ReasonSequentialRequiresOrder)
	}
	if len(g.Order) != len(g.Components) {
		return fmt.Errorf("spec.rolloutCoordination.groups[%d]: Sequential order must cover every component (%s)",
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
	for _, c := range order {
		if _, ok := in[c]; !ok {
			return fmt.Errorf("spec.rolloutCoordination.groups[%d]: order entry %q is not in components (%s)",
				idx, c, ReasonInvalidCoordinationOrder)
		}
	}
	return nil
}

func validateNoDuplicateMembership(groups []v1beta1.RolloutCoordinationGroup) error {
	seen := map[v1beta1.ComponentType]int{}
	for i, g := range groups {
		for _, c := range g.Components {
			if prev, ok := seen[c]; ok {
				return fmt.Errorf("spec.rolloutCoordination.groups: component %q appears in groups[%d] and groups[%d] (%s)",
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
			return fmt.Errorf("spec.rolloutCoordination.groups[%d]: no component in this group is declared on the InferenceService (%s)",
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
				return fmt.Errorf("spec.rolloutCoordination.groups[%d]: component %q has deploymentMode=%q; coordination requires OMENative (%s)",
					i, c, declared, ReasonCoordinationRequiresOMENative)
			}
		}
	}
	return nil
}

func validatePairedMixableLockstep(oldSpec, newSpec *v1beta1.InferenceServiceSpec, g v1beta1.RolloutCoordinationGroup) error {
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
		return fmt.Errorf("spec.rolloutCoordination: PairedMixable group requires all components to bump together (%s)",
			ReasonPairedMixableLockstepViolation)
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

// omenativeDeploymentModeMap returns Component → declared
// deploymentMode annotation value for each Component on the spec.
// Empty string when the annotation is absent.
func omenativeDeploymentModeMap(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil {
		out[v1beta1.EngineComponent] = spec.Engine.Annotations[constants.DeploymentMode]
	}
	if spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = spec.Decoder.Annotations[constants.DeploymentMode]
	}
	if spec.Router != nil {
		out[v1beta1.RouterComponent] = spec.Router.Annotations[constants.DeploymentMode]
	}
	return out
}

// componentImages returns the runner container image per Component
// in `components` from the spec. Empty when the Component or its
// runner is absent.
func componentImages(spec *v1beta1.InferenceServiceSpec, components []v1beta1.ComponentType) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	if spec == nil {
		return out
	}
	for _, c := range components {
		switch c {
		case v1beta1.EngineComponent:
			if spec.Engine != nil && spec.Engine.Runner != nil {
				out[c] = spec.Engine.Runner.Image
			}
		case v1beta1.DecoderComponent:
			if spec.Decoder != nil && spec.Decoder.Runner != nil {
				out[c] = spec.Decoder.Runner.Image
			}
		case v1beta1.RouterComponent:
			if spec.Router != nil && spec.Router.Runner != nil {
				out[c] = spec.Router.Runner.Image
			}
		}
	}
	return out
}
