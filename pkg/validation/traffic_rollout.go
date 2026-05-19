// OEP-0011 rollout validation.
//
// Webhook-level validation for spec.traffic.rollout (step-based
// progressive rollout plans) and its mutual exclusivity with the
// existing per-Component ComponentExtensionSpec.CanaryTrafficPercent.
// CRD enum tags enforce per-field types (TrafficWeight in [0,100],
// at least one step, etc.); this file adds the cross-step and
// cross-field rules the webhook owns.
package validation

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ValidateRollout runs the step-list and cross-field validation for
// spec.traffic.rollout. Returns nil when the rollout block is unset
// or internally consistent.
func ValidateRollout(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.Traffic == nil || spec.Traffic.Rollout == nil {
		return nil
	}

	// Per-step validation.
	for i, step := range spec.Traffic.Rollout.Steps {
		if err := validateRolloutStepCapacity(i, "newCapacity", step.NewCapacity); err != nil {
			return err
		}
		if step.OldCapacity != nil {
			if err := validateRolloutStepCapacity(i, "oldCapacity", *step.OldCapacity); err != nil {
				return err
			}
		}
		// Pause.Duration is metav1.Duration which already enforces
		// parseability at JSON decode; no extra check.
	}

	// Cross-step: the last step must reach 100% traffic, otherwise the
	// rollout never completes.
	steps := spec.Traffic.Rollout.Steps
	if len(steps) == 0 {
		// Kubebuilder marker MinItems=1 normally catches this at CRD
		// validation; the explicit guard avoids an index-out-of-range
		// panic in callers that bypass the apiserver (e.g. unit tests
		// or library consumers).
		return fmt.Errorf("spec.traffic.rollout.steps must contain at least one step (EmptyRolloutSteps)")
	}
	last := steps[len(steps)-1]
	if last.TrafficWeight != 100 {
		return fmt.Errorf(
			"spec.traffic.rollout.steps[%d].trafficWeight = %d; the final step must set trafficWeight=100 so the rollout completes (IncompleteRolloutPlan)",
			len(steps)-1, last.TrafficWeight)
	}

	// Mutual exclusivity with the simple per-Component form.
	if err := validateMixedRolloutForms(spec); err != nil {
		return err
	}

	return nil
}

// validateRolloutStepCapacity coerces an IntOrString capacity value
// and rejects out-of-range numbers. Percent strings ("25%") must lie
// in [0%, 200%] (200 allows capacity surge above MinReplicas). Int
// values must be >= 0.
func validateRolloutStepCapacity(stepIdx int, fieldName string, v intstr.IntOrString) error {
	switch v.Type {
	case intstr.Int:
		if v.IntValue() < 0 {
			return fmt.Errorf("spec.traffic.rollout.steps[%d].%s = %d; must be >= 0 (InvalidStepCapacity)",
				stepIdx, fieldName, v.IntValue())
		}
	case intstr.String:
		s := v.StrVal
		if !strings.HasSuffix(s, "%") {
			return fmt.Errorf("spec.traffic.rollout.steps[%d].%s = %q; string values must be a percentage like \"25%%\" (InvalidStepCapacity)",
				stepIdx, fieldName, s)
		}
		n, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
		if err != nil {
			return fmt.Errorf("spec.traffic.rollout.steps[%d].%s = %q; not a valid percentage (InvalidStepCapacity): %w",
				stepIdx, fieldName, s, err)
		}
		// Upper bound of 200% explicitly allows capacity surge
		// (e.g. newCapacity=100% AND oldCapacity=100% during a
		// blue-green validation phase).
		if n < 0 || n > 200 {
			return fmt.Errorf("spec.traffic.rollout.steps[%d].%s = %q; percentage must be in [0%%, 200%%] (InvalidStepCapacity)",
				stepIdx, fieldName, s)
		}
	default:
		return fmt.Errorf("spec.traffic.rollout.steps[%d].%s has unsupported IntOrString type %d (InvalidStepCapacity)",
			stepIdx, fieldName, v.Type)
	}
	return nil
}

// validateMixedRolloutForms enforces the mutual exclusivity between
// spec.traffic.rollout (declarative step-based) and the per-Component
// canaryTrafficPercent (simple form). Operators must pick one per
// ISVC.
func validateMixedRolloutForms(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine != nil && spec.Engine.CanaryTrafficPercent != nil {
		return fmt.Errorf(
			"spec.traffic.rollout is set AND spec.engine.canaryTrafficPercent=%d; pick one rollout form per ISVC (MixedRolloutForms)",
			*spec.Engine.CanaryTrafficPercent)
	}
	if spec.Decoder != nil && spec.Decoder.CanaryTrafficPercent != nil {
		return fmt.Errorf(
			"spec.traffic.rollout is set AND spec.decoder.canaryTrafficPercent=%d; pick one rollout form per ISVC (MixedRolloutForms)",
			*spec.Decoder.CanaryTrafficPercent)
	}
	if spec.Router != nil && spec.Router.CanaryTrafficPercent != nil {
		return fmt.Errorf(
			"spec.traffic.rollout is set AND spec.router.canaryTrafficPercent=%d; pick one rollout form per ISVC (MixedRolloutForms)",
			*spec.Router.CanaryTrafficPercent)
	}
	return nil
}
