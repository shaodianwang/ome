package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ReasonCanaryInvalid is the admission rejection reason for a malformed
// spec.rollout.canary plan. Operators and the test plan reference it by name.
const ReasonCanaryInvalid = "CanaryInvalid"

// ReasonCanaryRequiresOMENative is the rejection reason for a canary plan on a
// component that is not OMENative. Canary relies on per-revision pod selection,
// partition staging, and the InferenceReplica object — all OMENative/IR-managed
// constructs — so other deployment modes would silently wedge at Pending.
const ReasonCanaryRequiresOMENative = "CanaryRequiresOMENative"

// ValidateCanary checks a spec.rollout.canary plan. Returns nil when canary is
// unset (nil-safe). Rules, each guarding a concrete failure:
//   - Mutually exclusive with spec.rollout.coordination — an ISVC rolls either
//     by coordination groups or by a canary plan, never both.
//   - Steps non-empty — an empty plan has nothing to execute.
//   - Final step TrafficWeight == 100 — otherwise the rollout never reaches the
//     new revision and hangs forever.
//   - A step with TrafficWeight > 0 must have NewCapacity > 0 — you cannot route
//     traffic to zero pods.
func ValidateCanary(spec *v1beta1.InferenceServiceSpec) error {
	c := spec.GetCanary()
	if c == nil {
		return nil
	}
	if spec.Rollout.Coordination != nil {
		return fmt.Errorf("spec.rollout.canary and spec.rollout.coordination are mutually exclusive (%s)", ReasonCanaryInvalid)
	}
	if len(c.Steps) == 0 {
		return fmt.Errorf("spec.rollout.canary.steps must not be empty (%s)", ReasonCanaryInvalid)
	}
	// Canary runs only on the OMENative/IR-managed path; reject other modes at
	// admission rather than letting the rollout wedge at Pending. Mirrors
	// coordination's validateComponentsAreOMENative, but canary has no groups so
	// every declared component must be OMENative.
	for comp, mode := range omenativeDeploymentModeMap(spec) {
		if mode != string(constants.OMENative) {
			return fmt.Errorf("spec.rollout.canary: component %q has deploymentMode=%q; canary requires OMENative (%s)",
				comp, mode, ReasonCanaryRequiresOMENative)
		}
	}
	last := c.Steps[len(c.Steps)-1]
	if last.TrafficWeight != 100 {
		return fmt.Errorf("spec.rollout.canary final step trafficWeight must be 100, got %d (%s)", last.TrafficWeight, ReasonCanaryInvalid)
	}
	// The final step takes 100% traffic, so it must run at full capacity —
	// otherwise the rollout completes with 100% traffic on a partial fleet. Only
	// the percentage form is checkable at admission (absolute counts need the
	// desired replica count); the done sentinel also forces partition 0.
	if pct, ok := percentValue(last.NewCapacity); ok && pct != 100 {
		return fmt.Errorf("spec.rollout.canary final step newCapacity must be 100%% (rollout completes at full capacity), got %q (%s)", last.NewCapacity.String(), ReasonCanaryInvalid)
	}
	// PromotionPolicy is required once any step pauses: shouldAdvance defaults an
	// unset policy to Manual, so an Auto-intending operator who omits it would get
	// a rollout that waits forever for a promote annotation.
	if c.PromotionPolicy == nil {
		for i, s := range c.Steps {
			if s.Pause != nil {
				return fmt.Errorf("spec.rollout.canary.steps[%d] has a pause but spec.rollout.canary.promotionPolicy is unset; set Auto or Manual (%s)", i, ReasonCanaryInvalid)
			}
		}
	}
	var prevWeight int32
	for i, s := range c.Steps {
		if s.TrafficWeight < 0 || s.TrafficWeight > 100 {
			return fmt.Errorf("spec.rollout.canary.steps[%d]: trafficWeight %d must be in [0,100] (%s)", i, s.TrafficWeight, ReasonCanaryInvalid)
		}
		if s.TrafficWeight < prevWeight {
			return fmt.Errorf("spec.rollout.canary.steps[%d]: trafficWeight %d must be >= the previous step's %d (canary weights are non-decreasing) (%s)", i, s.TrafficWeight, prevWeight, ReasonCanaryInvalid)
		}
		prevWeight = s.TrafficWeight
		if s.TrafficWeight > 0 && newCapacityIsZero(s.NewCapacity) {
			return fmt.Errorf("spec.rollout.canary.steps[%d]: trafficWeight %d>0 requires newCapacity>0 (%s)", i, s.TrafficWeight, ReasonCanaryInvalid)
		}
		if err := validateStepOldCapacity(i, s); err != nil {
			return err
		}
	}
	return nil
}

// validateStepOldCapacity rejects an explicit OldCapacity that is not the
// complement of NewCapacity. v1 runs the old revision at (100% - NewCapacity) via
// the StatefulSet-style partition; an independent old capacity (surge above
// desired, or under-provisioning the old revision) is not expressible, so an
// OldCapacity that disagrees with the complement would be silently ignored.
// Reject it rather than mislead.
func validateStepOldCapacity(i int, s v1beta1.RolloutStep) error {
	if s.OldCapacity == nil {
		return nil
	}
	newPct, okNew := percentValue(s.NewCapacity)
	oldPct, okOld := percentValue(*s.OldCapacity)
	if !okNew || !okOld || newPct+oldPct != 100 {
		return fmt.Errorf("spec.rollout.canary.steps[%d]: oldCapacity must be a percentage equal to the complement of newCapacity (newCapacity %q + oldCapacity must total 100%%); independent old capacity / surge above desired is unsupported in v1 — omit oldCapacity to default to (100%%-newCapacity) (%s)",
			i, s.NewCapacity.String(), ReasonCanaryInvalid)
	}
	return nil
}

// percentValue extracts the integer N from an IntOrString "<N>%" form.
func percentValue(v intstr.IntOrString) (int, bool) {
	if v.Type != intstr.String || !strings.HasSuffix(v.StrVal, "%") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(v.StrVal, "%"))
	if err != nil {
		return 0, false
	}
	return n, true
}

// newCapacityIsZero reports whether a step's NewCapacity resolves to zero pods
// regardless of how it is expressed (integer 0, "0", or "0%").
func newCapacityIsZero(c intstr.IntOrString) bool {
	if c.Type == intstr.Int {
		return c.IntValue() == 0
	}
	return c.StrVal == "0" || c.StrVal == "0%"
}
