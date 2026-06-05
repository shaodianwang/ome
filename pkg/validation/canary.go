package validation

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ReasonCanaryInvalid is the admission rejection reason for a malformed
// spec.rollout.canary plan. Operators and the test plan reference it by name.
const ReasonCanaryInvalid = "CanaryInvalid"

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
	last := c.Steps[len(c.Steps)-1]
	if last.TrafficWeight != 100 {
		return fmt.Errorf("spec.rollout.canary final step trafficWeight must be 100, got %d (%s)", last.TrafficWeight, ReasonCanaryInvalid)
	}
	for i, s := range c.Steps {
		if s.TrafficWeight > 0 && newCapacityIsZero(s.NewCapacity) {
			return fmt.Errorf("spec.rollout.canary.steps[%d]: trafficWeight %d>0 requires newCapacity>0 (%s)", i, s.TrafficWeight, ReasonCanaryInvalid)
		}
	}
	return nil
}

// newCapacityIsZero reports whether a step's NewCapacity resolves to zero pods
// regardless of how it is expressed (integer 0, "0", or "0%").
func newCapacityIsZero(c intstr.IntOrString) bool {
	if c.Type == intstr.Int {
		return c.IntValue() == 0
	}
	return c.StrVal == "0" || c.StrVal == "0%"
}
