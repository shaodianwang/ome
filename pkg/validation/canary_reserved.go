// Reserved-canary admission guard.
//
// spec.rollout.canary is a reserved progressive-traffic-shift plan whose
// shape is fixed so enabling it later is not an API migration, but no step
// executor exists yet. The webhook rejects any spec that sets it.
package validation

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ReasonRolloutCanaryReserved is the admission rejection reason emitted when
// spec.rollout.canary is set. Operators and the test plan reference it by name.
const ReasonRolloutCanaryReserved = "RolloutCanaryReserved"

// ValidateRolloutCanaryReserved rejects an InferenceService that sets
// spec.rollout.canary. The canary plan is reserved (shape fixed, no executor),
// so setting it is an error until the step executor lands. Returns nil when
// spec.rollout or spec.rollout.canary is unset.
func ValidateRolloutCanaryReserved(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.Rollout == nil || spec.Rollout.Canary == nil {
		return nil
	}
	return fmt.Errorf("spec.rollout.canary is not yet implemented (%s)", ReasonRolloutCanaryReserved)
}
