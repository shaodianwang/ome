// OEP-0011 per-Component canaryTrafficPercent validation.
//
// Covers the simple (non-step-based) rollout form: the existing
// ComponentExtensionSpec.CanaryTrafficPercent field, generalized from
// Knative-only to all deployment modes. CRD has no Min/Max marker on
// the field, so the webhook owns the range check; PD coupling
// (engine == decoder) is a cross-field rule the CRD cannot express.
package validation

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ValidateCanaryPercent enforces:
//   - InvalidCanaryPercent: each component's CanaryTrafficPercent
//     must lie in [0, 100].
//   - MismatchedCanaryPercent: when both engine.canaryTrafficPercent
//     and decoder.canaryTrafficPercent are set (PD mode), they must
//     be equal. Routing engine traffic at one percent while decoder
//     traffic flows at another would mix v1 and v2 KV cache shapes
//     and silently corrupt PD requests.
func ValidateCanaryPercent(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}

	var enginePct, decoderPct, routerPct *int64
	if spec.Engine != nil {
		enginePct = spec.Engine.CanaryTrafficPercent
	}
	if spec.Decoder != nil {
		decoderPct = spec.Decoder.CanaryTrafficPercent
	}
	if spec.Router != nil {
		routerPct = spec.Router.CanaryTrafficPercent
	}

	if err := validateCanaryPercentRange("engine", enginePct); err != nil {
		return err
	}
	if err := validateCanaryPercentRange("decoder", decoderPct); err != nil {
		return err
	}
	if err := validateCanaryPercentRange("router", routerPct); err != nil {
		return err
	}

	if enginePct != nil && decoderPct != nil && *enginePct != *decoderPct {
		return fmt.Errorf(
			"spec.engine.canaryTrafficPercent=%d and spec.decoder.canaryTrafficPercent=%d must be equal in PD mode (MismatchedCanaryPercent); a split percent risks routing engine traffic to a stale decoder revision",
			*enginePct, *decoderPct,
		)
	}

	return nil
}

func validateCanaryPercentRange(component string, pct *int64) error {
	if pct == nil {
		return nil
	}
	if *pct < 0 || *pct > 100 {
		return fmt.Errorf(
			"spec.%s.canaryTrafficPercent=%d; must be in [0, 100] (InvalidCanaryPercent)",
			component, *pct,
		)
	}
	return nil
}
