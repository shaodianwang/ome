// Per-Component LifecycleSpec.UpdateStrategy.RollingUpdate validation.
//
// The CRD schema's XIntOrString marker already gates the structural
// shape (must parse as integer or string). These helpers add the
// semantic checks the apiserver can't express: percent strings must
// match "<n>%" with 0 <= n <= 100, integer values must be >= 0.
//
// MaxUnavailable=0 paired with MaxSurge=0 is permitted at this layer —
// the per-Component RollingUpdate fields don't directly drive the
// rollout budget today (the coordination-group MaxUnavailable /
// MaxSurge knobs do). The webhook still rejects the equivalent
// deadlock at the coordination-pacing layer (see
// validatePacingNotZeroBudget in coordination.go) so the operator-
// visible blast radius stays consistent.
package validation

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Admission rejection reasons for per-Component lifecycle settings.
const (
	ReasonInvalidRollingUpdatePercent = "InvalidRollingUpdatePercent"
	ReasonInvalidRollingUpdateInteger = "InvalidRollingUpdateInteger"
)

// ValidateLifecycle inspects every Component's LifecycleSpec on the
// InferenceService. Today the only per-Component knobs that need
// semantic validation past the CRD schema are
// UpdateStrategy.RollingUpdate.MaxUnavailable and .MaxSurge — both
// were widened from *int32 to *intstr.IntOrString (Gap 4 / Gap 5).
// Wired into the webhook from inference_service_validation.go.
func ValidateLifecycle(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	if spec.Engine != nil {
		if err := validateComponentLifecycle("engine", spec.Engine.Lifecycle); err != nil {
			return err
		}
	}
	if spec.Decoder != nil {
		if err := validateComponentLifecycle("decoder", spec.Decoder.Lifecycle); err != nil {
			return err
		}
	}
	if spec.Router != nil {
		if err := validateComponentLifecycle("router", spec.Router.Lifecycle); err != nil {
			return err
		}
	}
	return nil
}

func validateComponentLifecycle(name string, lc *v1beta1.LifecycleSpec) error {
	if lc == nil || lc.UpdateStrategy == nil || lc.UpdateStrategy.RollingUpdate == nil {
		return nil
	}
	ru := lc.UpdateStrategy.RollingUpdate
	if err := validateRollingUpdateIntOrString(name, "maxUnavailable", ru.MaxUnavailable); err != nil {
		return err
	}
	if err := validateRollingUpdateIntOrString(name, "maxSurge", ru.MaxSurge); err != nil {
		return err
	}
	return nil
}

// validateRollingUpdateIntOrString enforces:
//   - integer values >= 0
//   - string values match "<n>%" with 0 <= n <= 100
//
// Returns nil for nil inputs (the workload reconciler defaults nil to
// 25% — both the API documentation and pacingWithDefaults agree on
// the same fallback).
func validateRollingUpdateIntOrString(component, field string, v *intstr.IntOrString) error {
	if v == nil {
		return nil
	}
	if v.Type == intstr.Int {
		if v.IntValue() < 0 {
			return fmt.Errorf("spec.%s.lifecycle.updateStrategy.rollingUpdate.%s=%d must be >= 0 (%s)",
				component, field, v.IntValue(), ReasonInvalidRollingUpdateInteger)
		}
		return nil
	}
	// String form — must be "<n>%" with 0 <= n <= 100.
	s := v.StrVal
	if !strings.HasSuffix(s, "%") {
		return fmt.Errorf("spec.%s.lifecycle.updateStrategy.rollingUpdate.%s=%q must be an integer count or a percent string like \"25%%\" (%s)",
			component, field, s, ReasonInvalidRollingUpdatePercent)
	}
	pctStr := strings.TrimSuffix(s, "%")
	pct, err := strconv.Atoi(pctStr)
	if err != nil {
		return fmt.Errorf("spec.%s.lifecycle.updateStrategy.rollingUpdate.%s=%q has a malformed percent value (%s)",
			component, field, s, ReasonInvalidRollingUpdatePercent)
	}
	if pct < 0 || pct > 100 {
		return fmt.Errorf("spec.%s.lifecycle.updateStrategy.rollingUpdate.%s=%q percent must be between 0%% and 100%% (%s)",
			component, field, s, ReasonInvalidRollingUpdatePercent)
	}
	return nil
}
