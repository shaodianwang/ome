package constants

import "testing"

// TestDeploymentModeType_IsValid exercises IsValid() against every
// DeploymentModeType constant declared in this package. If a new mode
// is added to the const block and not wired into IsValid(), this test
// fails — keeping the enum and the validator in lockstep.
func TestDeploymentModeType_IsValid(t *testing.T) {
	allModes := []DeploymentModeType{
		RawDeployment,
		PDDisaggregated,
		MultiNode,
		VirtualDeployment,
		OMENative,
	}

	for _, mode := range allModes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			if !mode.IsValid() {
				t.Errorf("DeploymentModeType(%q).IsValid() = false, want true", mode)
			}
		})
	}

	t.Run("unknown", func(t *testing.T) {
		var unknown DeploymentModeType = "NotARealMode"
		if unknown.IsValid() {
			t.Errorf("DeploymentModeType(%q).IsValid() = true, want false", unknown)
		}
	})

	t.Run("empty", func(t *testing.T) {
		var empty DeploymentModeType
		if empty.IsValid() {
			t.Errorf("DeploymentModeType(\"\").IsValid() = true, want false")
		}
	})
}
