package validation

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestValidateCoordination_NilSpecIsOK(t *testing.T) {
	if err := ValidateCoordination(nil); err != nil {
		t.Errorf("nil spec: unexpected error %v", err)
	}
}

func TestValidateCoordination_MissingCoordinationBlockIsOK(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("missing coordination: unexpected error %v", err)
	}
}

func TestValidateCoordination_EmptyGroupsIsOK(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{
		RolloutCoordination: &v1beta1.RolloutCoordinationSpec{Groups: nil},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("empty groups: unexpected error %v", err)
	}
}

func TestValidateCoordination_DuplicateComponentRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonDuplicateComponentInCoordinationGroups) {
		t.Errorf("duplicate component: got %v want error with %s", err, ReasonDuplicateComponentInCoordinationGroups)
	}
}

func TestValidateCoordination_InvalidComponentName(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{"frontend"}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonInvalidComponentInCoordinationGroup) {
		t.Errorf("invalid component: got %v want %s", err, ReasonInvalidComponentInCoordinationGroup)
	}
}

func TestValidateCoordination_OrphanGroup(t *testing.T) {
	// Spec declares only engine; group references decoder + router.
	spec := omeNativeEngineOnlySpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonOrphanCoordinationGroup) {
		t.Errorf("orphan group: got %v want %s", err, ReasonOrphanCoordinationGroup)
	}
}

func TestValidateCoordination_ComponentMustBeOMENative(t *testing.T) {
	spec := omeNativePDSpec()
	// Strip the OMENative annotation off engine.
	spec.Engine.Annotations = map[string]string{}
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCoordinationRequiresOMENative) {
		t.Errorf("non-OMENative component: got %v want %s", err, ReasonCoordinationRequiresOMENative)
	}
}

func TestValidateCoordination_SequentialWithoutOrder(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonSequentialRequiresOrder) {
		t.Errorf("sequential without order: got %v want %s", err, ReasonSequentialRequiresOrder)
	}
}

func TestValidateCoordination_SequentialIncompleteOrder(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Router = &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonSequentialIncompleteOrder) {
		t.Errorf("sequential incomplete order: got %v want %s", err, ReasonSequentialIncompleteOrder)
	}
}

func TestValidateCoordination_SequentialSingleComponentRejected(t *testing.T) {
	spec := omeNativeEngineOnlySpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonSequentialRequiresMultipleComponents) {
		t.Errorf("sequential single component: got %v want %s", err, ReasonSequentialRequiresMultipleComponents)
	}
}

func TestValidateCoordination_RollingUpdateSingleComponentRejected(t *testing.T) {
	spec := omeNativeEngineOnlySpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonRollingUpdateRequiresMultipleComponents) {
		t.Errorf("rollingupdate single: got %v want %s", err, ReasonRollingUpdateRequiresMultipleComponents)
	}
}

func TestValidateCoordination_InvalidOrderComponent(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonInvalidCoordinationOrder) {
		t.Errorf("foreign order entry: got %v want %s", err, ReasonInvalidCoordinationOrder)
	}
}

func TestValidateCoordination_HappyPath_BlueGreen(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("happy atomic: unexpected error %v", err)
	}
}

func TestValidateCoordination_HappyPath_Sequential(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("happy sequential: unexpected error %v", err)
	}
}

// W-1 reproducer: Sequential + RatioBalanced is meaningless. Sequential
// serializes Components so the cross-Component ratio is undefined.
func TestValidateCoordination_SequentialWithRatioBalancedRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					Type: v1beta1.CoordinationPacingRatioBalanced,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonSequentialIncompatibleWithRatioBalanced) {
		t.Errorf("Sequential+RatioBalanced: got %v want error with %s", err, ReasonSequentialIncompatibleWithRatioBalanced)
	}
}

// Positive case: Sequential without RatioBalanced pacing is accepted.
// Sequential + PerComponent is the canonical combination; the
// validator must not over-reject.
func TestValidateCoordination_SequentialWithPerComponentPacingAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					Type: v1beta1.CoordinationPacingPerComponent,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("Sequential+PerComponent: unexpected error %v", err)
	}
}

// Positive case: RatioBalanced under a non-Sequential policy
// (BlueGreen) is accepted. The pacing/policy incompatibility check
// only fires for Sequential.
func TestValidateCoordination_RatioBalancedWithBlueGreenAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type: v1beta1.CoordinationPacingRatioBalanced,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("BlueGreen+RatioBalanced: unexpected error %v", err)
	}
}

// W-2 reproducer: maxSurge=0 AND maxUnavailable=0 (both explicit zero)
// is unstartable — no surge, no drain.
func TestValidateCoordination_ZeroBudgetPacingRejected(t *testing.T) {
	zero := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonZeroBudgetPacingUnstartable) {
		t.Errorf("zero-budget pacing: got %v want error with %s", err, ReasonZeroBudgetPacingUnstartable)
	}
}

// W-2 reproducer with string "0%" values — equivalent to literal 0
// and must produce the same rejection.
func TestValidateCoordination_ZeroBudgetPacingRejectedStringForm(t *testing.T) {
	zeroPct := intstr.FromString("0%")
	zeroInt := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &zeroPct,
					MaxUnavailable: &zeroInt,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonZeroBudgetPacingUnstartable) {
		t.Errorf("zero-budget (str/int mix): got %v want error with %s", err, ReasonZeroBudgetPacingUnstartable)
	}
}

// Positive case: pacing with non-zero MaxSurge is accepted even when
// MaxUnavailable=0 — the rollout still has surge headroom.
func TestValidateCoordination_NonZeroSurgePacingAccepted(t *testing.T) {
	surge := intstr.FromInt(2)
	zero := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &surge,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("non-zero surge + zero unavailable: unexpected error %v", err)
	}
}

// Positive case: pacing with non-zero MaxUnavailable is accepted even
// when MaxSurge=0 — the rollout still has drain headroom.
func TestValidateCoordination_NonZeroUnavailablePacingAccepted(t *testing.T) {
	zero := intstr.FromInt(0)
	pct := intstr.FromString("25%")
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &zero,
					MaxUnavailable: &pct,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("zero surge + non-zero unavailable: unexpected error %v", err)
	}
}

// Positive case: nil pacing values (no explicit zero) are accepted —
// the defaulter fills 25% for both MaxSurge and MaxUnavailable. The
// zero-budget check only fires when both are explicitly set to 0.
func TestValidateCoordination_NilPacingAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing:     &v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingPerComponent},
				// MaxSurge / MaxUnavailable nil — defaulter fills 25%.
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("nil pacing values: unexpected error %v", err)
	}
}

// Positive case: omitted Pacing block entirely is accepted —
// the defaulter fills a complete pacing block.
func TestValidateCoordination_OmittedPacingAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				// Pacing entirely absent.
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("omitted pacing: unexpected error %v", err)
	}
}

// The zero-budget check only fires for PerComponent pacing (or empty,
// which defaults to PerComponent). RatioBalanced has its own knob
// (ratioTolerance) and the maxSurge/maxUnavailable fields are
// interpreted differently — don't reject on the same shape.
func TestValidateCoordination_RatioBalancedWithZeroBudgetNotRejected(t *testing.T) {
	zero := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingRatioBalanced,
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("RatioBalanced + zero surge/unavailable: unexpected error %v (zero-budget check is PerComponent-only)", err)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateBothBumped(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v2")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
		},
	}
	if err := ValidateCoordinationUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("pairedmixable both-bumped: unexpected error %v", err)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateOnlyOneBumpedRejected(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v1")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
		},
	}
	err := ValidateCoordinationUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonRollingUpdateLockstepViolation) {
		t.Errorf("rollingupdate lockstep: got %v want %s", err, ReasonRollingUpdateLockstepViolation)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateNoBumpsNoOp(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
		},
	}
	if err := ValidateCoordinationUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("no bumps: unexpected error %v", err)
	}
}

func TestCoordinationRatioToleranceWarning_AboveThreshold(t *testing.T) {
	tol := int32(80)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerance: &tol,
				},
			},
		},
	}
	w := CoordinationRatioToleranceWarning(spec)
	if !strings.Contains(w, ReasonRatioToleranceTooHigh) {
		t.Errorf("tolerance warning: got %q want contains %s", w, ReasonRatioToleranceTooHigh)
	}
}

func TestCoordinationRatioToleranceWarning_BelowThresholdEmpty(t *testing.T) {
	tol := int32(5)
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerance: &tol,
				},
			},
		},
	}
	if w := CoordinationRatioToleranceWarning(spec); w != "" {
		t.Errorf("safe tolerance: got %q want empty", w)
	}
}

// Test fixtures.

func omeNativePDSpec() *v1beta1.InferenceServiceSpec {
	annot := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	return &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
		Decoder: &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
	}
}

func omeNativePDSpecWithImages(engineImage, decoderImage string) *v1beta1.InferenceServiceSpec {
	spec := omeNativePDSpec()
	spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Image: engineImage}}
	spec.Decoder.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Image: decoderImage}}
	return spec
}

func omeNativeEngineOnlySpec() *v1beta1.InferenceServiceSpec {
	annot := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	return &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
	}
}
