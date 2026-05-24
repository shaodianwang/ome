package validation

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

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
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyAtomic},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic},
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
			{Components: []v1beta1.ComponentType{"frontend"}, Policy: v1beta1.CoordinationPolicyAtomic},
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
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent}, Policy: v1beta1.CoordinationPolicyAtomic},
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
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic},
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

func TestValidateCoordination_PairedMixableSingleComponentRejected(t *testing.T) {
	spec := omeNativeEngineOnlySpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyPairedMixable,
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonPairedMixableRequiresMultipleComponents) {
		t.Errorf("pairedmixable single: got %v want %s", err, ReasonPairedMixableRequiresMultipleComponents)
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

func TestValidateCoordination_HappyPath_Atomic(t *testing.T) {
	spec := omeNativePDSpec()
	spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic},
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

func TestValidateCoordinationUpdate_PairedMixableBothBumped(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v2")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable},
		},
	}
	if err := ValidateCoordinationUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("pairedmixable both-bumped: unexpected error %v", err)
	}
}

func TestValidateCoordinationUpdate_PairedMixableOnlyOneBumpedRejected(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v1")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable},
		},
	}
	err := ValidateCoordinationUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonPairedMixableLockstepViolation) {
		t.Errorf("pairedmixable lockstep: got %v want %s", err, ReasonPairedMixableLockstepViolation)
	}
}

func TestValidateCoordinationUpdate_PairedMixableNoBumpsNoOp(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable},
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
				Policy:     v1beta1.CoordinationPolicyAtomic,
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
				Policy:     v1beta1.CoordinationPolicyAtomic,
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
