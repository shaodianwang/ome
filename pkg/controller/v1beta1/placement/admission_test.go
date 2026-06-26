package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func isvcWithInstances(component v1beta1.ComponentType, admitted ...bool) *v1beta1.InferenceService {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(admitted))
	for i, a := range admitted {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Admitted: a}
	}
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				component: {Lifecycle: &v1beta1.LifecycleStatus{InstanceStatuses: insts}},
			},
		},
	}
	declareComponent(isvc, component)
	return isvc
}

// declareComponent adds a minimal spec block for component so AllComponentsAdmitted
// (which reads declared components off the spec) sees it as declared.
func declareComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) {
	switch component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{}
	}
}

func TestAllComponentsAdmitted(t *testing.T) {
	// single declared component, admitted -> true.
	assert.True(t, AllComponentsAdmitted(isvcWithInstances(v1beta1.EngineComponent, false, true)))
	// single declared component, all gated -> false.
	assert.False(t, AllComponentsAdmitted(isvcWithInstances(v1beta1.EngineComponent, false, false)))
	// no declared components -> never a winner.
	assert.False(t, AllComponentsAdmitted(&v1beta1.InferenceService{}))

	// PD: engine admitted but decoder still gated -> NOT a winner.
	pd := isvcWithInstances(v1beta1.EngineComponent, true)
	pd.Spec.Decoder = &v1beta1.DecoderSpec{}
	pd.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
		Lifecycle: &v1beta1.LifecycleStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: false}}},
	}
	assert.False(t, AllComponentsAdmitted(pd), "engine admitted, decoder gated must not win")

	// PD: both admitted -> winner.
	pd.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
		Lifecycle: &v1beta1.LifecycleStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
	}
	assert.True(t, AllComponentsAdmitted(pd), "both components admitted must win")

	// declared component with no status entry yet -> false (no panic).
	missingStatus := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
	assert.False(t, AllComponentsAdmitted(missingStatus))
}

func TestAnyInstanceAdmitted(t *testing.T) {
	// at least one admitted instance -> true.
	assert.True(t, AnyInstanceAdmitted(isvcWithInstances(v1beta1.EngineComponent, false, true)))
	// all gated -> false.
	assert.False(t, AnyInstanceAdmitted(isvcWithInstances(v1beta1.EngineComponent, false, false)))
	// no components / no instances -> false.
	assert.False(t, AnyInstanceAdmitted(&v1beta1.InferenceService{}))
	// component present but nil Lifecycle -> false (no panic).
	assert.False(t, AnyInstanceAdmitted(&v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {}},
		},
	}))
}
