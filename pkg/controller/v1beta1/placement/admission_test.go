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
	return &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				component: {Lifecycle: &v1beta1.LifecycleStatus{InstanceStatuses: insts}},
			},
		},
	}
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
