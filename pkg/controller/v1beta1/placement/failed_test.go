package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func isvcWithPhases(component v1beta1.ComponentType, phases ...v1beta1.OMENativeInstancePhase) *v1beta1.InferenceService {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(phases))
	for i, p := range phases {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: p}
	}
	return &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				component: {Lifecycle: &v1beta1.LifecycleStatus{InstanceStatuses: insts}},
			},
		},
	}
}

func TestIsTerminallyFailed(t *testing.T) {
	assert.True(t, IsTerminallyFailed(isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceFailed)))
	assert.False(t, IsTerminallyFailed(isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceCreating)))
	assert.False(t, IsTerminallyFailed(&v1beta1.InferenceService{}))
}
