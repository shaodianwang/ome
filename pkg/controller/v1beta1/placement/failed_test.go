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

func TestIsTerminallyFailed(t *testing.T) {
	assert.True(t, IsTerminallyFailed(isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceFailed)))
	assert.False(t, IsTerminallyFailed(isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceCreating)))
	assert.False(t, IsTerminallyFailed(&v1beta1.InferenceService{}))

	// Non-OMENative failures surface via the model-status transition, with no
	// failed Instance phase. These must also be terminal so the placement does
	// not re-fan-out into the same failure forever.
	blockedLoad := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.BlockedByFailedLoad},
	}}
	assert.True(t, IsTerminallyFailed(blockedLoad), "model-load failure is terminal")

	invalidSpec := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.InvalidSpec},
	}}
	assert.True(t, IsTerminallyFailed(invalidSpec), "runtime-selection/spec failure is terminal")

	// In-progress model status is NOT terminal.
	inProgress := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.InProgress},
	}}
	assert.False(t, IsTerminallyFailed(inProgress))
}
