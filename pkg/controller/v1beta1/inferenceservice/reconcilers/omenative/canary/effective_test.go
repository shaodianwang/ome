package canary

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestEffectivePartition(t *testing.T) {
	mk := func(steps ...v1beta1.RolloutStep) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Canary: &v1beta1.CanarySpec{Steps: steps}}}}
	}

	// No canary → not active.
	if _, ok := EffectivePartition(&v1beta1.InferenceService{}, v1beta1.EngineComponent, 4); ok {
		t.Fatal("no canary must be inactive")
	}

	// Step 0 (no status yet) of a 50% step at N=4 → 2 new, partition = 4-2 = 2.
	isvc := mk(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	p, ok := EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 2 {
		t.Fatalf("step0: want partition 2, got %v ok=%v", p, ok)
	}

	// Advance to the final step → 100% new → partition 0.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 1}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("final: want partition 0, got %v ok=%v", p, ok)
	}

	// Done sentinel (CurrentStep == len(steps)) → partition 0 (all on the canary
	// revision, old drains). A finished canary must NOT re-default to step 0's
	// partition, which would hold instances on the old revision after completion.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 2}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("done sentinel: want partition 0, got %v ok=%v", p, ok)
	}

	// Rollback is deferred → the annotation is ignored; partition follows the
	// current step (here the final step → 0), NOT a hold-all-on-stable (which
	// wedged the rollout). To roll back, revert the spec.
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("rollback deferred: want step partition 0 (annotation ignored), got %v ok=%v", p, ok)
	}
}

func TestStampStepPartition(t *testing.T) {
	n := 4
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Rollout: &v1beta1.RolloutSpec{Canary: &v1beta1.CanarySpec{Steps: []v1beta1.RolloutStep{
			{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50},
			{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
		}}}}}
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 4}
	StampStepPartition(isvc, v1beta1.EngineComponent, ext)
	ru := ext.Lifecycle.UpdateStrategy.RollingUpdate
	if ru == nil || ru.Partition == nil || *ru.Partition != 2 {
		t.Fatalf("step0 50%% of 4 → partition 2, got %+v", ru)
	}

	// No canary → no-op (lifecycle chain not created).
	ext2 := &v1beta1.ComponentExtensionSpec{MinReplicas: &n}
	StampStepPartition(&v1beta1.InferenceService{}, v1beta1.EngineComponent, ext2)
	if ext2.Lifecycle != nil {
		t.Fatal("no canary must not stamp lifecycle")
	}
}
