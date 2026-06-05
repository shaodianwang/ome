package canary

import (
	"context"
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func promPolicy(p v1beta1.PromotionPolicy) *v1beta1.PromotionPolicy { return &p }
func i32(v int32) *int32                                            { return &v }

func canaryISVC(steps []v1beta1.RolloutStep, policy *v1beta1.PromotionPolicy, scaleDownDelay *int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Canary: &v1beta1.CanarySpec{
			Steps:                 steps,
			PromotionPolicy:       policy,
			ScaleDownDelaySeconds: scaleDownDelay,
		}}},
	}
}

func twoStep() []v1beta1.RolloutStep {
	return []v1beta1.RolloutStep{
		{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50, Pause: &v1beta1.RolloutPause{}},
		{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	}
}

func phaseOf(isvc *v1beta1.InferenceService) v1beta1.RolloutPhase {
	return isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

func baseInputs(isvc *v1beta1.InferenceService, pods map[string]int32) ReconcileInputs {
	return ReconcileInputs{
		ISVC:               isvc,
		Component:          v1beta1.EngineComponent,
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		DesiredReplicas:    4,
		PerRevisionPods:    pods,
		Now:                time.Unix(1000, 0),
	}
}

func TestReconcile_NoCanaryIsNoop(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	res, err := Reconcile(context.Background(), ReconcileInputs{ISVC: isvc})
	if err != nil || res == nil || res.Active {
		t.Fatalf("no canary must be inactive noop: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be written")
	}
}

func TestReconcile_CapacityGatePending(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("expected Pending while canary pods not ready, got %q", phaseOf(isvc))
	}
	if res.Partition != 2 { // desired 4 - newCount 2
		t.Fatalf("expected Partition 2, got %d", res.Partition)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should shift while gated")
	}
}

func TestReconcile_CanaryingPaused(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("expected Paused at a manual paused step, got %q", phaseOf(isvc))
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 2 {
		t.Fatalf("expected 50/50 split, got %+v", tr)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("should not advance without promote, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_ManualPromoteAdvances(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance to step 1, got %d", isvc.Status.Canary.CurrentStep)
	}
	if _, still := isvc.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("promote annotation should be consumed on advance")
	}
}

func TestReconcile_StalePromoteDoesNotAdvance(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "someoldhash"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("stale promote (wrong hash) must not advance, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_AutoAdvance(t *testing.T) {
	steps := []v1beta1.RolloutStep{
		{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 30 * time.Minute}}},
		{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	}
	isvc := canaryISVC(steps, promPolicy(v1beta1.PromotionPolicyAuto), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}

	in := baseInputs(isvc, map[string]int32{"new": 2})
	in.Now = time.Unix(1000, 0).Add(10 * time.Minute) // before duration
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("auto must not advance before duration, step=%d", isvc.Status.Canary.CurrentStep)
	}
	in.Now = time.Unix(1000, 0).Add(31 * time.Minute) // after duration
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("auto must advance after duration, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_FinalStepCompletes(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutStep{{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100}}, nil, nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("expected Stable on completion, got %q", phaseOf(isvc))
	}
	if !res.Complete || res.Partition != 0 {
		t.Fatalf("expected complete + partition 0, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("canary status should be cleared on completion")
	}
}

func TestReconcile_FinalStepDrainWait(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutStep{{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100}}, nil, i32(60))
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}

	in := baseInputs(isvc, map[string]int32{"new": 4})
	in.Now = time.Unix(1000, 0).Add(30 * time.Second) // within drain window
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("expected Promoting (draining), got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	in.Now = time.Unix(1000, 0).Add(61 * time.Second) // drain elapsed
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable || !res.Complete {
		t.Fatalf("expected Stable + complete after drain, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
}

func TestReconcile_Rollback(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}

	// canary pods still present → RollingBack, all traffic to stable, partition holds all on old.
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack || res.Complete {
		t.Fatalf("expected RollingBack while draining, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	if res.Partition != 4 {
		t.Fatalf("rollback must hold all instances on stable (partition=desired), got %d", res.Partition)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].Percent != 100 {
		t.Fatalf("rollback must route 100%% to stable, got %+v", tr)
	}

	// canary pods drained → Stable, complete, status + annotation cleared.
	res, _ = Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable || !res.Complete {
		t.Fatalf("expected Stable + complete after canary drains, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("canary status should be cleared after rollback")
	}
	if _, still := isvc.Annotations[constants.RolloutRollbackAnnotation]; still {
		t.Fatal("rollback annotation should be consumed")
	}
}
