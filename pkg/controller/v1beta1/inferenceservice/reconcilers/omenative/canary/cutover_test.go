package canary

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/rollout"
)

// expectSoleTarget asserts one Component's traffic rests entirely on one
// revision.
func expectSoleTarget(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) {
	t.Helper()
	want := coordination.PerRevisionServiceName(isvc.Name, comp, hash)
	tr := isvc.Status.Components[comp].Traffic
	if len(tr) != 1 || tr[0].RevisionName != want || tr[0].Percent != 100 {
		t.Fatalf("%s traffic must rest on %s alone, got %+v", comp, hash, tr)
	}
}

// The final step keeps the last stable instance serving until the traffic
// shift lands and the drain window elapses: the stable revision carries the
// previous step's weight until the 100% write, so its capacity cannot go
// before the write, and the drain window is the wait between that write and
// the scale-down. The step stages every other instance, shifts onto the
// canary capacity that is Ready, and the done sentinel releases the hold.
func TestReconcile_FinalStepHoldsTheLastStableInstanceThroughTheDrain(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(60)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // step 0 serves its split, manual hold
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in) // promoted to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)

	// The final step's partition keeps one instance on stable while the
	// stable revision still carries traffic.
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(4)}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("final step must hold the last stable instance (partition 1) before traffic moves, got %v", fmtPartition(p))
	}

	// Three canary instances Ready is all the hold allows; that is the
	// step's capacity, and the 100% write moves onto it.
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	res := mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("100%% traffic must move onto the Ready canary capacity, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	if res.Partition != 1 {
		t.Fatalf("the stable instance stays held through the drain window, got partition %d", res.Partition)
	}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("projected partition must keep the stable instance through the drain, got %v", fmtPartition(p))
	}

	// Inside the drain window nothing is released.
	in.Now = in.Now.Add(30 * time.Second)
	if res := mustReconcile(t, isvc, in); res.Complete || res.Partition != 1 {
		t.Fatalf("drain window must keep the hold, got %+v", res)
	}

	// The window elapses: the canary completes and the sentinel releases the
	// last instance to roll.
	in.Now = in.Now.Add(31 * time.Second)
	res = mustReconcile(t, isvc, in)
	if !res.Complete || res.Partition != 0 || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("drain elapsed must complete and release, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 0 {
		t.Fatalf("done sentinel must release the stable instance, got %v", fmtPartition(p))
	}
}

// A single-step plan cuts over the same way: all but one instance stage on
// the canary revision, traffic moves once they are Ready, and the last
// stable instance rolls after completion.
func TestReconcile_SingleStepPlanStagesAllButTheLastInstance(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Partition != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("staging must hold one stable instance, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	if res := mustReconcile(t, isvc, in); !res.Complete || res.Partition != 0 {
		t.Fatalf("three Ready canary instances complete an undelayed cutover, got %+v", res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
}

// A Component with a single instance has nothing to hold back: it stages
// in place as its only slot, and the ladder runs as declared.
func TestReconcile_SingleInstanceComponentStagesInPlace(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"old": 1})
	in.DesiredReplicas = 1
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Partition != 0 {
		t.Fatalf("a single instance rolls at step 0, got %+v", res)
	}
}

// The dispatcher-level cutover: the pass that finds the staged canary
// capacity Ready moves 100% traffic onto it while the projected partition
// still holds the last stable instance; the drain window then elapses and the
// sentinel releases it.
func TestDispatch_CutoverMovesTrafficBeforeReleasingStableCapacity(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), i32(60))
	isvc.Namespace, isvc.Name = ns, "cutover"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc)
	start := time.Unix(1000, 0)
	// The final step was just entered: the split served 50/50 at step 0.
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:              activeCanaryTargetID(isvc, g),
		CanaryRevisionHash:    "new",
		StableRevisionHash:    "old",
		CurrentStep:           1,
		StepEnteredTime:       &metav1.Time{Time: start},
		ObservedTrafficWeight: 50,
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePending)
	setTraffic(isvc, v1beta1.EngineComponent, coordination.BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent,
		canaryWeights("new", "old", "", "", 50)))

	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	// The held instance is still on stable; every other instance is Ready
	// on the canary revision.
	objs := []runtime.Object{isvc, engineIR,
		canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-1"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-2"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-3"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g, Now: start.Add(time.Minute)}
	ext := &isvc.Spec.Engine.ComponentExtensionSpec

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (cutover): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePromoting {
		t.Fatalf("staged capacity Ready must shift 100%% traffic, got phase %q", got)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("the last stable instance stays held while traffic settles, got partition %v", fmtPartition(p))
	}

	deps.Now = deps.Now.Add(61 * time.Second)
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (drain elapsed): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseStable {
		t.Fatalf("drain elapsed must complete the canary, got phase %q", got)
	}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 0 {
		t.Fatalf("completion must release the last stable instance, got partition %v", fmtPartition(p))
	}
}

// A secondary's capacity gate at the final step asks for every instance but
// the one its own hold keeps on stable, as the primary's does.
func TestSecondaryCapacityReady_FinalStepStagesAllButTheHeldInstance(t *testing.T) {
	ns := "default"
	n4 := 4
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pd.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "rtr", StableRevisionHash: "rtrOld", CurrentStep: 1}
	engineIR := ir(ns, "pd", v1beta1.EngineComponent, "eng")
	engineIR.Status.CurrentRevision = "pd-engine-engOld"
	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
		WithRuntimeObjects(ir(ns, "pd", v1beta1.RouterComponent, "rtr"), engineIR).Build()
	staged := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 3, "rtrOld": 1},
		v1beta1.EngineComponent: {"eng": 3, "engOld": 1},
	}
	if !mustSecondaryReady(t, context.Background(), reads, pd, staged, staged, v1beta1.RouterComponent) {
		t.Fatal("final step: a secondary with every instance but the held one Ready on its canary revision is staged")
	}
	short := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 3, "rtrOld": 1},
		v1beta1.EngineComponent: {"eng": 2, "engOld": 2},
	}
	if mustSecondaryReady(t, context.Background(), reads, pd, short, short, v1beta1.RouterComponent) {
		t.Fatal("final step: a secondary short of its staged count is not ready")
	}
}

func intPtr(v int) *int { return &v }

// fmtPartition renders a projected partition for a failure message.
func fmtPartition(p *int32) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.Itoa(int(*p))
}
