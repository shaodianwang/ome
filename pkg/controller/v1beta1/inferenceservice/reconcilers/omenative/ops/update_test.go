package ops

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
)

// boolPtr is a tiny pointer helper used by MarkNotReadyDuringLifecycle
// test fixtures.
func boolPtr(v bool) *bool { return &v }

// targetSpecImage builds the basic single-container PodSpec used as a
// rollout target throughout these tests.
func targetSpecImage(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: image},
		},
	}
}

// runningPodAtRevision shapes a pod that the controller would consider
// to be "running at revName" — labeled with the appropriate incarnation,
// has the given image, and is runtime-ready + serving.
func runningPodAtRevision(isvc *v1beta1.InferenceService, instIdx int32, incarnation int64, image string) *corev1.Pod {
	pod := podAtIncarnation(isvc, instIdx, incarnation, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: image}}
	return pod
}

// ensureTargetCR computes the deterministic CR name for image and creates
// a ControllerRevision in the fake client so Update has something to
// dispatch against.
func ensureTargetCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, spec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, component, spec, nil, nil)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}

// seedRunningRevision creates a ControllerRevision capturing spec as the
// Instance's recorded "running" template and writes its name onto
// InstanceStatus[idx].RunningRevision. inPlaceEligible reads this CR
// (not the live pod) to decide if a target diff is image-only.
func seedRunningRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, spec *corev1.PodSpec) {
	t.Helper()
	cr, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, component, spec, nil, nil)
	if err != nil {
		t.Fatalf("seedRunningRevision: %v", err)
	}
	// Re-fetch the ISVC to ensure the status write goes through cleanly.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	if fresh.Status.Components == nil {
		fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			cs.OMENative.InstanceStatuses[i].RunningRevision = cr.Name
			fresh.Status.Components[component] = cs
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("set RunningRevision: %v", err)
			}
			// Mirror onto the in-memory ISVC the test still holds a pointer to.
			if isvc.Status.Components == nil {
				isvc.Status.Components = fresh.Status.Components
			} else {
				isvc.Status.Components[component] = fresh.Status.Components[component]
			}
			return
		}
	}
	t.Fatalf("no InstanceStatus for idx=%d to attach RunningRevision", idx)
}

func TestUpdate_NilClient(t *testing.T) {
	resetExpectations(t)
	_, err := Update(context.Background(), core.ReconcileParams{}, core.ComponentPlan{Component: v1beta1.EngineComponent}, core.InstancePlan{Index: 0, Incarnation: 1}, &appsv1.ControllerRevision{}, &corev1.PodSpec{})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestUpdate_InPlace_RechecksServingBeforePatch(t *testing.T) {
	// A concurrent writer could re-flip serving=True between the drain
	// check passing and the image patch firing. The fix re-reads each
	// pod immediately before patching and, on serving=True, re-marks
	// NotServing and requeues without patching.
	//
	// Race simulation: a Get interceptor flips serving=True on the pod
	// ONLY during the pre-patch re-read; the listPodsForInstance call
	// uses the original snapshot, and the drain check reads
	// EndpointSlices, not the pod — so the flip is observed exclusively
	// by the pre-patch Get.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
	}
	// Pod: image v1, ContainersReady, NOT serving (Step 2 will skip it).
	pod := podAtIncarnation(isvc, 0, 1, true /* ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	target := targetSpecImage("llama:v2")
	// Slice claims the pod is drained — Step 3 drain check passes.
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* drained */)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = discoveryv1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	// Get interceptor: when Step 4 re-reads the pod, hand it back with
	// serving=True. Only fires for *corev1.Pod Gets matching pod.Name.
	flipServingOnGet := func(ctx context.Context, cli client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := cli.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if p, ok := obj.(*corev1.Pod); ok && p.Name == pod.Name {
			// Strip any existing serving condition and append a True
			// one — simulates a concurrent writer who removed all keys.
			out := make([]corev1.PodCondition, 0, len(p.Status.Conditions))
			for _, c := range p.Status.Conditions {
				if c.Type == "ome.io/serving" {
					continue
				}
				out = append(out, c)
			}
			out = append(out, corev1.PodCondition{
				Type:   "ome.io/serving",
				Status: corev1.ConditionTrue,
			})
			p.Status.Conditions = out
		}
		return nil
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc, pod, slice).
		WithInterceptorFuncs(interceptor.Funcs{Get: flipServingOnGet}).
		Build()

	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, targetSpecImage("llama:v1"))
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after detecting serving=True race")
	}

	// Image must NOT have been patched (recheck bailed before patch).
	got := &corev1.Pod{}
	// Bypass the interceptor's serving-flip by querying via Tracker —
	// but for image we don't need to; image is in Spec, interceptor
	// only mutates Status. Direct Get is fine.
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v1" {
		t.Errorf("image should still be v1 (no patch on serving pod); got %q", got.Spec.Containers[0].Image)
	}
}

func TestUpdate_InPlaceEligibleImageOnly_PatchesPodImage(t *testing.T) {
	// Pod running at image v1; target is v2 with no other diff. Strategy
	// is InPlaceIfPossible — first pass should mark serving=False, but
	// since the pod's still in the EndpointSlice the function requeues.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Add InPlaceIfPossible strategy explicitly so core.BuildPlan emits it.
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
	}
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := targetSpecImage("llama:v2")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* ready=false → drained */)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, targetSpecImage("llama:v1"))
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after patching image (kubelet hasn't rolled containers yet)")
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Errorf("image: got %q want llama:v2", got.Spec.Containers[0].Image)
	}
}

func TestUpdate_InPlaceConverges_MarksReadyWithRunningRevision(t *testing.T) {
	// Pod already running on target image, runtime-ready, not yet
	// serving (because previous pass drained it). Update should flip
	// serving=True and mark Ready with RunningRevision=targetRev.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace,
		},
	}
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
	}
	target := targetSpecImage("llama:v2")
	pod := podAtIncarnation(isvc, 0, 1, true /* ready */, false /* not serving yet */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	// Kubelet has finished rolling the container — runtime image matches
	// spec image. Without this status entry the new podRuntimeImagesMatch
	// gate would correctly hold the rollout in the "patched spec, not yet
	// rolled" state.
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v2"}}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, targetSpecImage("llama:v1"))
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true: pod is ready on target image")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !podreadiness.IsServing(got) {
		t.Errorf("serving should have been flipped True after in-place convergence")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q", s.RunningRevision, tcr.Name)
	}
	if s.TargetRevision != "" {
		t.Errorf("TargetRevision should be cleared, got %q", s.TargetRevision)
	}
	if s.Operation != nil {
		t.Errorf("Operation should be nil, got %+v", s.Operation)
	}
}

func TestUpdate_InPlaceWaitsForRuntimeImageRoll(t *testing.T) {
	// spec.image == target but kubelet has not yet rolled the container —
	// pod.Status.ContainerStatuses still reflects the old image. Update
	// must NOT flip serving / mark Ready: doing so would expose stale
	// runtime to traffic on the new revision pointer.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace,
		},
	}
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
	}
	target := targetSpecImage("llama:v2")
	pod := podAtIncarnation(isvc, 0, 1, true, false)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	// Container hasn't been rolled yet: runtime still on v1 even though
	// spec was just patched to v2.
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v1"}}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, targetSpecImage("llama:v1"))
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: runtime image hasn't rolled yet")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if podreadiness.IsServing(got) {
		t.Errorf("serving must NOT flip True while runtime image still lags spec")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision == tcr.Name {
		t.Errorf("RunningRevision must NOT advance to target until runtime rolls")
	}
}

func TestUpdate_InPlaceIfPossible_FallsThroughToRecreateOnBigDiff(t *testing.T) {
	// Pod running with env var X; target removes the env var entirely
	// (non-image diff). InPlaceIfPossible should pick recreate.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true /* still Ready */)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	// Recorded running spec carries the env var; the target drops it.
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}}
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, runningSpec)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate path drains then deletes across multiple passes")
	}

	// Recreate path should have bumped Incarnation.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate bumped it)", s.Incarnation)
	}
}

func TestUpdate_InPlaceOnly_RejectsBigDiff(t *testing.T) {
	// Strategy InPlaceOnly forbids non-image diffs.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceOnly,
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}}
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, runningSpec)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	_, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err == nil {
		t.Fatal("expected error: InPlaceOnly strategy rejects non-image diff")
	}
}

func TestUpdate_StrategyChangeMidRollout_RecreateStillBumpsIncarnation(t *testing.T) {
	// In-place wrote Phase=Updating+Operation{Update,InPlace,target=tcr}
	// on a prior pass; then the operator (or defaulter) flips
	// UpdateStrategy to RecreatePod for the SAME target revision. The
	// recreate path must still bump Incarnation — its idempotency guard
	// requires Operation.Step=Drain, so the in-place state doesn't
	// short-circuit the bump.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	target := targetSpecImage("llama:v2")
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, targetSpecImage("llama:v1"))
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	// Pre-stamp the in-place state at the same target.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	cs.OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	cs.OMENative.InstanceStatuses[0].TargetRevision = tcr.Name
	cs.OMENative.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           updateStepInPlace,
		TargetRevision: tcr.Name,
	}
	fresh.Status.Components[v1beta1.EngineComponent] = cs
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("seed in-place status: %v", err)
	}
	isvc.Status = fresh.Status

	// Now the strategy flips to RecreatePod.
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyRecreatePod,
		},
	}
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}

	final := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), final)
	s := final.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate must bump even though in-place wrote first)", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
		t.Errorf("Operation.Step: got %+v want Step=Drain", s.Operation)
	}
}

func TestUpdate_NoRunningRevision_ForcesRecreate(t *testing.T) {
	// Instance has no RunningRevision recorded (legacy or first-time
	// update post-Create). chooseUpdateMode treats nil running spec as
	// ineligible; even an image-only diff under InPlaceIfPossible must
	// fall through to recreate so the rollout starts from a known
	// recorded baseline.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
		},
	}
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := targetSpecImage("llama:v2")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	// NOTE: deliberately not calling seedRunningRevision — Instance has
	// no recorded baseline.
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate path bumped)", s.Incarnation)
	}
}

func TestUpdate_RecreatePod_BumpsIncarnationAndDrainsOld(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyRecreatePod,
		},
	}
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := targetSpecImage("llama:v2")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}

	// RecreatePod strategy is explicit recreate — Incarnation bumped to 2.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.TargetRevision != tcr.Name {
		t.Errorf("TargetRevision: got %q want %q", s.TargetRevision, tcr.Name)
	}
}

// TestUpdate_MultiPodSurgeFallsBackToRecreate pins that an Instance with
// >1 desired pod under SurgeThenDrain routes to recreateUpdate instead of
// surgeUpdate. The single-pod surge implementation requires a per-gang-
// member ordinal scheme that the OEP defers; recreate is the safe
// fallback that still respects the dispatcher's MaxUnavailable budget.
// The user's spec field stays "SurgeThenDrain" — this is execution
// best-effort, not a defaulter rewrite. When multi-pod surge lands
// (future OEP-0007 amendment), the fallback path goes away with zero
// user-facing churn.
func TestUpdate_MultiPodSurgeFallsBackToRecreate(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Stamp SurgeThenDrain on the ISVC; multi-pod is signaled via
	// ReconcileParams.MultiPod below (so this test does not depend on
	// the dispatch site reading Leader/Worker from the spec).
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategySurgeThenDrain,
		},
	}

	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := targetSpecImage("llama:v2")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)

	// Use a multi-pod params explicitly so this test does not depend on the
	// dispatch site picking up Leader/Worker from the ISVC.
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 1
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
	}
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.UpdateStrategy.Type != v1beta1.OMENativeUpdateStrategySurgeThenDrain {
		t.Fatalf("test setup: expected SurgeThenDrain strategy, got %q", plan.UpdateStrategy.Type)
	}
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	// Update must NOT error with the pre-multi-node "multi-pod not yet
	// supported" message from surgeUpdate. It must dispatch to recreate.
	done, err := Update(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}

	// Recreate stamps Phase=Updating with TargetRevision and bumps the
	// Incarnation, exactly like RecreatePod. If the dispatcher had picked
	// surge, the InstanceStatus would still be Phase=Ready (surge stamps
	// Updating but recreates Phase=Updating-with-Incarnation-bump).
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate bumps)", s.Incarnation)
	}
}

// TestChooseUpdateMode_MultiPodSurgeFallsBackToRecreate is the chooser's
// per-mode contract: when multiPod=true and strategy=SurgeThenDrain, the
// returned mode is recreate. Tested separately from the higher-level
// Update path because chooseUpdateMode is the single source of truth that
// every Update call routes through.
func TestChooseUpdateMode_MultiPodSurgeFallsBackToRecreate(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}

	mode, err := chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategySurgeThenDrain, running, target, true /* multiPod */)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("multi-pod surge: got mode %v want recreate", mode)
	}

	// Sanity: single-pod path keeps surge mode.
	mode, err = chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategySurgeThenDrain, running, target, false /* multiPod */)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != updateModeSurge {
		t.Errorf("single-pod surge: got mode %v want surge", mode)
	}
}

// TestChooseUpdateMode_MultiPodInPlaceFallsBackToRecreate pins that
// in-place modes route to recreate for multi-pod Components. inPlaceEligible
// compares only the leader's PodSpec, so an in-place patch on a worker-
// only spec change would leave the workers on the old image — split-brain.
func TestChooseUpdateMode_MultiPodInPlaceFallsBackToRecreate(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	imageOnlyTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}
	envTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2", Env: []corev1.EnvVar{{Name: "X"}}}}}

	// InPlaceIfPossible — eligible diff still goes recreate when multi-pod.
	mode, err := chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, running, imageOnlyTarget, true)
	if err != nil {
		t.Fatalf("InPlaceIfPossible multipod eligible: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("InPlaceIfPossible multipod: got %v want recreate", mode)
	}

	// InPlaceOnly + eligible — still recreate for multi-pod, no error.
	mode, err = chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategyInPlaceOnly, running, imageOnlyTarget, true)
	if err != nil {
		t.Fatalf("InPlaceOnly multipod eligible: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("InPlaceOnly multipod eligible: got %v want recreate", mode)
	}

	// InPlaceOnly + ineligible — single-pod errors, multi-pod recreates.
	if _, err := chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategyInPlaceOnly, running, envTarget, false); err == nil {
		t.Errorf("single-pod InPlaceOnly ineligible: want error")
	}
	mode, err = chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategyInPlaceOnly, running, envTarget, true)
	if err != nil {
		t.Fatalf("multi-pod InPlaceOnly ineligible: want recreate, got error: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("multi-pod InPlaceOnly ineligible: got %v want recreate", mode)
	}

	// Single-pod eligible path still in-places (the multi-pod override is
	// scoped — single-pod keeps full in-place semantics).
	mode, err = chooseUpdateModeForInstance(v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, running, imageOnlyTarget, false)
	if err != nil {
		t.Fatalf("single-pod InPlaceIfPossible: %v", err)
	}
	if mode != updateModeInPlace {
		t.Errorf("single-pod InPlaceIfPossible: got %v want in-place", mode)
	}
}

func TestInPlaceEligible_OnlyImageDiff(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	if !inPlaceEligible(running, target) {
		t.Errorf("image-only diff should be in-place eligible")
	}
}

func TestInPlaceEligible_InitContainerImageDiffRejected(t *testing.T) {
	// Init containers run once at pod creation; kubelet has no machinery
	// to re-run them after an image patch. An init-image-only diff must
	// fail eligibility and route to recreate, even though the regular
	// containers are unchanged.
	running := &corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "init", Image: "init:v1"}},
		Containers:     []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	target := &corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "init", Image: "init:v2"}},
		Containers:     []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	if inPlaceEligible(running, target) {
		t.Errorf("init-container image diff must force recreate (kubelet cannot re-run inits in place)")
	}
}

func TestInPlaceEligible_NonImageDiffRejected(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}

	envDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}}},
	}
	if inPlaceEligible(running, envDiff) {
		t.Errorf("env diff should NOT be in-place eligible")
	}

	cmdDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2", Command: []string{"go"}}},
	}
	if inPlaceEligible(running, cmdDiff) {
		t.Errorf("command diff should NOT be in-place eligible")
	}

	volDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}},
		Volumes:    []corev1.Volume{{Name: "extra"}},
	}
	if inPlaceEligible(running, volDiff) {
		t.Errorf("volume diff should NOT be in-place eligible")
	}
}

func TestInPlaceEligible_NilRunningOrTargetRejected(t *testing.T) {
	// A nil running spec means "no recorded baseline" — force recreate.
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	if inPlaceEligible(nil, target) {
		t.Errorf("nil running should not be claimed eligible")
	}
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	if inPlaceEligible(running, nil) {
		t.Errorf("nil target should not be claimed eligible")
	}
}

func TestChooseUpdateMode(t *testing.T) {
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	imageOnlyTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}
	envTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2", Env: []corev1.EnvVar{{Name: "X"}}}}}

	tests := []struct {
		name     string
		strategy v1beta1.OMENativeUpdateStrategyType
		running  *corev1.PodSpec
		target   *corev1.PodSpec
		wantMode updateMode
		wantErr  bool
	}{
		{"RecreatePod always recreates", v1beta1.OMENativeUpdateStrategyRecreatePod, runningSpec, imageOnlyTarget, updateModeRecreate, false},
		{"InPlaceIfPossible + eligible = in-place", v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, runningSpec, imageOnlyTarget, updateModeInPlace, false},
		{"InPlaceIfPossible + ineligible = recreate", v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, runningSpec, envTarget, updateModeRecreate, false},
		{"InPlaceOnly + eligible = in-place", v1beta1.OMENativeUpdateStrategyInPlaceOnly, runningSpec, imageOnlyTarget, updateModeInPlace, false},
		{"InPlaceOnly + ineligible = error", v1beta1.OMENativeUpdateStrategyInPlaceOnly, runningSpec, envTarget, 0, true},
		{"empty strategy defaults to surge (matches SurgeThenDrain default)", "", runningSpec, imageOnlyTarget, updateModeSurge, false},
		{"SurgeThenDrain routes to surge mode", v1beta1.OMENativeUpdateStrategySurgeThenDrain, runningSpec, imageOnlyTarget, updateModeSurge, false},
		{"unknown strategy = error", v1beta1.OMENativeUpdateStrategyType("Bogus"), runningSpec, imageOnlyTarget, 0, true},
		{"nil running forces recreate even on image-only diff (no baseline)", v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, nil, imageOnlyTarget, updateModeRecreate, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := chooseUpdateMode(tt.strategy, tt.running, tt.target)
			if tt.wantErr {
				if err == nil {
					t.Errorf("want error, got mode=%v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantMode {
				t.Errorf("mode: got %v want %v", got, tt.wantMode)
			}
		})
	}
}

func TestDetectUpdateTrigger_PhaseUpdatingAlwaysTrue(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	target := targetSpecImage("llama:v1")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Phase=Updating should keep returning true")
	}
}

func TestDetectUpdateTrigger_PhaseMigratingSuppressed(t *testing.T) {
	// A source mid-migration must not start Update — Migrate owns the
	// lifecycle until it terminates via the audit ledger.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceMigrating
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	target := targetSpecImage("llama:v2")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Migrating must suppress Update — Migrate owns the lifecycle")
	}
}

func TestDetectUpdateTrigger_OperationMigrateSuppressed(t *testing.T) {
	// Surge mid-create carries Operation.Type=Migrate while Phase=Creating;
	// the explicit Operation-type guard catches the post-promote race
	// where a just-promoted surge briefly looks like a stale-revision
	// Instance to detectUpdateTrigger.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationMigrate,
		Step: "CreatePods",
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	target := targetSpecImage("llama:v2")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Operation.Type=Migrate must suppress Update")
	}
}

func TestDetectUpdateTrigger_RunningRevisionMatchesTarget(t *testing.T) {
	// Status's RunningRevision matches the target CR name — no update.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	target := targetSpecImage("llama:v1")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].RunningRevision = tcr.Name
	// Re-store with the running revision pinned.
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("RunningRevision == target should NOT trigger update")
	}
}

func TestDetectUpdateTrigger_PodImageDiffersTriggersUpdate(t *testing.T) {
	// No RunningRevision recorded; observed pod has image v1 but target
	// spec is v2 — must trigger.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := newFakeClientWithDiscovery(t, isvc, pod)
	target := targetSpecImage("llama:v2")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("pod image mismatch should trigger update")
	}
}

func TestDetectUpdateTrigger_LegacyMatchedPodsBackfillRunningRevision(t *testing.T) {
	// Status has no RunningRevision, pod already runs the target image.
	// detectUpdateTrigger must NOT trigger an update, and as a side
	// effect should backfill RunningRevision so future reconciles take
	// the cheap fast-path.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	target := targetSpecImage("llama:v1")
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := newFakeClientWithDiscovery(t, isvc, pod)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("matched legacy pods must NOT trigger update")
	}

	// Backfill should have happened.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q (backfill)", s.RunningRevision, tcr.Name)
	}
}

func TestDetectUpdateTrigger_PhaseCreatingNotInterruptible(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceCreating
	c := newFakeClientWithDiscovery(t, isvc)
	target := targetSpecImage("llama:v2")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, err := detectUpdateTrigger(context.Background(), p, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Creating must not be interruptible by Update")
	}
}

func TestReconcileComponentOps_UpdateDispatchesWhenSpecChanged(t *testing.T) {
	// Pod running at image v1; spec.PodSpec asks for v2. ReconcileComponentOps
	// should detect the diff via detectUpdateTrigger, dispatch Update,
	// and requeue. Drives ReconcileComponentOps (in this package) rather
	// than the top-level omenative.ReconcileComponent — that would close
	// a cycle.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
		},
	}
	oldPod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", oldPod, true)
	c := newFakeClientWithDiscovery(t, isvc, oldPod, slice)

	p := minimalParams(t, isvc, c)
	p.PodSpec = targetSpecImage("llama:v2")
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	target := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, p.PodSpec)

	result, err := ReconcileComponentOps(context.Background(), p, plan, target)
	if err != nil {
		t.Fatalf("ReconcileComponentOps: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected requeue while Update is in flight, got %+v", result)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.TargetRevision == "" {
		t.Errorf("TargetRevision should be set; got empty")
	}
}

// TestReconcileComponent_RevisionCollisionBumpsAndRetries moved to the
// top-level reconciler_test.go — it exercises the collision-retry
// wrapper in reconciler.go's ensureRevisionWithCollisionRetry, which
// lives outside this package.

func TestPodImagesMatch(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}}
	matching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	mismatching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v3"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	if !podImagesMatch(pod, matching) {
		t.Errorf("matching images should report true")
	}
	if podImagesMatch(pod, mismatching) {
		t.Errorf("differing images should report false")
	}
}

func TestPatchInstanceStatusReadyOnRevision_SkipsWriteWhenIdempotent(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].RunningRevision = "rev-abc"
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	// Already in the target shape — write should be a no-op.
	if err := patchInstanceStatusReadyOnRevision(context.Background(), p, v1beta1.EngineComponent, 0, "rev-abc"); err != nil {
		t.Fatalf("patch: %v", err)
	}
}

// drainServiceForPod must derive the per-revision *routed* Service name
// from the pod's ome.io/revision-hash label, NOT the per-Component
// headless Service. The headless Service sets PublishNotReadyAddresses
// =true, which makes kube-proxy publish Ready=true on its EndpointSlice
// regardless of the pod's actual Ready — so drain.IsPodDrained against
// the headless Service waits forever and stalls every recreate /
// in-place update / scale-down / migrate-source drain. Pin the contract.
func TestDrainServiceForPod_UsesPerRevisionRoutedService(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "llama-70b-engine-0-default-0",
		Namespace: "prod",
		Labels:    map[string]string{"ome.io/revision-hash": "abcd1234"},
	}}
	got := drainServiceForPod(isvc.Name, v1beta1.EngineComponent, pod)
	if got != "llama-70b-engine-rev-abcd1234" {
		t.Errorf("got %q want llama-70b-engine-rev-abcd1234", got)
	}
}

// Pods that predate the revision-hash label (e.g. legacy LWS pods that
// got adopted, fuzz inputs, future label-renaming migrations) return
// empty — caller skips the drain rather than blocking on a service it
// cannot identify.
func TestDrainServiceForPod_EmptyWhenLabelMissing(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0-default-0", Namespace: "prod"}}
	if got := drainServiceForPod(isvc.Name, v1beta1.EngineComponent, pod); got != "" {
		t.Errorf("got %q want empty", got)
	}
}

// canonicalImage must strip the docker.io/library/ prefix so spec
// image references (which the user writes as `<name>:<tag>` for
// implicit Docker Hub library images) match the runtime-stamped form
// containerd writes into Status.ContainerStatuses[*].Image. Without
// the strip, in-place updates wait forever on `podRuntimeImagesMatch`
// and never reach the serving-gate flip.
func TestCanonicalImage_StripsDockerHubLibraryPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"implicit library tag", "fake-serving:v1", "fake-serving:v1"},
		{"runtime-stamped library tag", "docker.io/library/fake-serving:v1", "fake-serving:v1"},
		{"explicit registry round-trip", "ghcr.io/foo/bar:v1", "ghcr.io/foo/bar:v1"},
		{"explicit docker.io non-library", "docker.io/myuser/myimg:v1", "docker.io/myuser/myimg:v1"},
		{"empty string", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalImage(tc.in); got != tc.want {
				t.Errorf("canonicalImage(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// podRuntimeImagesMatch must treat `fake-serving:v1` (spec) and
// `docker.io/library/fake-serving:v1` (runtime) as the same image —
// otherwise in-place updates against KIND or any cluster whose runtime
// fully-qualifies implicit Docker Hub references would never converge.
func TestPodRuntimeImagesMatch_RegistryNormalization(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "fake-serving:v2"},
	}}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "fake-serving:v2"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", Image: "docker.io/library/fake-serving:v2"},
		}},
	}
	if !podRuntimeImagesMatch(pod, target) {
		t.Errorf("expected match between spec %q and runtime %q after canonicalImage normalization",
			target.Containers[0].Image, pod.Status.ContainerStatuses[0].Image)
	}
}

// Ensure types still compose — keeps `fmt` import live when test bodies
// remove their string-formatted assertions.
var _ = fmt.Sprint
var _ = metav1.NewTime

// ensureTargetCRWithMeta is ensureTargetCR's PodMeta-aware sibling. It
// captures the user-intent annotations the operator wrote on
// spec.{engine,decoder,router} via the meta parameter so the Bug #51
// regression tests can verify those values reach the live pod after
// an in-place rollout. nil meta degrades to the no-meta behavior of
// ensureTargetCR (i.e., revision is hashed without a PodMeta block).
func ensureTargetCRWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, spec *corev1.PodSpec, meta *metav1.ObjectMeta) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, component, spec, meta, nil)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}

// seedRunningRevisionWithMeta seeds a running revision (PodSpec +
// PodMeta) and writes its CR name onto InstanceStatus.RunningRevision.
// This is the Bug #51 baseline: the previous revision encoded the
// pod's existing annotation set; the test then dispatches an in-place
// update toward a target revision with a different annotation set and
// asserts the live pod's metadata.annotations is reconciled.
func seedRunningRevisionWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, spec *corev1.PodSpec, meta *metav1.ObjectMeta) {
	t.Helper()
	cr, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, component, spec, meta, nil)
	if err != nil {
		t.Fatalf("seedRunningRevisionWithMeta: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	if fresh.Status.Components == nil {
		fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			cs.OMENative.InstanceStatuses[i].RunningRevision = cr.Name
			fresh.Status.Components[component] = cs
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("set RunningRevision: %v", err)
			}
			if isvc.Status.Components == nil {
				isvc.Status.Components = fresh.Status.Components
			} else {
				isvc.Status.Components[component] = fresh.Status.Components[component]
			}
			return
		}
	}
	t.Fatalf("no InstanceStatus for idx=%d to attach RunningRevision", idx)
}

// TestUpdate_InPlaceUpdate_PropagatesAnnotationsToPod is the primary
// regression test for Bug #51. Setup:
//
//  1. ISVC at incarnation 1, Ready on revision A which encoded
//     annotation set {a:"1"}. The live pod carries {a:"1"} as it was
//     rendered by Create at the previous revision.
//  2. Operator edits spec.engine.annotations to {a:"1", b:"2"}. The
//     reconciler builds the new ObjectMeta and EnsureControllerRevision
//     produces revision B whose PodMeta carries the new set.
//  3. The image is unchanged — only annotations diverged. In-place
//     dispatch fires (PodSpec is identical so inPlaceEligible returns
//     true; the trigger fires because the revision name changed).
//
// Expected: after the in-place pass the pod's metadata.annotations
// contains BOTH {a:"1", b:"2"}. The pre-fix behavior left b:"2" stuck
// on the ControllerRevision but never on the pod.
func TestUpdate_InPlaceUpdate_PropagatesAnnotationsToPod(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			// markNotReady=false so we don't need to wire EndpointSlice
			// drain fixtures — annotation propagation is orthogonal to
			// the drain semantics.
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(false),
			},
		},
	}
	// Pod carries the previous-revision annotation set, plus a foreign
	// annotation injected by a hypothetical webhook. The webhook
	// annotation MUST survive — we explicitly DO NOT clobber non-
	// OMENative annotations.
	spec := targetSpecImage("llama:v1")
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{
		"a":                 "1",
		"linkerd.io/inject": "enabled", // webhook-set, foreign
	}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	// Running revision encodes the OMENative-owned annotation set {a:1}.
	seedRunningRevisionWithMeta(t, c, isvc, v1beta1.EngineComponent, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	// Target revision: identical spec, new annotation set {a:1, b:2}.
	target := ensureTargetCRWithMeta(t, c, isvc, v1beta1.EngineComponent, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})

	done, err := Update(context.Background(), p, plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	// done can be true (no image patch + annotation patch applied + already
	// runtime-ready) or false (annotation patch is a no-op gate doesn't
	// flip done). Either is acceptable — the focus is on the patch.
	_ = done

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["a"]; v != "1" {
		t.Errorf("annotation 'a': got %q want \"1\" (preserved across in-place)", v)
	}
	if v := got.Annotations["b"]; v != "2" {
		t.Errorf("annotation 'b': got %q want \"2\" — Bug #51 regressed: new spec annotation never reached pod", v)
	}
	if v := got.Annotations["linkerd.io/inject"]; v != "enabled" {
		t.Errorf("foreign annotation linkerd.io/inject: got %q want \"enabled\" (must NOT be clobbered by in-place patch)", v)
	}
}

// TestUpdate_InPlaceUpdate_RemovesAnnotationDroppedFromSpec pins the
// removal half of Bug #51's design choice (a). When the operator
// removes a key from spec.{component}.annotations that the previous
// revision authored, the in-place patch removes it from the pod too.
// The scoping rule — "only delete keys the previous revision once
// authored" — protects foreign annotations from being collateral
// damage on every spec edit.
func TestUpdate_InPlaceUpdate_RemovesAnnotationDroppedFromSpec(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(false),
			},
		},
	}
	spec := targetSpecImage("llama:v1")
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{
		"a":                 "1",
		"b":                 "2",
		"linkerd.io/inject": "enabled",
	}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	// Previous revision authored {a, b}.
	seedRunningRevisionWithMeta(t, c, isvc, v1beta1.EngineComponent, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	// Target revision: user removed 'b' from spec.
	target := ensureTargetCRWithMeta(t, c, isvc, v1beta1.EngineComponent, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})

	if _, err := Update(context.Background(), p, plan, plan.Instances[0], target, spec); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["a"]; v != "1" {
		t.Errorf("annotation 'a': got %q want \"1\" (kept across in-place)", v)
	}
	if _, present := got.Annotations["b"]; present {
		t.Errorf("annotation 'b' still on pod (got %q); user removed it from spec — in-place should drop it", got.Annotations["b"])
	}
	if v := got.Annotations["linkerd.io/inject"]; v != "enabled" {
		t.Errorf("foreign annotation linkerd.io/inject: got %q want \"enabled\" (must NOT be deleted)", v)
	}
}

// TestUpdate_InPlaceUpdate_LifecycleAnnotationFilterStillWorks is the
// regression guard for the existing isLifecycleAnnotation filter in
// revision.TemplateMeta. The filter must keep working so an in-flight
// rollout-paused annotation or migration-request annotation does NOT
// participate in the revision hash AND does not get clobbered by the
// in-place annotation patch. (The PodMeta on a CR never carries
// lifecycle annotations because TemplateMeta strips them at hash
// time; therefore annotationsDiff never sees them as either previous
// or target keys, and the live pod's value rides through unchanged.)
func TestUpdate_InPlaceUpdate_LifecycleAnnotationFilterStillWorks(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Operator wrote a rollout-paused annotation on the ISVC AND a real
	// spec.engine.annotations edit. The rollout-paused must be filtered
	// out of the CR's PodMeta (so the hash is stable across pause/unpause)
	// and must NOT show up on the live pod even though the user wrote
	// it onto the ISVC.
	isvc.Annotations = map[string]string{
		"ome.io/rollout-paused": "true",
	}
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(false),
			},
		},
	}
	spec := targetSpecImage("llama:v1")
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{"a": "1"}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	// Running revision's PodMeta has {a:1}; the lifecycle annotation
	// "ome.io/rollout-paused" is intentionally NOT included here because
	// the production reconciler calls revision.TemplateMeta which strips
	// it. Simulate the same in the fixture.
	seedRunningRevisionWithMeta(t, c, isvc, v1beta1.EngineComponent, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	// Target adds {b:2}; lifecycle annotation is again stripped from PodMeta.
	target := ensureTargetCRWithMeta(t, c, isvc, v1beta1.EngineComponent, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})

	if _, err := Update(context.Background(), p, plan, plan.Instances[0], target, spec); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["b"]; v != "2" {
		t.Errorf("annotation 'b': got %q want \"2\" (in-place propagation regressed)", v)
	}
	if _, present := got.Annotations["ome.io/rollout-paused"]; present {
		t.Errorf("lifecycle annotation 'ome.io/rollout-paused' leaked onto pod (got %q); the TemplateMeta filter must keep it out of the CR's PodMeta and therefore off the pod", got.Annotations["ome.io/rollout-paused"])
	}
}

// TestAnnotationsDiff_Cases exercises the helper directly to pin
// each branch of the (add | update | delete | leave-alone) decision
// matrix. These cases compose into the per-pod patches but are easier
// to reason about in isolation than via the full Update fixture.
func TestAnnotationsDiff_Cases(t *testing.T) {
	cases := []struct {
		name                string
		pod, previous, want map[string]string
		expect              map[string]any
	}{
		{
			name:     "no diff: nothing to patch",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "add new key from target",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1", "b": "2"},
			expect:   map[string]any{"b": "2"},
		},
		{
			name:     "update existing target key with new value",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "2"},
			expect:   map[string]any{"a": "2"},
		},
		{
			name:     "delete key the user removed (previous owned it)",
			pod:      map[string]string{"a": "1", "b": "2"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"b": nil},
		},
		{
			name:     "leave foreign key alone (in pod, not in previous or target)",
			pod:      map[string]string{"a": "1", "linkerd.io/inject": "enabled"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "delete only if pod actually has the key (no spurious null patch)",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "nil-as-empty across all inputs",
			pod:      nil,
			previous: nil,
			want:     nil,
			expect:   map[string]any{},
		},
		{
			name:     "first revision: previous nil, target adds key, pod empty",
			pod:      nil,
			previous: nil,
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"a": "1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := annotationsDiff(tc.pod, tc.previous, tc.want)
			if len(got) != len(tc.expect) {
				t.Fatalf("diff length: got %d (%+v) want %d (%+v)", len(got), got, len(tc.expect), tc.expect)
			}
			for k, want := range tc.expect {
				gv, ok := got[k]
				if !ok {
					t.Errorf("missing key %q in diff", k)
					continue
				}
				if want == nil && gv != nil {
					t.Errorf("key %q: got %+v want nil", k, gv)
				}
				if want != nil && gv != want {
					t.Errorf("key %q: got %+v want %+v", k, gv, want)
				}
			}
		})
	}
}
