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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/revision"
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
	cr, _, err := revision.EnsureControllerRevision(context.Background(), c, c, isvc, component, spec, nil, nil)
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
	cr, _, err := revision.EnsureControllerRevision(context.Background(), c, c, isvc, component, spec, nil, nil)
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
		{"empty strategy defaults to recreate (SurgeThenDrain delegates)", "", runningSpec, imageOnlyTarget, updateModeRecreate, false},
		{"SurgeThenDrain delegates to recreate", v1beta1.OMENativeUpdateStrategySurgeThenDrain, runningSpec, imageOnlyTarget, updateModeRecreate, false},
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

// Ensure types still compose — keeps `fmt` import live when test bodies
// remove their string-formatted assertions.
var _ = fmt.Sprint
var _ = metav1.NewTime
