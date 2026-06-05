package ops

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// testISVCGVK is the GroupVersionKind for InferenceService owner refs
// (duplicated from omenative/core to keep workload/ops free of the
// dispatch-side helpers).
var testISVCGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// testKey produces the ISVC-form workload.Key the render path consumes:
// Namespace + OwnerName from the ISVC, Component cast across the type
// boundary, SelectorLabels = the OMENative pod-selector trio.
func testKey(isvc *v1beta1.InferenceService, component workload.ComponentType) workload.Key {
	return workload.Key{
		Namespace: isvc.Namespace,
		Component: component,
		OwnerName: isvc.Name,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// testRender wraps Render with the (isvc, podSpec, plan, inst, runner,
// ordinal) signature test bodies use directly, deriving the
// owner/key/hook arguments from the ISVC under test.
func testRender(isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32) (*corev1.Pod, error) {
	if isvc == nil {
		// Tests rely on Render rejecting a nil owner — pass nil
		// through to exercise that branch.
		return Render(nil, testISVCGVK, workload.Key{}, podSpec, plan, inst, runner, ordinal, nil)
	}
	return Render(isvc, testISVCGVK, testKey(isvc, plan.Component), podSpec, plan, inst, runner, ordinal, nil)
}

// testRenderWithRevision wraps RenderWithRevision with the (isvc,
// podSpec, podTemplateMeta, plan, inst, runner, ordinal, revisionHash)
// signature test bodies use directly.
func testRenderWithRevision(isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, meta *metav1.ObjectMeta, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, revisionHash string) (*corev1.Pod, error) {
	if isvc == nil {
		return RenderWithRevision(nil, testISVCGVK, workload.Key{}, podSpec, meta, plan, inst, runner, ordinal, revisionHash, nil)
	}
	return RenderWithRevision(isvc, testISVCGVK, testKey(isvc, plan.Component), podSpec, meta, plan, inst, runner, ordinal, revisionHash, nil)
}

func singlePodPlan() (workload.ComponentPlan, workload.InstancePlan, workload.RunnerPlan) {
	runner := workload.RunnerPlan{Name: "default", Size: 1}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{runner}}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst},
	}
	return plan, inst, runner
}

// multiPodPlan returns a 2-Runner Instance shape: 1 leader + workerSize
// workers. Used by the multi-pod render tests.
func multiPodPlan(workerSize int32) (workload.ComponentPlan, workload.InstancePlan, []workload.RunnerPlan) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: workerSize},
	}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{inst},
	}
	return plan, inst, runners
}

func basicISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("isvc-uid-1"),
		},
	}
}

func basicPodSpec() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "runner", Image: "llama:v1"},
		},
	}
}

func TestRender_NilInputsRejected(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	if _, err := testRender(nil, basicPodSpec(), plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
	if _, err := testRender(basicISVC(), nil, plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil PodSpec")
	}
}

func TestRender_StableNameAndDNS(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pod.Name != "llama-70b-engine-0-default-0" {
		t.Errorf("Name: got %q want llama-70b-engine-0-default-0", pod.Name)
	}
	if pod.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", pod.Namespace)
	}
	if pod.Spec.Hostname != "llama-70b-engine-0-default-0" {
		t.Errorf("Hostname: got %q", pod.Spec.Hostname)
	}
	if pod.Spec.Subdomain != "llama-70b-engine-headless" {
		t.Errorf("Subdomain: got %q want llama-70b-engine-headless", pod.Spec.Subdomain)
	}
}

func TestRender_Labels(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		query.LabelInstanceIdx:                "0",
		query.LabelInstanceIncarnation:        "1",
		query.LabelRunner:                     "default",
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 "0",
	}
	if diff := cmp.Diff(want, pod.Labels); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
}

// TestRender_SinglePod_OmitsPodGroupLabel pins that single-pod
// Instances do NOT carry the scheduler-plugins pod-group label. The
// gang-scheduling contract is clear: no PodGroup, no gang — and stamping
// the label without a matching PodGroup would put the scheduler-plugins
// coscheduler into a wait-for-non-existent-gang state.
func TestRender_SinglePod_OmitsPodGroupLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, present := pod.Labels[query.LabelPodGroup]; present {
		t.Errorf("%s must be absent on single-pod Instance, got %q", query.LabelPodGroup, got)
	}
}

// TestRender_MultiPod_StampsPodGroupLabel pins that every pod in a
// multi-pod Instance carries the scheduler-plugins pod-group label
// (`scheduling.x-k8s.io/pod-group=<isvc>-<comp>-<idx>`). The leader
// and every worker must agree on the same PodGroup name so the
// coscheduler treats them as one gang.
func TestRender_MultiPod_StampsPodGroupLabel(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	leader := runners[0]
	worker := runners[1]

	leaderPod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	worker0, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker 0: %v", err)
	}
	worker1, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 1)
	if err != nil {
		t.Fatalf("worker 1: %v", err)
	}

	const wantPodGroup = "llama-70b-engine-0"
	for _, p := range []*corev1.Pod{leaderPod, worker0, worker1} {
		got, present := p.Labels[query.LabelPodGroup]
		if !present {
			t.Errorf("%s: pod %s missing %s label, labels=%+v",
				p.Name, p.Name, query.LabelPodGroup, p.Labels)
			continue
		}
		if got != wantPodGroup {
			t.Errorf("%s %s: got %q want %q", p.Name, query.LabelPodGroup, got, wantPodGroup)
		}
	}
}

// TestRender_MultiPod_PodGroupLabelPerInstance pins that the
// pod-group label is per-Instance: Instance 0's pods carry one
// pod-group name, Instance 1's pods carry a different name. Otherwise
// the coscheduler would treat both Instances as one mega-gang and
// hang both until every member of both is schedulable.
func TestRender_MultiPod_PodGroupLabelPerInstance(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	inst0 := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	inst1 := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst0, inst1},
	}

	inst0Pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst0, runners[1], 0)
	if err != nil {
		t.Fatalf("inst0 worker: %v", err)
	}
	inst1Pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst1, runners[1], 0)
	if err != nil {
		t.Fatalf("inst1 worker: %v", err)
	}

	if inst0Pod.Labels[query.LabelPodGroup] != "llama-70b-engine-0" {
		t.Errorf("inst0 %s: got %q want llama-70b-engine-0",
			query.LabelPodGroup, inst0Pod.Labels[query.LabelPodGroup])
	}
	if inst1Pod.Labels[query.LabelPodGroup] != "llama-70b-engine-1" {
		t.Errorf("inst1 %s: got %q want llama-70b-engine-1",
			query.LabelPodGroup, inst1Pod.Labels[query.LabelPodGroup])
	}
	if inst0Pod.Labels[query.LabelPodGroup] == inst1Pod.Labels[query.LabelPodGroup] {
		t.Errorf("inst0 and inst1 must carry distinct pod-group names; both got %q",
			inst0Pod.Labels[query.LabelPodGroup])
	}
}

func TestRenderWithRevision_StampsRevisionHashLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), nil, plan, inst, runner, 0, "abc12345")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelRevisionHash]; got != "abc12345" {
		t.Errorf("%s: got %q want %q", query.LabelRevisionHash, got, "abc12345")
	}
}

func TestRenderWithRevision_EmptyHashOmitsRevisionLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), nil, plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("%s should be absent when hash is empty, got: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

func TestRender_DelegatesToRenderWithRevisionEmptyHash(t *testing.T) {
	// Render() is the back-compat entry point and must produce the same
	// pod as testRenderWithRevision(..., "") — i.e., no revision-hash label.
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("Render() must omit %s; got pod labels: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

// TestRenderWithRevision_PropagatesComponentMetaAnnotationsAndLabels pins
// the merge from the upstream-built componentObjectMeta onto the rendered
// pod. Before this fix the renderer dropped both, breaking PD runtimes
// that need multus IB annotations (k8s.v1.cni.cncf.io/networks) to spin
// up. OME-mandatory labels still win on collision so selectors keep
// working.
func TestRenderWithRevision_PropagatesComponentMetaAnnotationsAndLabels(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	meta := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"k8s.v1.cni.cncf.io/networks": "[{\"name\":\"ibs0p0-macvlan\",\"namespace\":\"cw-multus\"}]",
			"fb.metric.schema":            "sglang_metrics",
		},
		Labels: map[string]string{
			"team":                                "ml-infra",
			constants.InferenceServicePodLabelKey: "should-be-overwritten-by-OMENative",
		},
	}
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), meta, plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Annotations["k8s.v1.cni.cncf.io/networks"]; got == "" {
		t.Errorf("multus networks annotation missing: %+v", pod.Annotations)
	}
	if got := pod.Annotations["fb.metric.schema"]; got != "sglang_metrics" {
		t.Errorf("fb.metric.schema annotation: got %q want %q", got, "sglang_metrics")
	}
	if got := pod.Labels["team"]; got != "ml-infra" {
		t.Errorf("user-declared label dropped: %+v", pod.Labels)
	}
	if got := pod.Labels[constants.InferenceServicePodLabelKey]; got == "should-be-overwritten-by-OMENative" {
		t.Errorf("OMENative selector label was clobbered by user label; got %q (must be the ISVC name)", got)
	}
}

func TestRender_IncarnationStampedOnLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	inst.Incarnation = 7
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != "7" {
		t.Errorf("%s: got %q want 7", query.LabelInstanceIncarnation, got)
	}
}

func TestRender_OwnerReference(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(pod.OwnerReferences))
	}
	ref := pod.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

func TestRender_AppendsServingReadinessGateWithoutClobberingExisting(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := basicPodSpec()
	ps.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: "user.example.com/ready"},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 2 {
		t.Fatalf("ReadinessGates: got %d want 2", len(pod.Spec.ReadinessGates))
	}
	if pod.Spec.ReadinessGates[0].ConditionType != "user.example.com/ready" {
		t.Errorf("user gate clobbered: %+v", pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[1].ConditionType != query.ServingConditionType {
		t.Errorf("ome.io/serving not appended: %+v", pod.Spec.ReadinessGates)
	}
}

func TestRender_DoesNotDuplicateExistingServingGate(t *testing.T) {
	// If the user template already declares ome.io/serving, Render must
	// not append a duplicate — kubelet ANDs all gates and the controller
	// writes only one condition, so a duplicate keeps PodReady=False
	// forever.
	plan, inst, runner := singlePodPlan()
	ps := basicPodSpec()
	ps.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: query.ServingConditionType},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 1 {
		t.Fatalf("ReadinessGates duplicated: got %d want 1: %+v", len(pod.Spec.ReadinessGates), pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[0].ConditionType != query.ServingConditionType {
		t.Errorf("preserved gate type wrong: %v", pod.Spec.ReadinessGates[0].ConditionType)
	}
}

func TestRender_OMEEnvVarsInEveryContainer(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: "llama:v1"},
			{Name: "sidecar", Image: "metrics:v1"},
		},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		"OME_INFERENCESERVICE_NAME": "llama-70b",
		"OME_COMPONENT":             "engine",
		"OME_COMPONENT_REPLICAS":    "2",
		"OME_INSTANCE_INDEX":        "0",
		"OME_RUNNER":                "default",
		"OME_RUNNER_SIZE":           "1",
		"OME_RUNNER_INDEX":          "0",
		"OME_INSTANCE_SUBDOMAIN":    "llama-70b-engine-0",
	}
	for ci, c := range pod.Spec.Containers {
		got := envByName(c.Env)
		for k, v := range want {
			if got[k] != v {
				t.Errorf("container[%d].env[%s]: got %q want %q", ci, k, got[k], v)
			}
		}
		// OME_LEADER_ADDRESS only emitted for multi-pod; should not appear here.
		if _, ok := got["OME_LEADER_ADDRESS"]; ok {
			t.Errorf("container[%d].env: OME_LEADER_ADDRESS should not be set for single-pod", ci)
		}
	}
}

// TestRender_MultiPod_LeaderEnvVarsForLeader pins the leader pod's
// OME_RUNNER / OME_RUNNER_SIZE / OME_LEADER_ADDRESS env vars. The leader's
// OME_LEADER_ADDRESS points at itself so leader-side code that reads the
// var to identify the gang head sees the same value as workers.
//
// Address shape: short form <pod-name>.<headless-service>; cluster DNS
// search path resolves it the same as the fully qualified
// <pod>.<headless>.<ns>.svc.<cluster-domain>.
func TestRender_MultiPod_LeaderEnvVarsForLeader(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	leader := runners[0]
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	want := map[string]string{
		"OME_RUNNER":             "leader",
		"OME_RUNNER_SIZE":        "1",
		"OME_RUNNER_INDEX":       "0",
		"OME_LEADER_ADDRESS":     "llama-70b-engine-0-leader-0.llama-70b-engine-headless",
		"OME_INSTANCE_INDEX":     "0",
		"OME_COMPONENT":          "engine",
		"OME_INSTANCE_SUBDOMAIN": "llama-70b-engine-0",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env[%s]: got %q want %q", k, got[k], v)
		}
	}
}

// TestRender_MultiPod_LeaderAddressOnWorker pins that every worker pod
// in a multi-pod Instance gets OME_LEADER_ADDRESS pointing at the leader
// pod of the SAME Instance. Worker's own OME_RUNNER_INDEX is its ordinal
// (NCCL rank position), not the leader's 0.
func TestRender_MultiPod_LeaderAddressOnWorker(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	worker := runners[1]

	pod0, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker 0: %v", err)
	}
	pod1, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 1)
	if err != nil {
		t.Fatalf("worker 1: %v", err)
	}

	for i, pod := range []*corev1.Pod{pod0, pod1} {
		got := envByName(pod.Spec.Containers[0].Env)
		if got["OME_LEADER_ADDRESS"] != "llama-70b-engine-0-leader-0.llama-70b-engine-headless" {
			t.Errorf("worker %d OME_LEADER_ADDRESS: got %q", i, got["OME_LEADER_ADDRESS"])
		}
		if got["OME_RUNNER"] != "worker" {
			t.Errorf("worker %d OME_RUNNER: got %q want worker", i, got["OME_RUNNER"])
		}
		if got["OME_RUNNER_SIZE"] != "2" {
			t.Errorf("worker %d OME_RUNNER_SIZE: got %q want 2", i, got["OME_RUNNER_SIZE"])
		}
	}
	if envByName(pod0.Spec.Containers[0].Env)["OME_RUNNER_INDEX"] != "0" {
		t.Errorf("worker 0 OME_RUNNER_INDEX: got %q want 0", envByName(pod0.Spec.Containers[0].Env)["OME_RUNNER_INDEX"])
	}
	if envByName(pod1.Spec.Containers[0].Env)["OME_RUNNER_INDEX"] != "1" {
		t.Errorf("worker 1 OME_RUNNER_INDEX: got %q want 1", envByName(pod1.Spec.Containers[0].Env)["OME_RUNNER_INDEX"])
	}
}

// TestRender_MultiPod_InstancePodRankAndCount pins the gang-global node-rank
// env. OME_INSTANCE_POD_RANK is the pod's flat rank across the WHOLE Instance
// (leader runner first, then worker runner); OME_INSTANCE_POD_COUNT is the
// gang's total pod count. Multi-node frameworks (sglang --node-rank/--nnodes,
// torchrun) read these directly instead of reconstructing a global rank from
// the per-runner OME_RUNNER_INDEX — which can't serve as a gang rank because
// leader and worker each index from 0 (raw OME_RUNNER_INDEX would collide
// leader-0 with worker-0). Gated on multi-pod, like OME_LEADER_ADDRESS.
func TestRender_MultiPod_InstancePodRankAndCount(t *testing.T) {
	plan, inst, runners := multiPodPlan(3) // 1 leader + 3 workers = 4-node gang
	leader, worker := runners[0], runners[1]

	// Leader is global rank 0; gang size 4.
	lpod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	lenv := envByName(lpod.Spec.Containers[0].Env)
	if lenv["OME_INSTANCE_POD_RANK"] != "0" {
		t.Errorf("leader OME_INSTANCE_POD_RANK: got %q want 0", lenv["OME_INSTANCE_POD_RANK"])
	}
	if lenv["OME_INSTANCE_POD_COUNT"] != "4" {
		t.Errorf("leader OME_INSTANCE_POD_COUNT: got %q want 4", lenv["OME_INSTANCE_POD_COUNT"])
	}

	// Workers follow the leader: ordinal 0/1/2 → global rank 1/2/3, count 4.
	for _, w := range []struct {
		ord  int32
		rank string
	}{{0, "1"}, {1, "2"}, {2, "3"}} {
		wpod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, w.ord)
		if err != nil {
			t.Fatalf("worker ord %d: %v", w.ord, err)
		}
		wenv := envByName(wpod.Spec.Containers[0].Env)
		if wenv["OME_INSTANCE_POD_RANK"] != w.rank {
			t.Errorf("worker ord %d OME_INSTANCE_POD_RANK: got %q want %s", w.ord, wenv["OME_INSTANCE_POD_RANK"], w.rank)
		}
		if wenv["OME_INSTANCE_POD_COUNT"] != "4" {
			t.Errorf("worker ord %d OME_INSTANCE_POD_COUNT: got %q want 4", w.ord, wenv["OME_INSTANCE_POD_COUNT"])
		}
	}

	// Single-pod Instances must NOT get the gang-global vars (same multi-pod
	// gate as OME_LEADER_ADDRESS) — runtimes branch on presence to detect multi-node.
	splan, sinst, srunner := singlePodPlan()
	spod, err := testRender(basicISVC(), basicPodSpec(), splan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("single-pod: %v", err)
	}
	senv := envByName(spod.Spec.Containers[0].Env)
	for _, k := range []string{"OME_INSTANCE_POD_RANK", "OME_INSTANCE_POD_COUNT"} {
		if v, ok := senv[k]; ok {
			t.Errorf("single-pod must not set %s (multi-pod only); got %q", k, v)
		}
	}
}

// TestRender_MultiPod_LeaderAddressPerInstance pins that the leader
// address is per-Instance: Instance 0's workers point at the Instance 0
// leader; Instance 1's workers point at the Instance 1 leader. Cross-talk
// would be a sharding bug.
func TestRender_MultiPod_LeaderAddressPerInstance(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	inst0 := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	inst1 := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst0, inst1},
	}

	pod0Worker, err := testRender(basicISVC(), basicPodSpec(), plan, inst0, runners[1], 0)
	if err != nil {
		t.Fatalf("inst0 worker: %v", err)
	}
	pod1Worker, err := testRender(basicISVC(), basicPodSpec(), plan, inst1, runners[1], 0)
	if err != nil {
		t.Fatalf("inst1 worker: %v", err)
	}

	got0 := envByName(pod0Worker.Spec.Containers[0].Env)["OME_LEADER_ADDRESS"]
	got1 := envByName(pod1Worker.Spec.Containers[0].Env)["OME_LEADER_ADDRESS"]
	want0 := "llama-70b-engine-0-leader-0.llama-70b-engine-headless"
	want1 := "llama-70b-engine-1-leader-0.llama-70b-engine-headless"
	if got0 != want0 {
		t.Errorf("inst0 worker OME_LEADER_ADDRESS: got %q want %q", got0, want0)
	}
	if got1 != want1 {
		t.Errorf("inst1 worker OME_LEADER_ADDRESS: got %q want %q", got1, want1)
	}
}

// TestRender_SinglePod_OmitsLeaderAddress pins the negative: single-pod
// "default" runner does NOT carry OME_LEADER_ADDRESS. Routers and any
// non-multi-pod Engine/Decoder must not see this variable; runtimes that
// branch on it would interpret its presence as a multi-pod environment.
func TestRender_SinglePod_OmitsLeaderAddress(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	if _, present := got["OME_LEADER_ADDRESS"]; present {
		t.Errorf("single-pod must not set OME_LEADER_ADDRESS; got %q", got["OME_LEADER_ADDRESS"])
	}
}

func TestRender_OMEEnvOverridesUserEnvWithSameName(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "main", Image: "llama:v1",
				Env: []corev1.EnvVar{
					{Name: "OME_COMPONENT", Value: "user-supplied-junk"},
					{Name: "USER_KEEP", Value: "kept"},
				},
			},
		},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	if got["OME_COMPONENT"] != "engine" {
		t.Errorf("OME_COMPONENT: got %q want engine", got["OME_COMPONENT"])
	}
	if got["USER_KEEP"] != "kept" {
		t.Errorf("USER_KEEP: got %q want kept", got["USER_KEEP"])
	}
	// No duplicate OME_COMPONENT
	count := 0
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == "OME_COMPONENT" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("OME_COMPONENT appeared %d times, want 1", count)
	}
}

func TestRender_NameVariesWithEachAxis(t *testing.T) {
	tests := []struct {
		name      string
		isvc      string
		component workload.ComponentType
		inst      int32
		runner    string
		ordinal   int32
		want      string
	}{
		{"engine, single-pod default, ord 0", "llama", workload.ComponentEngine, 0, "default", 0, "llama-engine-0-default-0"},
		{"decoder, instance 2", "llama", workload.ComponentDecoder, 2, "default", 0, "llama-decoder-2-default-0"},
		{"router, instance 5", "llama", workload.ComponentRouter, 5, "default", 0, "llama-router-5-default-0"},
		{"engine, leader runner ord 0 (multi-pod shape preview)", "llama", workload.ComponentEngine, 0, "leader", 0, "llama-engine-0-leader-0"},
		{"engine, worker runner ord 2 (multi-pod shape preview)", "llama", workload.ComponentEngine, 0, "worker", 2, "llama-engine-0-worker-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := query.PodName(tt.isvc, tt.component, tt.inst, tt.runner, tt.ordinal)
			if got != tt.want {
				t.Errorf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRender_HeadlessServiceName(t *testing.T) {
	if got := query.HeadlessServiceName("llama-70b", workload.ComponentEngine); got != "llama-70b-engine-headless" {
		t.Errorf("got %q want llama-70b-engine-headless", got)
	}
	if got := query.HeadlessServiceName("llama-70b", workload.ComponentDecoder); got != "llama-70b-decoder-headless" {
		t.Errorf("got %q want llama-70b-decoder-headless", got)
	}
}

func envByName(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// envKeys is a small debugging helper; sorted for stable diff messages.
//
//nolint:unused
func envKeys(env []corev1.EnvVar) []string {
	keys := make([]string, 0, len(env))
	for _, e := range env {
		keys = append(keys, e.Name)
	}
	sort.Strings(keys)
	return keys
}

// --- applyMigrationOverlay idempotency ---

func TestApplyMigrationOverlay_IdempotentDoesNotDuplicateRequirements(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{}}
	overlay := &workload.MigrationOverlay{
		FromNode:        "node5",
		HintTargetNodes: []string{"node3", "node7"},
	}
	applyMigrationOverlay(pod, overlay)
	applyMigrationOverlay(pod, overlay)

	req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) != 1 {
		t.Fatalf("expected exactly one term, got %+v", req)
	}
	notInCount := 0
	for _, expr := range req.NodeSelectorTerms[0].MatchExpressions {
		if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpNotIn {
			notInCount++
		}
	}
	if notInCount != 1 {
		t.Errorf("NotIn[hostname] requirement should appear once, got %d", notInCount)
	}

	pref := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 {
		t.Errorf("preferred terms should appear once, got %d: %+v", len(pref), pref)
	}
}

func TestApplyMigrationOverlay_PreferredWeightIsSentinel(t *testing.T) {
	// Weight is a documented sentinel — pinned so future changes are
	// deliberate and don't silently collide with operator preferences.
	pod := &corev1.Pod{Spec: corev1.PodSpec{}}
	applyMigrationOverlay(pod, &workload.MigrationOverlay{HintTargetNodes: []string{"node3"}})
	pref := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 || pref[0].Weight != overlayPreferredHintWeight {
		t.Errorf("preferred weight: got %+v want sentinel=%d", pref, overlayPreferredHintWeight)
	}
}

// --- wouldOverlayConflictWithNodeAffinity ---

func TestWouldOverlayConflictWithNodeAffinity_DetectsUnsatisfiablePin(t *testing.T) {
	// Source PodSpec required hostname=node5; overlay's NotIn[node5]
	// would collapse the term to "hostname=node5 AND hostname!=node5".
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node5"},
						}},
					}},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if !WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("hostname=FromNode pin should be detected as conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_HostnameSetWithOtherNodesIsSatisfiable(t *testing.T) {
	// Source required hostname IN {node5, node7}. Overlay's NotIn[node5]
	// excludes node5; node7 remains scheduable. NOT a conflict.
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node5", "node7"},
						}},
					}},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("multi-value hostname set must not be flagged as conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_OnlyConflictIfEveryTermPins(t *testing.T) {
	// Terms are OR'd. Term 1 pins to node5 (conflict), Term 2 is unconstrained
	// (satisfiable). Overall: still satisfiable, so NOT a conflict.
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node5"},
						}}},
						{MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-west-2a"},
						}}},
					},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("at least one satisfiable term must clear the conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_NilSpecOrAffinityNoConflict(t *testing.T) {
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(nil, overlay) {
		t.Errorf("nil spec should not conflict")
	}
	if WouldOverlayConflictWithNodeAffinity(&corev1.PodSpec{}, overlay) {
		t.Errorf("empty affinity should not conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_NilOverlayOrEmptyFromNode(t *testing.T) {
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node5"},
						}},
					}},
				},
			},
		},
	}
	if WouldOverlayConflictWithNodeAffinity(spec, nil) {
		t.Errorf("nil overlay must not conflict")
	}
	if WouldOverlayConflictWithNodeAffinity(spec, &workload.MigrationOverlay{FromNode: ""}) {
		t.Errorf("empty FromNode must not conflict")
	}
}

// TestRender_ContainerName_FallbackPrecedence pins the precedence walk
// for rendered container names when the upstream merge layer hasn't
// stamped a name. Render is the apply-time chokepoint that talks to the
// apiserver; an empty container Name produces a Pod-create rejection
// (`spec.containers[N].name: Required value`) that deadlocks OMENative
// rollouts (see Bug #42).
//
// The precedence under test:
//
//  1. Container name set on the PodSpec we receive → preserved as-is.
//     Upstream merge ran or the user template explicitly named the
//     container; either way we don't overwrite.
//  2. Container name empty AND we have a PodSpec → backfill to the
//     canonical "ome-container". Single source of truth at apply time
//     so the bug surfaces neither at Pod-create nor at runtime.
//  3. Multiple containers with mixed empty/non-empty names → only the
//     empty ones get the default; named siblings are untouched.
//  4. Init containers get the same treatment as regular containers.
//
// Note: cases where upstream merge correctly picks the ServingRuntime's
// engineConfig.runner.name are covered by utils.MergeRuntimeContainers
// tests (since the merge happens upstream of render); this test focuses
// on the render-layer last-line backfill that catches direct callers.
func TestRender_ContainerName_FallbackPrecedence(t *testing.T) {
	plan, inst, runner := singlePodPlan()

	// Case (a): caller set a name explicitly → preserved.
	t.Run("explicit name is preserved", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "my-runner", Image: "img:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != "my-runner" {
			t.Errorf("Containers[0].Name: got %q want my-runner", got)
		}
	})

	// Case (b): caller left the container unnamed (e.g., neither ISVC
	// nor runtime stamped one and the merge produced "") → backfill to
	// the canonical default.
	t.Run("empty name falls back to ome-container", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "", Image: "img:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != constants.MainContainerName {
			t.Errorf("Containers[0].Name: got %q want %q", got, constants.MainContainerName)
		}
	})

	// Case (c): mixed slice — empty names get the default; named
	// siblings are untouched.
	t.Run("only empty names are backfilled", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "sidecar", Image: "sc:v1"},
				{Name: "", Image: "main:v1"},
			},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != "sidecar" {
			t.Errorf("Containers[0].Name (sidecar): got %q want sidecar", got)
		}
		if got := pod.Spec.Containers[1].Name; got != constants.MainContainerName {
			t.Errorf("Containers[1].Name (was empty): got %q want %q", got, constants.MainContainerName)
		}
	})

	// Case (d): init containers get the same treatment; an unnamed init
	// container is just as fatal at apply-time as an unnamed regular one.
	t.Run("init containers backfilled too", func(t *testing.T) {
		spec := &corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "", Image: "init:v1"}},
			Containers:     []corev1.Container{{Name: "main", Image: "main:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.InitContainers[0].Name; got != constants.MainContainerName {
			t.Errorf("InitContainers[0].Name: got %q want %q", got, constants.MainContainerName)
		}
	})
}

// TestBackfillEmptyContainerNames pins the helper's nil-and-zero
// semantics directly — Render's wrapper test above exercises the
// "with-Render" path; this one isolates the helper for documentation.
func TestBackfillEmptyContainerNames(t *testing.T) {
	t.Run("nil spec is a no-op", func(t *testing.T) {
		backfillEmptyContainerNames(nil) // must not panic
	})
	t.Run("zero containers is a no-op", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		backfillEmptyContainerNames(spec)
		if len(spec.Containers) != 0 || len(spec.InitContainers) != 0 {
			t.Errorf("must not invent containers; got %+v", spec)
		}
	})
}
