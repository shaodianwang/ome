package omenative

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func singlePodPlan() (ComponentPlan, InstancePlan, RunnerPlan) {
	runner := RunnerPlan{Name: "default", Size: 1}
	inst := InstancePlan{Index: 0, Incarnation: 1, Runners: []RunnerPlan{runner}}
	plan := ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  2,
		Instances: []InstancePlan{inst},
	}
	return plan, inst, runner
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
	if _, err := Render(nil, basicPodSpec(), plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
	if _, err := Render(basicISVC(), nil, plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil PodSpec")
	}
}

func TestRender_StableNameAndDNS(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
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
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		labelInstanceIdx:                      "0",
		labelInstanceIncarnation:              "1",
		labelRunner:                           "default",
		labelManagedBy:                        managedByOMENative,
	}
	if diff := cmp.Diff(want, pod.Labels); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_IncarnationStampedOnLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	inst.Incarnation = 7
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[labelInstanceIncarnation]; got != "7" {
		t.Errorf("%s: got %q want 7", labelInstanceIncarnation, got)
	}
}

func TestRender_OwnerReference(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
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
	pod, err := Render(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 2 {
		t.Fatalf("ReadinessGates: got %d want 2", len(pod.Spec.ReadinessGates))
	}
	if pod.Spec.ReadinessGates[0].ConditionType != "user.example.com/ready" {
		t.Errorf("user gate clobbered: %+v", pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[1].ConditionType != servingConditionType {
		t.Errorf("ome.io/serving not appended: %+v", pod.Spec.ReadinessGates)
	}
}

func TestRender_DoesNotDuplicateExistingServingGate(t *testing.T) {
	// REV-1 fix: if the user template already declares ome.io/serving,
	// Render must not append a duplicate. Two copies of the same gate
	// would keep PodReady=False forever — kubelet AND's all gates,
	// the controller only writes one condition.
	plan, inst, runner := singlePodPlan()
	ps := basicPodSpec()
	ps.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: servingConditionType},
	}
	pod, err := Render(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 1 {
		t.Fatalf("ReadinessGates duplicated: got %d want 1: %+v", len(pod.Spec.ReadinessGates), pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[0].ConditionType != servingConditionType {
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
	pod, err := Render(basicISVC(), ps, plan, inst, runner, 0)
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
	pod, err := Render(basicISVC(), ps, plan, inst, runner, 0)
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
		component v1beta1.ComponentType
		inst      int32
		runner    string
		ordinal   int32
		want      string
	}{
		{"engine, single-pod default, ord 0", "llama", v1beta1.EngineComponent, 0, "default", 0, "llama-engine-0-default-0"},
		{"decoder, instance 2", "llama", v1beta1.DecoderComponent, 2, "default", 0, "llama-decoder-2-default-0"},
		{"router, instance 5", "llama", v1beta1.RouterComponent, 5, "default", 0, "llama-router-5-default-0"},
		{"engine, leader runner ord 0 (multi-pod shape preview)", "llama", v1beta1.EngineComponent, 0, "leader", 0, "llama-engine-0-leader-0"},
		{"engine, worker runner ord 2 (multi-pod shape preview)", "llama", v1beta1.EngineComponent, 0, "worker", 2, "llama-engine-0-worker-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := podName(tt.isvc, tt.component, tt.inst, tt.runner, tt.ordinal)
			if got != tt.want {
				t.Errorf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRender_HeadlessServiceName(t *testing.T) {
	if got := headlessServiceName("llama-70b", v1beta1.EngineComponent); got != "llama-70b-engine-headless" {
		t.Errorf("got %q want llama-70b-engine-headless", got)
	}
	if got := headlessServiceName("llama-70b", v1beta1.DecoderComponent); got != "llama-70b-decoder-headless" {
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
