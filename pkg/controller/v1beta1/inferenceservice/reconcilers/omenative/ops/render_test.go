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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

func singlePodPlan() (core.ComponentPlan, core.InstancePlan, core.RunnerPlan) {
	runner := core.RunnerPlan{Name: "default", Size: 1}
	inst := core.InstancePlan{Index: 0, Incarnation: 1, Runners: []core.RunnerPlan{runner}}
	plan := core.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  2,
		Instances: []core.InstancePlan{inst},
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
		query.LabelInstanceIdx:                "0",
		query.LabelInstanceIncarnation:        "1",
		query.LabelRunner:                     "default",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	if diff := cmp.Diff(want, pod.Labels); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderWithRevision_StampsRevisionHashLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := RenderWithRevision(basicISVC(), basicPodSpec(), plan, inst, runner, 0, "abc12345")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelRevisionHash]; got != "abc12345" {
		t.Errorf("%s: got %q want %q", query.LabelRevisionHash, got, "abc12345")
	}
}

func TestRenderWithRevision_EmptyHashOmitsRevisionLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := RenderWithRevision(basicISVC(), basicPodSpec(), plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("%s should be absent when hash is empty, got: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

func TestRender_DelegatesToRenderWithRevisionEmptyHash(t *testing.T) {
	// Render() is the back-compat entry point and must produce the same
	// pod as RenderWithRevision(..., "") — i.e., no revision-hash label.
	plan, inst, runner := singlePodPlan()
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("Render() must omit %s; got pod labels: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

func TestRender_IncarnationStampedOnLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	inst.Incarnation = 7
	pod, err := Render(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != "7" {
		t.Errorf("%s: got %q want 7", query.LabelInstanceIncarnation, got)
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
	pod, err := Render(basicISVC(), ps, plan, inst, runner, 0)
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
			got := query.PodName(tt.isvc, tt.component, tt.inst, tt.runner, tt.ordinal)
			if got != tt.want {
				t.Errorf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRender_HeadlessServiceName(t *testing.T) {
	if got := query.HeadlessServiceName("llama-70b", v1beta1.EngineComponent); got != "llama-70b-engine-headless" {
		t.Errorf("got %q want llama-70b-engine-headless", got)
	}
	if got := query.HeadlessServiceName("llama-70b", v1beta1.DecoderComponent); got != "llama-70b-decoder-headless" {
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
	overlay := &core.MigrationOverlay{
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
	applyMigrationOverlay(pod, &core.MigrationOverlay{HintTargetNodes: []string{"node3"}})
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
	overlay := &core.MigrationOverlay{FromNode: "node5"}
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
	overlay := &core.MigrationOverlay{FromNode: "node5"}
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
	overlay := &core.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("at least one satisfiable term must clear the conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_NilSpecOrAffinityNoConflict(t *testing.T) {
	overlay := &core.MigrationOverlay{FromNode: "node5"}
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
	if WouldOverlayConflictWithNodeAffinity(spec, &core.MigrationOverlay{FromNode: ""}) {
		t.Errorf("empty FromNode must not conflict")
	}
}
