package v1beta1

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// TestInferenceReplica_JSONRoundTrip exercises a fully-populated
// InferenceReplica round-trip through JSON and verifies every field
// survives. Each field added to the spec / status MUST appear here so
// renames or removals fail loudly.
func TestInferenceReplica_JSONRoundTrip(t *testing.T) {
	startedAt := metav1.NewTime(time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC))
	mu := intstr.FromString("25%")
	partition := int32(2)
	replicas := int32(3)
	collisionCount := int32(1)
	timeout := metav1.Duration{Duration: 20 * time.Minute}

	original := InferenceReplica{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ome.io/v1beta1",
			Kind:       "InferenceReplica",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine",
			Namespace: "prod-models",
			Labels: map[string]string{
				"ome.io/inferenceservice": "llama",
				"ome.io/component":        "engine",
			},
			Annotations: map[string]string{
				"ome.io/controller-write": "true",
			},
		},
		Spec: InferenceReplicaSpec{
			ParentRef: ParentReference{
				Name: "llama",
			},
			Component:       EngineComponent,
			Replicas:        &replicas,
			MinReadySeconds: 15,
			Runners: []Runner{
				{
					Name: RunnerNameLeader,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"ome.io/runner": "leader",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ome-container",
								Image: "sgl-leader:tier5",
							}},
						},
					},
				},
				{
					Name: RunnerNameWorker,
					Size: 3,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"ome.io/runner": "worker",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ome-container",
								Image: "sgl-worker:tier5",
							}},
						},
					},
				},
			},
			Lifecycle: &LifecycleSpec{
				RestartPolicy:        omenativeRestartPolicyPtr(InstanceRestartPolicyRecreateInstance),
				InstanceReadyTimeout: &timeout,
				UpdateStrategy: &UpdateStrategy{
					Type: UpdateStrategySurgeThenDrain,
					RollingUpdate: &RollingUpdate{
						Partition: &partition,
					},
				},
			},
			Pacing: &InferenceReplicaPacing{
				Partition:      &partition,
				MaxUnavailable: &mu,
			},
			Paused: false,
		},
		Status: InferenceReplicaStatus{
			ObservedGeneration:   17,
			Replicas:             3,
			ReadyReplicas:        2,
			ServingReplicas:      2,
			AvailableReplicas:    2,
			UpdatedReplicas:      1,
			UpdatedReadyReplicas: 1,
			CurrentRevision:      "llama-engine-rev-abc123",
			UpdateRevision:       "llama-engine-rev-def456",
			CollisionCount:       &collisionCount,
			LabelSelector:        "ome.io/inferenceservice=llama,ome.io/component=engine,ome.io/managed-by=OMENative",
			Conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionTrue, LastTransitionTime: startedAt, Reason: "InstancesReady"},
				{Type: "Progressing", Status: metav1.ConditionTrue, LastTransitionTime: startedAt, Reason: "RolloutInProgress"},
			},
			InstanceStatuses: []OMENativeInstanceStatus{
				{
					Index:           0,
					Incarnation:     1,
					Phase:           OMENativeInstanceReady,
					RunningRevision: "llama-engine-rev-abc123",
					TargetRevision:  "llama-engine-rev-abc123",
					PodCount:        4,
					ReadyPodCount:   4,
					ServingPodCount: 4,
					ActiveOrdinal:   0,
				},
			},
			Traffic: []ComponentTrafficTarget{
				{RevisionName: "llama-engine-rev-abc123", Percent: 100, LatestRevision: true},
			},
			CoordinationGroupRef: "default",
		},
	}

	data, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	roundtripped := InferenceReplica{}
	if err := json.Unmarshal(data, &roundtripped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if diff := cmp.Diff(original, roundtripped); diff != "" {
		t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

// TestInferenceReplica_DefaultsToZero verifies the InferenceReplica
// zero value JSON-marshals cleanly. The CRD validation layer should
// reject empty Runners at admission; the Go type must not panic on
// the zero value.
func TestInferenceReplica_DefaultsToZero(t *testing.T) {
	zero := InferenceReplica{}
	data, err := json.Marshal(&zero)
	if err != nil {
		t.Fatalf("marshal zero: %v", err)
	}
	got := InferenceReplica{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal zero: %v", err)
	}
	if got.Spec.Paused != false {
		t.Errorf("expected default Paused=false, got %v", got.Spec.Paused)
	}
	if got.Spec.Replicas != nil {
		t.Errorf("expected default Replicas=nil, got %v", got.Spec.Replicas)
	}
}

// TestInferenceReplicaList_RoundTrip exercises the list type.
func TestInferenceReplicaList_RoundTrip(t *testing.T) {
	list := InferenceReplicaList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ome.io/v1beta1",
			Kind:       "InferenceReplicaList",
		},
		Items: []InferenceReplica{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "engine", Namespace: "ns"},
				Spec: InferenceReplicaSpec{
					ParentRef: ParentReference{Name: "llama"},
					Component: EngineComponent,
					Runners: []Runner{
						{Name: RunnerNameDefault, Size: 1},
					},
				},
			},
		},
	}
	data, err := json.Marshal(&list)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := InferenceReplicaList{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if diff := cmp.Diff(list, got); diff != "" {
		t.Errorf("list round-trip mismatch (-want +got):\n%s", diff)
	}
}

// TestParentReference_JSONShape pins the wire shape of the
// ParentReference helper so future renames don't silently break
// existing InferenceReplica YAML.
func TestParentReference_JSONShape(t *testing.T) {
	pr := ParentReference{Name: "llama"}
	data, err := json.Marshal(&pr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"name":"llama"}`
	if string(data) != want {
		t.Errorf("ParentReference JSON shape changed:\n want %s\n got %s", want, string(data))
	}
}

// TestRunner_RoleNames verifies the RunnerName enum values match the
// OEP-0007 vocabulary (default, leader, worker).
func TestRunner_RoleNames(t *testing.T) {
	cases := []struct {
		role RunnerName
		want string
	}{
		{RunnerNameDefault, "default"},
		{RunnerNameLeader, "leader"},
		{RunnerNameWorker, "worker"},
	}
	for _, c := range cases {
		if string(c.role) != c.want {
			t.Errorf("RunnerName(%q) got %q", c.want, string(c.role))
		}
	}
}

func omenativeRestartPolicyPtr(p InstanceRestartPolicy) *InstanceRestartPolicy {
	return &p
}
