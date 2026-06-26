package inferencereplica

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// managedPod returns a baseline OMENative-managed pod with the routing
// labels and a Running phase the predicate considers reconcile-relevant.
func managedPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "isvc-engine-abc-0",
			Namespace: "ns",
			Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: "isvc",
				constants.OMEComponentLabel:           "engine",
				query.LabelInstanceIdx:                "0",
				query.LabelRevisionHash:               "abc",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func withContainersReady(p *corev1.Pod, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   corev1.ContainersReady,
		Status: status,
	})
	return p
}

func withServing(p *corev1.Pod, serving bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if serving {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   podreadiness.ConditionType,
		Status: status,
	})
	return p
}

func TestManagedByOMENativePredicate_CreateDeleteGenericGateOnManagedBy(t *testing.T) {
	p := managedByOMENativePredicate()
	managed := managedPod()
	unmanaged := managedPod()
	delete(unmanaged.Labels, query.LabelManagedBy)

	if !p.Create(event.CreateEvent{Object: managed}) {
		t.Error("Create on managed pod must pass")
	}
	if p.Create(event.CreateEvent{Object: unmanaged}) {
		t.Error("Create on unmanaged pod must be dropped")
	}
	if !p.Delete(event.DeleteEvent{Object: managed}) {
		t.Error("Delete on managed pod must pass")
	}
	if p.Delete(event.DeleteEvent{Object: unmanaged}) {
		t.Error("Delete on unmanaged pod must be dropped")
	}
	if !p.Generic(event.GenericEvent{Object: managed}) {
		t.Error("Generic on managed pod must pass")
	}
	if p.Generic(event.GenericEvent{Object: unmanaged}) {
		t.Error("Generic on unmanaged pod must be dropped")
	}
}

func TestManagedByOMENativePredicate_UpdateFieldDiff(t *testing.T) {
	terminal := &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}

	tests := []struct {
		name     string
		old      *corev1.Pod
		new      *corev1.Pod
		wantPass bool
	}{
		{
			name:     "no relevant change (heartbeat) is dropped",
			old:      managedPod(),
			new:      managedPod(),
			wantPass: false,
		},
		{
			name: "only timestamp/podIP churn is dropped",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.PodIP = "10.0.0.5"
				p.Status.StartTime = &metav1.Time{}
				return p
			}(),
			wantPass: false,
		},
		{
			name:     "Status.Phase change passes",
			old:      managedPod(),
			new:      func() *corev1.Pod { p := managedPod(); p.Status.Phase = corev1.PodFailed; return p }(),
			wantPass: true,
		},
		{
			name:     "ContainersReady flip passes",
			old:      withContainersReady(managedPod(), false),
			new:      withContainersReady(managedPod(), true),
			wantPass: true,
		},
		{
			name:     "serving-gate flip passes",
			old:      withServing(managedPod(), false),
			new:      withServing(managedPod(), true),
			wantPass: true,
		},
		{
			name: "ContainerStatuses terminal-failure change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: terminal}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "Waiting.Reason change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "Terminated exit-code change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "LastTerminationState crash detail change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "container Image flip (in-place convergence) passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v2"},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "InitContainerStatuses terminal-failure change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "init", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "init", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerError"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "container added passes (length change)",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
					{Name: "sidecar", Image: "sidecar:v1"},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "ImageID/RestartCount/Resources heartbeat churn is dropped",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:         "main",
						Image:        "fake-serving:v1",
						ImageID:      "sha256:aaaa",
						RestartCount: 1,
						Ready:        true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Now()}},
					},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:         "main",
						Image:        "fake-serving:v1",
						ImageID:      "sha256:bbbb", // churn
						RestartCount: 2,             // churn
						Ready:        true,          // per-container Ready (pod condition handled separately)
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Now()}}, // timestamp churn
						AllocatedResources: corev1.ResourceList{}, // resource churn
					},
				}
				return p
			}(),
			wantPass: false,
		},
		{
			name: "SchedulingGates removal (Kueue gate-exit) passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				p.Spec.SchedulingGates = []corev1.PodSchedulingGate{
					{Name: "kueue.x-k8s.io/admission"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				return p
			}(),
			wantPass: true,
		},
		{
			name: "NodeName bind passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				p.Spec.NodeName = "node-1"
				return p
			}(),
			wantPass: true,
		},
		{
			name: "DeletionTimestamp set passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				now := metav1.Now()
				p.DeletionTimestamp = &now
				return p
			}(),
			wantPass: true,
		},
		{
			name: "OwnerReferences change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				ctrl := true
				p.OwnerReferences = []metav1.OwnerReference{
					{Kind: irKind, Name: "isvc-engine", Controller: &ctrl},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "revision-hash label change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Labels[query.LabelRevisionHash] = "def"
				return p
			}(),
			wantPass: true,
		},
		{
			name: "orthogonal label churn is dropped",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Labels["unrelated/label"] = "x"
				return p
			}(),
			wantPass: false,
		},
		{
			name:     "both unmanaged is dropped",
			old:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			new:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			wantPass: false,
		},
		{
			name:     "managed-by stripped (old managed) still passes",
			old:      managedPod(),
			new:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			wantPass: true,
		},
	}

	p := managedByOMENativePredicate()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Update(event.UpdateEvent{
				ObjectOld: client.Object(tc.old),
				ObjectNew: client.Object(tc.new),
			})
			if got != tc.wantPass {
				t.Errorf("Update predicate = %v, want %v", got, tc.wantPass)
			}
		})
	}
}
