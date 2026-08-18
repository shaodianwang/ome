package inferencereplica

import (
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

func TestPublicationObservationMatchesInlineV1Projection(t *testing.T) {
	ir := baselineIR("model-engine", "default", 10)
	ready := podForIR(ir, 0, "default", 0, true, true)
	ready.Spec.NodeName = "node-b"
	notReady := podForIR(ir, 1, "default", 0, false, false)
	notReady.Spec.NodeName = "node-a"
	gated := podForIR(ir, 2, "default", 0, false, false)
	gated.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example-gate"}}
	terminating := podForIR(ir, 3, "default", 0, true, true)
	terminating.Spec.NodeName = "node-c"
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	foreignOwner := podForIR(ir, 4, "default", 0, true, true)
	foreignOwner.OwnerReferences = []metav1.OwnerReference{{UID: "different-owner", Controller: boolPointer(true)}}
	malformed := podForIR(ir, 5, "default", 0, true, true)
	malformed.Labels[query.LabelInstanceIdx] = "invalid"
	liveOnly := podForIR(ir, 99, "default", 0, true, true)

	gangPods := make([]*corev1.Pod, 0, 8)
	for i := 0; i < 8; i++ {
		pod := podForIR(ir, 7, "worker", int32(i), true, true)
		if i%2 == 0 {
			pod.Spec.NodeName = "node-b"
		} else {
			pod.Spec.NodeName = "node-a"
		}
		gangPods = append(gangPods, pod)
	}

	tests := []struct {
		name      string
		statuses  []v1beta1.OMENativeInstanceStatus
		pods      []*corev1.Pod
		available map[string]struct{}
	}{
		{
			name: "stale status trails a ready singleton",
			statuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			}},
			pods:      []*corev1.Pod{ready},
			available: map[string]struct{}{ready.Name: {}},
		},
		{
			name: "stale status leads a non-ready singleton",
			statuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 1, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1,
				AvailablePodCount: 1, ScheduledPodCount: 1, Admitted: true,
				NodesOccupied: []string{"old-node"},
			}},
			pods: []*corev1.Pod{notReady},
		},
		{
			name:     "zero-pod create window",
			statuses: []v1beta1.OMENativeInstanceStatus{fullyPopulatedInstanceStatus(6)},
		},
		{
			name: "admission-gated pod",
			statuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 2, Admitted: true,
			}},
			pods: []*corev1.Pod{gated},
		},
		{
			name: "eight-pod gang with duplicate nodes",
			statuses: []v1beta1.OMENativeInstanceStatus{
				fullyPopulatedInstanceStatus(7),
				fullyPopulatedInstanceStatus(7),
			},
			pods: gangPods,
			available: map[string]struct{}{
				gangPods[0].Name: {}, gangPods[1].Name: {}, gangPods[2].Name: {}, gangPods[3].Name: {},
			},
		},
		{
			name: "selector membership includes terminating and foreign-owner pods",
			statuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 3},
				{Index: 4},
			},
			pods:      []*corev1.Pod{terminating, foreignOwner},
			available: map[string]struct{}{foreignOwner.Name: {}},
		},
		{
			name: "malformed and rowless pod indexes do not create rows",
			statuses: []v1beta1.OMENativeInstanceStatus{
				fullyPopulatedInstanceStatus(5),
				fullyPopulatedInstanceStatus(8),
			},
			pods: []*corev1.Pod{malformed, liveOnly},
		},
		{
			name:     "nil status remains nil",
			statuses: nil,
			pods:     []*corev1.Pod{liveOnly},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := cloneAPIInstanceStatuses(test.statuses)
			byIndex := query.BucketPodsByInstanceIdx(test.pods)
			want := legacyInlineV1Projection(test.statuses, byIndex, test.available)

			observation, err := workload.NewOwnedPublicationObservation(
				v1beta1convert.InstanceStatusSliceToWorkload(test.statuses),
				workload.NewCachedSelectorPodObservation(test.pods, nil),
				test.available,
			)
			if err != nil {
				t.Fatalf("NewOwnedPublicationObservation: %v", err)
			}
			materialized, err := observation.TakeInlineV1Statuses()
			if err != nil {
				t.Fatalf("InlineV1Statuses: %v", err)
			}
			got := v1beta1convert.InstanceStatusSliceFromWorkload(materialized)

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("publication projection mismatch\n got: %#v\nwant: %#v", got, want)
			}
			if !reflect.DeepEqual(test.statuses, original) {
				t.Fatalf("input status mutated\n got: %#v\nwant: %#v", test.statuses, original)
			}
		})
	}
}

func TestPublicationObservationMatchesInlineV1ProjectionAtScale(t *testing.T) {
	tests := []struct {
		name            string
		instances       int
		podsPerInstance int
	}{
		{name: "empty", instances: 0, podsPerInstance: 1},
		{name: "singleton", instances: 1, podsPerInstance: 1},
		{name: "one hundred single-pod instances", instances: 100, podsPerInstance: 1},
		{name: "two thousand single-pod instances", instances: 2000, podsPerInstance: 1},
		{name: "one hundred eight-pod gangs", instances: 100, podsPerInstance: 8},
		{name: "two thousand eight-pod gangs", instances: 2000, podsPerInstance: 8},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ir := baselineIR("model-engine", "default", int32(test.instances))
			statuses := make([]v1beta1.OMENativeInstanceStatus, test.instances)
			pods := make([]*corev1.Pod, 0, test.instances*test.podsPerInstance+2)
			available := make(map[string]struct{})
			for index := 0; index < test.instances; index++ {
				statuses[index] = fullyPopulatedInstanceStatus(int32(index))
				for ordinal := 0; ordinal < test.podsPerInstance; ordinal++ {
					ready := (index+ordinal)%3 != 0
					serving := ready && (index+ordinal)%2 == 0
					runner := "default"
					if test.podsPerInstance > 1 {
						runner = "worker"
					}
					pod := podForIR(ir, int32(index), runner, int32(ordinal), ready, serving)
					if ordinal%5 != 0 {
						pod.Spec.NodeName = fmt.Sprintf("node-%02d", ordinal%4)
					}
					if (index+ordinal)%17 == 0 {
						pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example-gate"}}
					}
					if ready && (index+ordinal)%4 != 0 {
						available[pod.Name] = struct{}{}
					}
					pods = append(pods, pod)
				}
			}
			rowless := podForIR(ir, int32(test.instances+10), "default", 0, true, true)
			malformed := podForIR(ir, int32(test.instances+11), "default", 0, true, true)
			malformed.Labels[query.LabelInstanceIdx] = "invalid"
			pods = append(pods, rowless, malformed)

			byIndex := query.BucketPodsByInstanceIdx(pods)
			want := legacyInlineV1Projection(statuses, byIndex, available)
			observation, err := workload.NewOwnedPublicationObservation(
				v1beta1convert.InstanceStatusSliceToWorkload(statuses),
				workload.NewCachedSelectorPodObservation(pods, nil),
				available,
			)
			if err != nil {
				t.Fatalf("NewOwnedPublicationObservation: %v", err)
			}
			materialized, err := observation.TakeInlineV1Statuses()
			if err != nil {
				t.Fatalf("InlineV1Statuses: %v", err)
			}
			got := v1beta1convert.InstanceStatusSliceFromWorkload(materialized)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%d x %d projection differed", test.instances, test.podsPerInstance)
			}
		})
	}
}

func legacyInlineV1Projection(instances []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod, available map[string]struct{}) []v1beta1.OMENativeInstanceStatus {
	out := cloneAPIInstanceStatuses(instances)
	for i := range out {
		pods := byIndex[out[i].Index]
		out[i].PodCount = int32(len(pods))
		out[i].ReadyPodCount = workload.CountReadyPods(pods)
		out[i].ServingPodCount = workload.CountServingPods(pods)
		out[i].AvailablePodCount = workload.CountAvailablePods(pods, available)
		out[i].ScheduledPodCount = workload.CountScheduledPods(pods)
		out[i].Admitted = legacyPodsAdmitted(pods)
		out[i].NodesOccupied = workload.UniqueNodes(pods)
	}
	return out
}

func cloneAPIInstanceStatuses(in []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
	return v1beta1convert.InstanceStatusSliceFromWorkload(v1beta1convert.InstanceStatusSliceToWorkload(in))
}

func legacyPodsAdmitted(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if workload.PodAdmissionGated(pod) {
			return false
		}
	}
	return true
}

func boolPointer(value bool) *bool { return &value }
