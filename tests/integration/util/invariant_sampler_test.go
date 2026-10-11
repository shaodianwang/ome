package util

import (
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/escalation"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// invPod builds one OMENative pod of a Component and Instance index. serving
// makes it Ready with the serving gate True, the state that carries traffic.
func invPod(name string, comp v1beta1.ComponentType, idx int32, hash, protocol string, serving bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
		constants.OMEComponentLabel: string(comp),
		query.LabelInstanceIdx:      strconv.Itoa(int(idx)),
		query.LabelRevisionHash:     hash,
	}}}
	if protocol != "" {
		pod.Labels[query.LabelPairingProtocol] = protocol
	}
	if serving {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
		}
	}
	return pod
}

func invObservation(budgets map[v1beta1.ComponentType]ComponentBudget, pods ...*corev1.Pod) *targetObservation {
	return &targetObservation{
		target:  InvariantTarget{Namespace: "ns", Name: "svc"},
		isvc:    &v1beta1.InferenceService{},
		pods:    pods,
		rows:    map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{},
		budgets: budgets,
	}
}

func findingsOf(t *testing.T, got []finding, invariant string) []finding {
	t.Helper()
	var out []finding
	for _, f := range got {
		if f.invariant == invariant {
			out = append(out, f)
		}
	}
	return out
}

func expectOneFinding(t *testing.T, got []finding, invariant, subject, detailPart string) {
	t.Helper()
	fs := findingsOf(t, got, invariant)
	if len(fs) != 1 {
		t.Fatalf("want one %s finding, got %+v", invariant, fs)
	}
	if fs[0].subject != subject || !strings.Contains(fs[0].detail, detailPart) {
		t.Fatalf("want %s finding on %q mentioning %q, got %+v", invariant, subject, detailPart, fs[0])
	}
}

func expectNoFinding(t *testing.T, got []finding, invariant string) {
	t.Helper()
	if fs := findingsOf(t, got, invariant); len(fs) != 0 {
		t.Fatalf("want no %s finding, got %+v", invariant, fs)
	}
}

func TestInvariantPairableCapacity(t *testing.T) {
	engineOnly := map[v1beta1.ComponentType]ComponentBudget{v1beta1.EngineComponent: {Desired: 2, MaxUnavailable: 0}}

	t.Run("floor holds while every desired pod serves", func(t *testing.T) {
		obs := invObservation(engineOnly,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h1", "", true))
		armed := map[string]int32{}
		expectNoFinding(t, evaluateTarget(obs, armed), InvariantPairableCapacity)
		if armed["ns/svc/engine"] != 2 {
			t.Fatalf("a fully serving Component must arm its floor, got %v", armed)
		}
	})

	t.Run("floor broken once armed", func(t *testing.T) {
		obs := invObservation(engineOnly,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h1", "", false))
		armed := map[string]int32{"ns/svc/engine": 2}
		expectOneFinding(t, evaluateTarget(obs, armed), InvariantPairableCapacity, "engine", "1 pairable serving pods, floor 2")
	})

	t.Run("a Component still starting is not read as a drop", func(t *testing.T) {
		obs := invObservation(engineOnly, invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true))
		expectNoFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantPairableCapacity)
	})

	t.Run("no cap means no floor", func(t *testing.T) {
		uncapped := map[v1beta1.ComponentType]ComponentBudget{v1beta1.EngineComponent: {Desired: 2, MaxUnavailable: escalation.BudgetNoLimit}}
		obs := invObservation(uncapped)
		expectNoFinding(t, evaluateTarget(obs, map[string]int32{"ns/svc/engine": 2}), InvariantPairableCapacity)
	})

	pair := map[v1beta1.ComponentType]ComponentBudget{
		v1beta1.EngineComponent:  {Desired: 2, MaxUnavailable: 0},
		v1beta1.DecoderComponent: {Desired: 2, MaxUnavailable: 0},
	}
	armedPair := func() map[string]int32 { return map[string]int32{"ns/svc/engine": 2, "ns/svc/decoder": 2} }

	t.Run("protocol transition counts only pairable pods", func(t *testing.T) {
		obs := invObservation(pair,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "v1", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h2", "v2", true),
			invPod("d0", v1beta1.DecoderComponent, 0, "h3", "v1", true),
			invPod("d1", v1beta1.DecoderComponent, 1, "h3", "v1", true))
		obs.isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("v2")}
		got := evaluateTarget(obs, armedPair())
		// e1 on v2 has no decoder to pair with; both decoders pair with e0.
		expectOneFinding(t, got, InvariantPairableCapacity, "engine", "1 pairable serving pods, floor 2")
	})

	t.Run("protocol transition with a pair on each protocol holds", func(t *testing.T) {
		obs := invObservation(pair,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "v1", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h2", "v2", true),
			invPod("d0", v1beta1.DecoderComponent, 0, "h3", "v1", true),
			invPod("d1", v1beta1.DecoderComponent, 1, "h4", "v2", true))
		obs.isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("v2")}
		expectNoFinding(t, evaluateTarget(obs, armedPair()), InvariantPairableCapacity)
	})

	t.Run("empty protocol pairs with anything", func(t *testing.T) {
		obs := invObservation(pair,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "v1", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h2", "v2", true),
			invPod("d0", v1beta1.DecoderComponent, 0, "h3", "", true),
			invPod("d1", v1beta1.DecoderComponent, 1, "h3", "", true))
		obs.isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("v2")}
		expectNoFinding(t, evaluateTarget(obs, armedPair()), InvariantPairableCapacity)
	})
}

func TestInvariantBudgetPlusOne(t *testing.T) {
	engine := v1beta1.EngineComponent
	// Four engines under maxUnavailable 1: the floor is three serving pods.
	budgets := map[v1beta1.ComponentType]ComponentBudget{engine: {Desired: 4, MaxUnavailable: 1}}
	readyAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	promoted := func(index int32) v1beta1.OMENativeInstanceStatus {
		return v1beta1.OMENativeInstanceStatus{Index: index, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadySince: &metav1.Time{Time: readyAt}}
	}
	serving := func(names ...string) []*corev1.Pod {
		var out []*corev1.Pod
		for i, name := range names {
			out = append(out, invPod(name, engine, int32(i+1), "h2", "", true))
		}
		return out
	}
	cases := []struct {
		name string
		// e0 is instance 0's pod; the others serve on their own indexes.
		e0          *corev1.Pod
		others      []*corev1.Pod
		row         v1beta1.OMENativeInstanceStatus
		wantSubject string
		wantDetail  string
	}{
		{
			name:        "one short while a promoted pod restarted after Ready",
			e0:          withLastTermination(invPod("e0", engine, 0, "h2", "", false), readyAt.Add(10*time.Second)),
			others:      serving("e1", "e2"),
			row:         promoted(0),
			wantSubject: "engine (budget plus one while a promoted set is unproven)",
			wantDetail:  "2 pairable serving pods, floor 3 (desired 4, maxUnavailable 1, serving 2); instance 0 was promoted and has not proven itself since",
		},
		{
			name:   "one short while the row remembers a crash after Ready",
			e0:     invPod("e0", engine, 0, "h2", "", false),
			others: serving("e1", "e2"),
			row: func() v1beta1.OMENativeInstanceStatus {
				row := promoted(0)
				row.LastFailure = &v1beta1.InstanceTermination{PodName: "e0", Reason: "Error", Time: metav1.NewTime(readyAt.Add(10 * time.Second))}
				return row
			}(),
			wantSubject: "engine (budget plus one while a promoted set is unproven)",
			wantDetail:  "instance 0 was promoted and has not proven itself since",
		},
		{
			name:        "one short with no promoted set unproven",
			e0:          invPod("e0", engine, 0, "h2", "", false),
			others:      serving("e1", "e2"),
			row:         promoted(0),
			wantSubject: "engine",
			wantDetail:  "2 pairable serving pods, floor 3 (desired 4, maxUnavailable 1, serving 2)",
		},
		{
			name:        "a restart before Ready is boot, not a crash",
			e0:          withLastTermination(invPod("e0", engine, 0, "h2", "", false), readyAt.Add(-10*time.Second)),
			others:      serving("e1", "e2"),
			row:         promoted(0),
			wantSubject: "engine",
			wantDetail:  "2 pairable serving pods, floor 3",
		},
		{
			name:        "two short is the plain finding even with an unproven set",
			e0:          withLastTermination(invPod("e0", engine, 0, "h2", "", false), readyAt.Add(10*time.Second)),
			others:      serving("e1"),
			row:         promoted(0),
			wantSubject: "engine",
			wantDetail:  "1 pairable serving pods, floor 3",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := invObservation(budgets, append([]*corev1.Pod{tc.e0}, tc.others...)...)
			obs.rows = map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{engine: {tc.row}}
			got := evaluateTarget(obs, map[string]int32{"ns/svc/engine": 4})
			expectOneFinding(t, got, InvariantPairableCapacity, tc.wantSubject, tc.wantDetail)
		})
	}
}

func TestInvariantTrafficBacked(t *testing.T) {
	target := func(revision string, percent int32) v1beta1.ComponentTrafficTarget {
		return v1beta1.ComponentTrafficTarget{RevisionName: "svc-engine-" + revision, Percent: percent}
	}
	cases := []struct {
		name    string
		pods    []*corev1.Pod
		targets []v1beta1.ComponentTrafficTarget
		// wantSubject is the one expected finding's subject; empty wants none.
		wantSubject, wantDetail string
	}{
		{
			name:    "a serving pod on the revision backs the target",
			pods:    []*corev1.Pod{invPod("e0", v1beta1.EngineComponent, 0, "abc", "", true)},
			targets: []v1beta1.ComponentTrafficTarget{target("abc", 100)},
		},
		{
			name:    "a zero-percent target asks for nothing",
			pods:    []*corev1.Pod{invPod("e0", v1beta1.EngineComponent, 0, "abc", "", true)},
			targets: []v1beta1.ComponentTrafficTarget{target("abc", 100), target("old", 0)},
		},
		{
			name: "a Component with no serving pod at all is not the traffic check's",
			pods: []*corev1.Pod{
				invPod("e0", v1beta1.EngineComponent, 0, "abc", "", false),
				invPod("e1", v1beta1.EngineComponent, 1, "def", "", false)},
			targets: []v1beta1.ComponentTrafficTarget{target("abc", 100)},
		},
		{
			name: "the revision has pods and none serves while another revision does",
			pods: []*corev1.Pod{
				invPod("e0", v1beta1.EngineComponent, 0, "def", "", true),
				invPod("e1", v1beta1.EngineComponent, 1, "abc", "", false)},
			targets:     []v1beta1.ComponentTrafficTarget{target("abc", 100)},
			wantSubject: "engine svc-engine-abc (no pod serves)",
			wantDetail:  "1 pods carry revision hash abc and none serves (revision pods: e1; serving pods: e0)",
		},
		{
			name:        "the revision has no pod left and another revision serves",
			pods:        []*corev1.Pod{invPod("e0", v1beta1.EngineComponent, 0, "def", "", true)},
			targets:     []v1beta1.ComponentTrafficTarget{target("abc", 100)},
			wantSubject: "engine svc-engine-abc (a pod of another revision serves)",
			wantDetail:  "no pod carries revision hash abc, the serving pods are on other revisions (serving pods: e0)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := invObservation(nil, tc.pods...)
			obs.isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {Traffic: tc.targets},
			}
			got := evaluateTarget(obs, map[string]int32{})
			if tc.wantSubject == "" {
				expectNoFinding(t, got, InvariantTrafficBacked)
				return
			}
			expectOneFinding(t, got, InvariantTrafficBacked, tc.wantSubject, tc.wantDetail)
		})
	}
}

// withIncarnation stamps the incarnation label a recreate bumps, so a pod
// can be placed in or out of a row's current set.
func withIncarnation(pod *corev1.Pod, incarnation int64) *corev1.Pod {
	pod.Labels[query.LabelInstanceIncarnation] = strconv.FormatInt(incarnation, 10)
	return pod
}

// withRestarts gives the pod's main container a restart count.
func withRestarts(pod *corev1.Pod, restarts int32) *corev1.Pod {
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: constants.MainContainerName, RestartCount: restarts})
	return pod
}

// withLastTermination records that the pod's main container last exited at
// finishedAt, the trace a restart leaves on the pod.
func withLastTermination(pod *corev1.Pod, finishedAt time.Time) *corev1.Pod {
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
		Name:                 constants.MainContainerName,
		RestartCount:         1,
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finishedAt)}},
	})
	return pod
}

func TestInvariantStatusTruth(t *testing.T) {
	engine := v1beta1.EngineComponent
	failure := func(podName string) *v1beta1.InstanceTermination {
		return &v1beta1.InstanceTermination{PodName: podName, Reason: "ImagePullBackOff"}
	}
	cases := []struct {
		name string
		// setSize is how many pods one Instance owns; zero reads as one.
		setSize int32
		pods    []*corev1.Pod
		rows    []v1beta1.OMENativeInstanceStatus
		// wantSubject is the one expected finding's subject; empty wants none.
		wantSubject, wantDetail string
	}{
		{
			name: "rows match the pods",
			pods: []*corev1.Pod{
				invPod("e0", engine, 0, "h1", "", true),
				invPod("e1", engine, 1, "h1", "", false)},
			rows: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1},
				{Index: 1, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1}},
		},
		{
			name:        "a Ready row whose pod crashed after it served",
			pods:        []*corev1.Pod{withRestarts(invPod("e0", engine, 0, "h1", "", false), 3)},
			rows:        []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ServingPodCount: 0}},
			wantSubject: "engine instance 0",
			wantDetail:  "row is Ready but 0 of 1 pods of its set serve (servingPodCount 0; restarts: e0=3; pods: e0)",
		},
		{
			name:    "a Ready gang row with one pod of its set down",
			setSize: 2,
			pods: []*corev1.Pod{
				invPod("e0-leader", engine, 0, "h1", "", true),
				invPod("e0-worker", engine, 0, "h1", "", false)},
			rows:        []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2, ServingPodCount: 1}},
			wantSubject: "engine instance 0",
			wantDetail:  "row is Ready but 1 of 2 pods of its set serve (servingPodCount 1; restarts: e0-leader=0,e0-worker=0; pods: e0-leader,e0-worker)",
		},
		{
			name:    "a Ready gang row whose old set is still going away",
			setSize: 2,
			pods: []*corev1.Pod{
				withIncarnation(invPod("e0-leader", engine, 0, "h1", "", true), 2),
				withIncarnation(invPod("e0-worker", engine, 0, "h1", "", true), 2),
				withIncarnation(invPod("e0-worker-old", engine, 0, "h1", "", false), 1)},
			rows: []v1beta1.OMENativeInstanceStatus{{Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady, PodCount: 3}},
		},
		{
			name:    "a Ready single-pod row during its surge",
			setSize: 1,
			pods: []*corev1.Pod{
				invPod("e0-0", engine, 0, "h1", "", false),
				invPod("e0-1", engine, 0, "h2", "", true)},
			rows: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2}},
		},
		{
			name:        "a Failed row whose only pod serves",
			pods:        []*corev1.Pod{invPod("e0", engine, 0, "h1", "", true)},
			rows:        []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1}},
			wantSubject: "engine instance 0",
			wantDetail:  "row is Failed but every pod of its set serves (1 of 1, pods: e0; no failure record)",
		},
		{
			name: "a Failed row whose replacement died beside the serving source",
			pods: []*corev1.Pod{
				invPod("e0-0", engine, 0, "h1", "", true),
				invPod("e0-1", engine, 0, "h2", "", false)},
			rows: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 2, LastFailure: failure("e0-1")}},
		},
		{
			name:        "a Failed row whose failure names the pod that serves",
			pods:        []*corev1.Pod{invPod("e0", engine, 0, "h1", "", true)},
			rows:        []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1, LastFailure: failure("e0")}},
			wantSubject: "engine instance 0",
			wantDetail:  "failure record names pod e0 (ImagePullBackOff)",
		},
		{
			name:    "a Failed gang row that remembers a crash of the set now serving",
			setSize: 2,
			pods: []*corev1.Pod{
				withRestarts(invPod("e0-leader", engine, 0, "h1", "", true), 2),
				invPod("e0-worker", engine, 0, "h1", "", true)},
			rows: []v1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 2,
				ReadySince:  &metav1.Time{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
				LastFailure: &v1beta1.InstanceTermination{PodName: "e0-leader", Reason: "Error", Time: metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC))},
			}},
			wantSubject: "engine instance 0 (Failed after promotion while the set restarts)",
			wantDetail:  "failure record names pod e0-leader (Error) at 2026-01-01T00:00:30Z, after the row entered Ready at 2026-01-01T00:00:00Z (restarts: e0-leader=2,e0-worker=0; pods: e0-leader,e0-worker)",
		},
		{
			name: "a Failed row whose failure predates its Ready is the plain finding",
			pods: []*corev1.Pod{invPod("e0", engine, 0, "h1", "", true)},
			rows: []v1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1,
				ReadySince:  &metav1.Time{Time: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)},
				LastFailure: &v1beta1.InstanceTermination{PodName: "e0", Reason: "Error", Time: metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC))},
			}},
			wantSubject: "engine instance 0",
			wantDetail:  "row is Failed but every pod of its set serves (1 of 1, pods: e0; failure record names pod e0 (Error))",
		},
		{
			name:    "a Failed gang row with only part of its set serving",
			setSize: 2,
			pods: []*corev1.Pod{
				invPod("e0-leader", engine, 0, "h1", "", false),
				invPod("e0-worker", engine, 0, "h1", "", true)},
			rows: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 2}},
		},
		{
			name: "podCount off the listed pods",
			pods: []*corev1.Pod{
				invPod("e0", engine, 0, "h1", "", true),
				invPod("e0-surge", engine, 0, "h2", "", false)},
			rows:        []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1}},
			wantSubject: "engine instance 0 podCount",
			wantDetail:  "the API lists 2 pods",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := invObservation(nil, tc.pods...)
			obs.rows = map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{engine: tc.rows}
			obs.setSizes = map[v1beta1.ComponentType]int32{engine: tc.setSize}
			got := evaluateTarget(obs, map[string]int32{})
			if tc.wantSubject == "" {
				expectNoFinding(t, got, InvariantStatusTruth)
				return
			}
			expectOneFinding(t, got, InvariantStatusTruth, tc.wantSubject, tc.wantDetail)
		})
	}
}

func TestInstanceSetSize(t *testing.T) {
	ir := &v1beta1.InferenceReplica{}
	if got := instanceSetSize(ir); got != 1 {
		t.Fatalf("an InferenceReplica without runners is single-pod, got %d", got)
	}
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1},
		{Name: v1beta1.RunnerNameWorker, Size: 3},
	}
	if got := instanceSetSize(ir); got != 4 {
		t.Fatalf("a gang owns every runner's pods, got %d", got)
	}
}

func TestInvariantSustainDebounce(t *testing.T) {
	s := NewInvariantSampler(nil, InvariantTarget{Namespace: "ns", Name: "svc"}, InvariantSamplerOptions{Sustain: 5 * time.Second})
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := finding{invariant: InvariantPairableCapacity, target: "ns/svc", subject: "engine", detail: "1 pairable serving pods, floor 2"}
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

	s.foldLocked(at(0), []finding{f})
	s.foldLocked(at(2), []finding{f})
	if got := s.Violations(); len(got) != 0 {
		t.Fatalf("a span shorter than the window must not count, got %+v", got)
	}
	s.foldLocked(at(5), []finding{f})
	got := s.Violations()
	if len(got) != 1 || !got[0].FirstSeen.Equal(at(0)) || !got[0].LastSeen.Equal(at(5)) || got[0].Count != 1 {
		t.Fatalf("a span reaching the window must count once with its first and last sample, got %+v", got)
	}
	s.foldLocked(at(6), []finding{f})
	if got := s.Violations(); got[0].Count != 2 || !got[0].LastSeen.Equal(at(6)) {
		t.Fatalf("every later sample extends the violation, got %+v", got)
	}

	// A clear sample closes the span; a later blip shorter than the window
	// adds nothing, and a second key that keeps clearing never counts.
	s.foldLocked(at(7), nil)
	blip := finding{invariant: InvariantTrafficBacked, target: "ns/svc", subject: "engine svc-engine-abc", detail: "unbacked"}
	s.foldLocked(at(8), []finding{f, blip})
	s.foldLocked(at(9), []finding{f})
	s.foldLocked(at(10), []finding{f, blip})
	s.foldLocked(at(12), []finding{f})
	got = s.Violations()
	if len(got) != 1 || got[0].Count != 2 || !got[0].LastSeen.Equal(at(6)) {
		t.Fatalf("re-opened and blipping spans below the window must not count, got %+v", got)
	}
	if stopped := s.Stop(); len(stopped) != 1 {
		t.Fatalf("Stop returns the recorded violations without a running loop, got %+v", stopped)
	}
	if text := formatInvariantViolations(got); !strings.Contains(text, "I1 ns/svc engine") || !strings.Contains(text, "2 samples") {
		t.Fatalf("report line must name the invariant, target, subject and count, got %q", text)
	}
}

func TestInvariantFirstAndLastDetail(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	key := finding{invariant: InvariantTrafficBacked, target: "ns/svc", subject: "engine svc-engine-abc (a pod of another revision serves)"}
	cases := []struct {
		name string
		// details is the finding's detail on each of six consecutive samples.
		details        [6]string
		wantFirst      string
		wantLast       string
		wantLineToHold []string
	}{
		{
			name:           "a window that changes shape keeps both ends",
			details:        [6]string{"serving pods: none", "serving pods: none", "serving pods: none", "serving pods: e1", "serving pods: e1", "serving pods: e1"},
			wantFirst:      "serving pods: none",
			wantLast:       "serving pods: e1",
			wantLineToHold: []string{"first sample: serving pods: none; last sample: serving pods: e1"},
		},
		{
			name:           "a window that does not change says so once",
			details:        [6]string{"serving pods: e1", "serving pods: e1", "serving pods: e1", "serving pods: e1", "serving pods: e1", "serving pods: e1"},
			wantFirst:      "serving pods: e1",
			wantLast:       "serving pods: e1",
			wantLineToHold: []string{"1 samples: serving pods: e1 (same at first and last sample)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewInvariantSampler(nil, InvariantTarget{Namespace: "ns", Name: "svc"}, InvariantSamplerOptions{Sustain: 5 * time.Second})
			for sec, detail := range tc.details {
				f := key
				f.detail = detail
				s.foldLocked(t0.Add(time.Duration(sec)*time.Second), []finding{f})
			}
			// Samples 0 to 4 build the span; sample 5 reaches the window and
			// is recorded with the first sample's detail beside its own.
			got := s.Violations()
			if len(got) != 1 || got[0].FirstDetail != tc.wantFirst || got[0].Detail != tc.wantLast {
				t.Fatalf("want first %q and last %q, got %+v", tc.wantFirst, tc.wantLast, got)
			}
			text := formatInvariantViolations(got)
			for _, part := range tc.wantLineToHold {
				if !strings.Contains(text, part) {
					t.Fatalf("report line must hold %q, got %q", part, text)
				}
			}
		})
	}
}

func TestInvariantPodCountTolerance(t *testing.T) {
	obs := invObservation(nil,
		invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true),
		invPod("e0-surge", v1beta1.EngineComponent, 0, "h2", "", false))
	obs.rows = map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.EngineComponent: {
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1},
	}}
	fs := findingsOf(t, evaluateTarget(obs, map[string]int32{}), InvariantStatusTruth)
	if len(fs) != 1 || fs[0].window != InvariantPodCountTolerance {
		t.Fatalf("the podCount finding must carry the tolerance as its window, got %+v", fs)
	}
	podCount := fs[0]

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		// pattern is one sample per second from t0: x failing, . clear.
		pattern string
		want    int
	}{
		{name: "a span inside the tolerance never counts", pattern: strings.Repeat("x", 15), want: 0},
		{name: "a span reaching the tolerance counts", pattern: strings.Repeat("x", 16), want: 1},
		{name: "a clear sample restarts the clock", pattern: "xxxxxxxxxx." + strings.Repeat("x", 15), want: 0},
		{name: "the restarted clock reaches the tolerance on its own", pattern: "xxxxxxxxxx." + strings.Repeat("x", 16), want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewInvariantSampler(nil, InvariantTarget{Namespace: "ns", Name: "svc"}, InvariantSamplerOptions{Sustain: 5 * time.Second})
			for sec, state := range tc.pattern {
				var fs []finding
				if state == 'x' {
					fs = []finding{podCount}
				}
				s.foldLocked(t0.Add(time.Duration(sec)*time.Second), fs)
			}
			if got := s.Violations(); len(got) != tc.want {
				t.Fatalf("want %d violations, got %+v", tc.want, got)
			}
		})
	}
}

func TestDeriveComponentBudgets(t *testing.T) {
	ir := func(replicas int32, maxUnavailable *intstr.IntOrString) *v1beta1.InferenceReplica {
		out := &v1beta1.InferenceReplica{}
		out.Spec.Replicas = ptr.To(replicas)
		if maxUnavailable != nil {
			out.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: maxUnavailable},
			}}
		}
		return out
	}
	isvc := &v1beta1.InferenceService{}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt32(1))},
	}}}
	got := DeriveComponentBudgets(isvc, map[v1beta1.ComponentType]*v1beta1.InferenceReplica{
		// 50% of 4 is 2 for the Component; the group's 1 is stricter.
		v1beta1.EngineComponent: ir(4, ptr.To(intstr.FromString("50%"))),
		// No Component cap; the group's 1 applies.
		v1beta1.DecoderComponent: ir(2, nil),
		// Outside the group and uncapped: no floor.
		v1beta1.RouterComponent: ir(1, nil),
	})
	want := map[v1beta1.ComponentType]ComponentBudget{
		v1beta1.EngineComponent:  {Desired: 4, MaxUnavailable: 1},
		v1beta1.DecoderComponent: {Desired: 2, MaxUnavailable: 1},
		v1beta1.RouterComponent:  {Desired: 1, MaxUnavailable: escalation.BudgetNoLimit},
	}
	for c, w := range want {
		if got[c] != w {
			t.Fatalf("%s: want %+v, got %+v", c, w, got[c])
		}
	}
	// A percent rounds up on its own: 25% of 2 is 1, not 0.
	alone := DeriveComponentBudgets(&v1beta1.InferenceService{}, map[v1beta1.ComponentType]*v1beta1.InferenceReplica{
		v1beta1.EngineComponent: ir(2, ptr.To(intstr.FromString("25%"))),
	})
	if alone[v1beta1.EngineComponent] != (ComponentBudget{Desired: 2, MaxUnavailable: 1}) {
		t.Fatalf("25%% of 2 must round up to 1, got %+v", alone[v1beta1.EngineComponent])
	}
}
