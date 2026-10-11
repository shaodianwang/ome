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

func TestInvariantTrafficBacked(t *testing.T) {
	traffic := func(targets ...v1beta1.ComponentTrafficTarget) *v1beta1.InferenceService {
		isvc := &v1beta1.InferenceService{}
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Traffic: targets},
		}
		return isvc
	}

	t.Run("a serving pod on the revision backs the target", func(t *testing.T) {
		obs := invObservation(nil, invPod("e0", v1beta1.EngineComponent, 0, "abc", "", true))
		obs.isvc = traffic(v1beta1.ComponentTrafficTarget{RevisionName: "svc-engine-abc", Percent: 100})
		expectNoFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantTrafficBacked)
	})

	t.Run("a target with no serving pod on its revision is unbacked", func(t *testing.T) {
		obs := invObservation(nil,
			invPod("e0", v1beta1.EngineComponent, 0, "def", "", true),
			invPod("e1", v1beta1.EngineComponent, 1, "abc", "", false))
		obs.isvc = traffic(
			v1beta1.ComponentTrafficTarget{RevisionName: "svc-engine-abc", Percent: 100},
			v1beta1.ComponentTrafficTarget{RevisionName: "svc-engine-old", Percent: 0})
		got := evaluateTarget(obs, map[string]int32{})
		// The zero-percent target asks for nothing; the live one has only a
		// serving pod on another revision and a non-serving pod on its own.
		expectOneFinding(t, got, InvariantTrafficBacked, "engine svc-engine-abc", "no serving pod carries revision hash abc")
	})
}

func TestInvariantStatusTruth(t *testing.T) {
	rows := func(rs ...v1beta1.OMENativeInstanceStatus) map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus {
		return map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.EngineComponent: rs}
	}

	t.Run("rows match the pods", func(t *testing.T) {
		obs := invObservation(nil,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true),
			invPod("e1", v1beta1.EngineComponent, 1, "h1", "", false))
		obs.rows = rows(
			v1beta1.OMENativeInstanceStatus{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1},
			v1beta1.OMENativeInstanceStatus{Index: 1, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1})
		expectNoFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantStatusTruth)
	})

	t.Run("a Ready row without a serving pod", func(t *testing.T) {
		obs := invObservation(nil, invPod("e0", v1beta1.EngineComponent, 0, "h1", "", false))
		obs.rows = rows(v1beta1.OMENativeInstanceStatus{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1})
		expectOneFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantStatusTruth, "engine instance 0", "row is Ready but no pod")
	})

	t.Run("a Failed row with a serving pod", func(t *testing.T) {
		obs := invObservation(nil, invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true))
		obs.rows = rows(v1beta1.OMENativeInstanceStatus{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, PodCount: 1})
		expectOneFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantStatusTruth, "engine instance 0", "row is Failed but 1 pods")
	})

	t.Run("podCount off the listed pods", func(t *testing.T) {
		obs := invObservation(nil,
			invPod("e0", v1beta1.EngineComponent, 0, "h1", "", true),
			invPod("e0-surge", v1beta1.EngineComponent, 0, "h2", "", false))
		obs.rows = rows(v1beta1.OMENativeInstanceStatus{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1})
		expectOneFinding(t, evaluateTarget(obs, map[string]int32{}), InvariantStatusTruth, "engine instance 0 podCount", "the API lists 2 pods")
	})
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
