package replay

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	types "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// minimalScenario is the smallest loadable timeline, for tests that need a
// constructed driver rather than a full replay.
const minimalScenario = `
scenario: minimal
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    image: registry.example.com/runtime:v1
timeline:
  - tick: 1
`

var traceStart = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// TestNormalizerTokensAreFirstAppearanceOrdered pins the property every
// golden rests on: the same identifier always renders as the same token,
// and the token is derived from the order the run met it.
func TestNormalizerTokensAreFirstAppearanceOrdered(t *testing.T) {
	n := newNormalizer(traceStart)
	if got := n.op("b4f2"); got != "op#1" {
		t.Fatalf("first operation: got %q want op#1", got)
	}
	if got := n.op("9ac1"); got != "op#2" {
		t.Fatalf("second operation: got %q want op#2", got)
	}
	if got := n.op("b4f2"); got != "op#1" {
		t.Fatalf("repeat operation: got %q want op#1", got)
	}
	if got := n.uid("pod-uid-1"); got != "uid#1" {
		t.Fatalf("first uid: got %q want uid#1", got)
	}
	if got := n.op(""); got != "" {
		t.Fatalf("empty id must not consume a token, got %q", got)
	}
}

// TestNormalizerRegistersNameAndHash proves a revision reads the same
// whether a line names the object or only its hash.
func TestNormalizerRegistersNameAndHash(t *testing.T) {
	n := newNormalizer(traceStart)
	n.revision("svc-a-engine-882059bb", "882059bb")
	n.revision("svc-a-engine-6f8b1e6c", "6f8b1e6c")
	for name, want := range map[string]string{
		"svc-a-engine-882059bb": "rev#1",
		"882059bb":              "rev#1",
		"svc-a-engine-6f8b1e6c": "rev#2",
		"6f8b1e6c":              "rev#2",
		"svc-a-engine-unknown":  "svc-a-engine-unknown",
	} {
		if got := n.rev(name); got != want {
			t.Errorf("rev(%q): got %q want %q", name, got, want)
		}
	}
}

// TestNormalizerTextReplacesLongestKeyFirst is the guard against a hash
// shadowing the revision name it is a substring of: replacing "882059bb"
// before "svc-a-engine-882059bb" would leave "svc-a-engine-rev#1".
func TestNormalizerTextReplacesLongestKeyFirst(t *testing.T) {
	n := newNormalizer(traceStart)
	n.revision("svc-a-engine-882059bb", "882059bb")
	got := n.text("revision svc-a-engine-882059bb (hash 882059bb) rolled")
	if want := "revision rev#1 (hash rev#1) rolled"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

// TestNormalizerTextRewritesTimestamps keeps a wall-clock instant the
// engine embedded in a message from making every run differ.
func TestNormalizerTextRewritesTimestamps(t *testing.T) {
	n := newNormalizer(traceStart)
	got := n.text("deadline 2024-01-01T00:02:00Z elapsed")
	if want := "deadline t+2m0s elapsed"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := n.text("not a time: 12:00:00"); got != "not a time: 12:00:00" {
		t.Fatalf("a non-timestamp must survive verbatim, got %q", got)
	}
}

func TestNormalizerAtRendersBothDirections(t *testing.T) {
	n := newNormalizer(traceStart)
	if got := n.at(traceStart.Add(90 * time.Second)); got != "t+1m30s" {
		t.Fatalf("forward: got %q", got)
	}
	if got := n.at(traceStart.Add(-time.Minute)); got != "t-1m0s" {
		t.Fatalf("backward: got %q", got)
	}
	if got := n.at(time.Time{}); got != "nil" {
		t.Fatalf("zero: got %q", got)
	}
}

// TestMapDiffRecordsValues pins the fix that made the in-place lock real:
// a changed key is recorded with both of its values, normalized.
func TestMapDiffRecordsValues(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	d.norm.revision("svc-a-engine-882059bb", "882059bb")
	got := d.mapDiff(
		map[string]string{"keep": "same", "drop": "gone", "hash": "882059bb"},
		map[string]string{"keep": "same", "hash": "882059bb", "add": "new"},
	)
	if want := "[add:nil→new drop:gone→nil]"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := d.mapDiff(map[string]string{"a": "1"}, map[string]string{"a": "1"}); got != "" {
		t.Fatalf("an unchanged map must produce no diff, got %q", got)
	}
}

func TestPodWriteDiffCoversEveryWrittenField(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	before := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{query.LabelRevisionHash: "old"},
			Annotations: map[string]string{"marker": "a"},
			Finalizers:  []string{"keep"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main", Image: "img:v1",
			Env: []corev1.EnvVar{{Name: "OME_PEER", Value: "one"}},
		}}},
	}
	after := before.DeepCopy()
	after.Labels[query.LabelRevisionHash] = "new"
	after.Annotations["marker"] = "b"
	after.Finalizers = nil
	after.Spec.Containers[0].Image = "img:v2"
	after.Spec.Containers[0].Env[0].Value = "two"
	after.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "queue"}}
	setPodCondition(after, query.ServingConditionType, corev1.ConditionTrue, traceStart)

	got := d.podWriteDiff(before, after)
	for _, want := range []string{
		"images=main=img:v1→main=img:v2",
		"serving=absent→True",
		"labels=[ome.io/revision-hash:old→new]",
		"annotations=[marker:a→b]",
		"env=[main.OME_PEER:one→two]",
		"schedulingGates=nil→[queue]",
		"finalizers=[keep]→nil",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if got := d.podWriteDiff(before, before.DeepCopy()); got != "unchanged-fields" {
		t.Fatalf("an inert write must say so, got %q", got)
	}
}

// TestPodWriteDiffIgnoresServingTransitionTime keeps the one wall-clock
// value the engine writes out of the goldens.
func TestPodWriteDiffIgnoresServingTransitionTime(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	before := &corev1.Pod{}
	setPodCondition(before, query.ServingConditionType, corev1.ConditionTrue, traceStart)
	after := before.DeepCopy()
	after.Status.Conditions[0].LastTransitionTime = metav1.NewTime(traceStart.Add(time.Hour))
	if got := d.podWriteDiff(before, after); got != "unchanged-fields" {
		t.Fatalf("a moved transition time must not reach the trace, got %q", got)
	}
}

func TestResourceOfNamesEveryKind(t *testing.T) {
	for _, tc := range []struct {
		obj  client.Object
		want string
	}{
		{&corev1.Pod{}, "pods"},
		{&corev1.Service{}, "services"},
		{&discoveryv1.EndpointSlice{}, "endpointslices"},
	} {
		if got := resourceOf(tc.obj); got != tc.want {
			t.Errorf("got %q want %q", got, tc.want)
		}
	}
}

// TestTakeRejectionIgnoresStagedWrites is the fidelity guard on injection:
// a refusal armed for the engine must survive every write the scenario
// itself makes to set the stage.
func TestTakeRejectionIgnoresStagedWrites(t *testing.T) {
	d := &driver{rejections: []*pendingRejection{{
		verb: "create", resource: "pods", remaining: 1,
		err: errors.New("refused"), label: "api.quotaDenied",
	}}}
	d.quiet = true
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a staged write consumed the rejection")
	}
	d.quiet = false
	if err, label := d.takeRejection("create", &corev1.Pod{}); err == nil || label != "api.quotaDenied" {
		t.Fatalf("the engine's write did not consume the rejection: %v %q", err, label)
	}
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a one-shot rejection fired twice")
	}
}

func TestTakeRejectionMatchesVerbAndResource(t *testing.T) {
	arm := func() *driver {
		return &driver{rejections: []*pendingRejection{{
			verb: "patch", resource: "pods", remaining: -1, err: errors.New("refused"), label: "api.invalid",
		}}}
	}
	if err, _ := arm().takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a create consumed a patch rejection")
	}
	if err, _ := arm().takeRejection("patch", &corev1.Service{}); err != nil {
		t.Fatalf("a service write consumed a pod rejection")
	}
	d := arm()
	for i := 0; i < 3; i++ {
		if err, _ := d.takeRejection("patch", &corev1.Pod{}); err == nil {
			t.Fatalf("an unbounded rejection stopped firing at %d", i)
		}
	}
}

func TestInstanceIndexOfRejectsAMissingLabel(t *testing.T) {
	if _, err := instanceIndexOf(&corev1.Pod{}); err == nil {
		t.Fatalf("a pod with no instance label must not read as index 0")
	}
	bad := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "x"}}}
	if _, err := instanceIndexOf(bad); err == nil {
		t.Fatalf("an unparseable instance label must not read as index 0")
	}
	good := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "7"}}}
	idx, err := instanceIndexOf(good)
	if err != nil || idx != 7 {
		t.Fatalf("got %d, %v", idx, err)
	}
}

// TestApplyOperationDeadlineNeedsASlack keeps the harness from inventing a
// behavioral value the scenario did not state.
func TestApplyOperationDeadlineNeedsASlack(t *testing.T) {
	d := &driver{store: NewRowStore(nil, nil, nil)}
	_, err := applyOperationDeadline(context.Background(), d, TimelineEvent{ID: "timer.operationDeadline"})
	if err == nil || !strings.Contains(err.Error(), "no in-flight operation") {
		t.Fatalf("expected the no-deadline error, got %v", err)
	}
}

// TestRemoveInstanceForgetsTheExpectation pins the adapter behavior the
// driver stands in for: a row that left the owner status takes its
// create/delete expectation with it, so a later Instance reusing the index
// does not inherit a wait nobody will satisfy.
func TestRemoveInstanceForgetsTheExpectation(t *testing.T) {
	ctx := context.Background()
	scenario, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(ctx, scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	if err := d.store.MutateInstance(ctx, 0, func(s *types.InstanceStatus) bool {
		s.Phase = types.InstancePhaseCreating
		return true
	}); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	d.expectations.ExpectCreates(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0, 1)
	if d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("an outstanding create must not read as satisfied")
	}

	input := types.ReconcileInput{OwnerObject: d.owner}
	d.store.Install(&input)
	d.wrapCallbacks(&input)
	if _, err := input.RemoveInstance(ctx, 0); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("removing the row left its expectation behind")
	}
}
