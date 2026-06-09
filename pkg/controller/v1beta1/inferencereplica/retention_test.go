package inferencereplica

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// seedControllerRevision builds a ControllerRevision carrying the IR's
// revision Key label set and a monotonic .Revision number, named like a
// real CR (`<parent>-<component>-<suffix>`). Seeded directly into the
// fake client so the retention sweep has history to trim. Data is left
// empty — retention keys only on name + .Revision + the live-name union,
// never on the payload.
func seedControllerRevision(ir *v1beta1.InferenceReplica, suffix string, rev int64) *appsv1.ControllerRevision {
	key := irRevisionKey(ir)
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revision.Name(key, suffix),
			Namespace: ir.Namespace,
			Labels:    revision.Labels(key),
		},
		Revision: rev,
	}
}

// listRevisionNames returns the surviving CR names in the IR's namespace
// for assertion readability.
func listRevisionNames(t *testing.T, c client.Client, namespace string) map[string]struct{} {
	t.Helper()
	list := &appsv1.ControllerRevisionList{}
	if err := c.List(context.Background(), list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list ControllerRevisions: %v", err)
	}
	out := make(map[string]struct{}, len(list.Items))
	for _, cr := range list.Items {
		out[cr.Name] = struct{}{}
	}
	return out
}

// TestReconcile_RetainsControllerRevisions is the end-to-end retention
// wire-in test: it seeds far more than defaultRevisionRetention non-live
// ControllerRevisions plus one live revision (pinned via the IR's
// CurrentRevision), drives a reconcile, and asserts:
//
//   - the live revision survives even though it is the OLDEST seeded CR
//     (proving the live-name union, not just recency, protects it);
//   - non-live revisions beyond defaultRevisionRetention are deleted;
//   - exactly defaultRevisionRetention non-live CRs remain (the newest),
//     plus the 1 live one.
//
// This exercises the real reconcile defer path (sweepRevisions →
// revision.RetainControllerRevisions + revision.CollectLiveRevisionNames)
// against the fake client, so it covers the call-site wiring the unit
// tests in workload/revision/ cannot reach. The per-Instance
// RunningRevision union is covered separately by
// TestSweepRevisions_HonorsRunningRevisionUnion below (kept off the
// reconcile path so the workload dispatcher doesn't try to load the
// pinned CR's payload).
func TestReconcile_RetainsControllerRevisions(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)

	// The live revision is the OLDEST (.Revision=1) so retention by
	// recency alone would delete it — only the live-name union saves it.
	liveCR := seedControllerRevision(ir, "live", 1)

	// Pin it live via the Component-level CurrentRevision. No
	// InstanceStatuses are set so the workload dispatcher stays on the
	// Create path and never tries to load the pinned CR's payload (which
	// the seed leaves empty); the retention assertion is unaffected.
	ir.Status.CurrentRevision = liveCR.Name

	// Seed defaultRevisionRetention+5 non-live CRs with strictly higher
	// (newer) revision numbers than the live one. Retention must keep the
	// newest defaultRevisionRetention of these and drop the oldest 5.
	const extra = 5
	nonLive := make([]*appsv1.ControllerRevision, 0, defaultRevisionRetention+extra)
	for i := 0; i < defaultRevisionRetention+extra; i++ {
		// .Revision starts at 2 (above the live CR's 1) and increases.
		cr := seedControllerRevision(ir, fmt.Sprintf("nonlive%02d", i), int64(i+2))
		nonLive = append(nonLive, cr)
	}

	objs := []client.Object{ir, liveCR}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"reconcile should not fail; retention is best-effort and non-fatal")

	survivors := listRevisionNames(t, c, ir.Namespace)

	// The live (oldest) CR must survive purely on the live-name union.
	g.Expect(survivors).To(gomega.HaveKey(liveCR.Name),
		"live revision (oldest) must never be swept even past the retention bound")

	// The oldest `extra` non-live CRs (lowest .Revision) must be gone.
	for i := 0; i < extra; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"oldest non-live revision %s should have been swept", nonLive[i].Name)
	}

	// The newest defaultRevisionRetention non-live CRs must remain.
	for i := extra; i < len(nonLive); i++ {
		g.Expect(survivors).To(gomega.HaveKey(nonLive[i].Name),
			"newest non-live revision %s must be retained", nonLive[i].Name)
	}

	// The reconcile may itself ensure one fresh CR for the live PodSpec.
	// Floor the count: at least the live + the retained non-live window.
	// Upper-bound it so a regression that disables the sweep (leaving all
	// defaultRevisionRetention+extra non-live CRs) fails loudly.
	g.Expect(len(survivors)).To(gomega.BeNumerically("<", 1+defaultRevisionRetention+extra),
		"retention must have deleted at least the oldest non-live revisions")
	g.Expect(len(survivors)).To(gomega.BeNumerically(">=", 1+defaultRevisionRetention),
		"live revision plus the newest retention window must all survive")
}

// TestSweepRevisions_NonFatalOnNilRecorder pins the best-effort
// contract at the helper boundary: sweepRevisions must not panic when
// the Recorder is nil (the reconciler tolerates a nil Recorder) and must
// not return an error channel that could fail the reconcile — it returns
// nothing. Seeds no CRs so the sweep is a clean no-op.
func TestSweepRevisions_NonFatalOnNilRecorder(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	r, _ := newReconciler(t, ir)
	r.Recorder = nil // explicit: the helper must tolerate this

	g.Expect(func() {
		r.sweepRevisions(context.Background(), ir)
	}).NotTo(gomega.Panic(),
		"sweepRevisions must be a safe no-op with no history and a nil Recorder")
}

// TestSweepRevisions_HonorsRunningRevisionUnion pins the migration-
// safety case at the helper boundary: a revision pinned ONLY by a
// per-Instance RunningRevision (not CurrentRevision / UpdateRevision)
// must survive retention even when it is the oldest CR and the bound is
// exceeded. This proves sweepRevisions threads the per-Instance union
// from ir.Status.InstanceStatuses into CollectLiveRevisionNames — the
// guard that stops an in-flight migration's source CR from being swept
// out from under it. Calls sweepRevisions directly so the workload
// dispatcher (which would try to load the pinned CR's payload) stays out
// of the picture.
func TestSweepRevisions_HonorsRunningRevisionUnion(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)

	// migrationSource is the OLDEST CR, pinned ONLY via a per-Instance
	// RunningRevision — recency would delete it; the union must save it.
	migrationSource := seedControllerRevision(ir, "migsrc", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, RunningRevision: migrationSource.Name},
	}

	const extra = 3
	nonLive := make([]*appsv1.ControllerRevision, 0, defaultRevisionRetention+extra)
	for i := 0; i < defaultRevisionRetention+extra; i++ {
		nonLive = append(nonLive, seedControllerRevision(ir, fmt.Sprintf("nl%02d", i), int64(i+2)))
	}

	objs := []client.Object{ir, migrationSource}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)

	r.sweepRevisions(context.Background(), ir)

	survivors := listRevisionNames(t, c, ir.Namespace)
	g.Expect(survivors).To(gomega.HaveKey(migrationSource.Name),
		"a revision pinned only by InstanceStatus.RunningRevision must survive retention")
	// Oldest `extra` non-live trimmed; newest defaultRevisionRetention kept.
	for i := 0; i < extra; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"oldest non-live revision %s should have been swept", nonLive[i].Name)
	}
	for i := extra; i < len(nonLive); i++ {
		g.Expect(survivors).To(gomega.HaveKey(nonLive[i].Name),
			"newest non-live revision %s must be retained", nonLive[i].Name)
	}
}
