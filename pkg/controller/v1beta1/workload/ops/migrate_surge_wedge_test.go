// Tests for the early close a migration takes when its surge pod is
// parked in a terminal kubelet waiting reason: the record — not the
// source row — is what fails, the source goes back to serving on the
// revision it never left, and the surge index is unpinned for the
// ordinary scale-down pipeline. Shares the entry-lifecycle harness
// (migFixture) with an injected fake clock and event recorder.
package ops

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// wedgeGrace is the stuck-pod grace the wedge fixtures configure. Long
// enough that a freshly created pod is inside it, so the tests can step
// the clock across the boundary explicitly.
const wedgeGrace = 5 * time.Minute

// migWedgeEvents drains the fixture recorder.
func migWedgeEvents(t *testing.T, f *migFixture) []string {
	t.Helper()
	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// armWedgeFixture wires the fake clock, the recorder and the stuck-pod
// grace, then seeds one Manual record whose Deadline is far enough out
// that only the evidence can end the migration.
func armWedgeFixture(t *testing.T, f *migFixture, uuid string, sourceIdx int32, fromNode string) *clocktesting.FakeClock {
	t.Helper()
	clk := f.withFakeClock()
	f.recorder = record.NewFakeRecorder(16)
	f.stuckPodGrace = wedgeGrace
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, sourceIdx, fromNode, clk.Now().Add(4*time.Hour)),
	}
	return clk
}

// migPodsForInstance returns the fixture's live pods for one index.
func migPodsForInstance(t *testing.T, f *migFixture, idx int32) []*corev1.Pod {
	t.Helper()
	var out []*corev1.Pod
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == strconv.Itoa(int(idx)) {
			out = append(out, pod)
		}
	}
	return out
}

// wedgePod parks a container of pod in reason, drops ContainersReady
// the way a kubelet does for a container that will not start, and
// stamps a creation time the fake clock can be advanced past — the
// kubelet exposes no per-state transition time, so creation is what the
// grace is measured from.
func wedgePod(t *testing.T, f *migFixture, pod *corev1.Pod, reason string) {
	t.Helper()
	ctx := context.Background()
	if pod.CreationTimestamp.IsZero() {
		pod.CreationTimestamp = metav1.NewTime(f.clk.Now())
		if err := f.c.Update(ctx, pod); err != nil {
			t.Fatalf("stamp pod %s creation time: %v", pod.Name, err)
		}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	kept := make([]corev1.PodCondition, 0, len(pod.Status.Conditions)+1)
	for _, c := range pod.Status.Conditions {
		if c.Type != corev1.ContainersReady {
			kept = append(kept, c)
		}
	}
	pod.Status.Conditions = append(kept, corev1.PodCondition{
		Type: corev1.ContainersReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(f.clk.Now()),
	})
	if err := f.c.Status().Update(ctx, pod); err != nil {
		t.Fatalf("wedge pod %s in %s: %v", pod.Name, reason, err)
	}
}

// driveToSurgePods runs passes until the surge index has live pods, and
// returns them. The gang shape needs one extra pass so its PodGroup is
// created before its members render.
func driveToSurgePods(t *testing.T, f *migFixture, uuid string, surgeIdx int32) []*corev1.Pod {
	t.Helper()
	for i := 0; i < 4; i++ {
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want in-flight", i+1, done, accepted)
		}
		if pods := migPodsForInstance(t, f, surgeIdx); len(pods) > 0 {
			return pods
		}
	}
	t.Fatalf("surge index %d never got pods; record=%+v", surgeIdx, *f.record(t, uuid))
	return nil
}

// assertSourceRestored pins the restore half of the close: the source is
// Ready again on the RunningRevision it never left, unpinned, and back
// in rotation with the drive's drain hold gone.
func assertSourceRestored(t *testing.T, f *migFixture, sourceIdx int32, uuid, wantRevision string) {
	t.Helper()
	src := findInstanceStatusOnIRForFixture(t, f, sourceIdx)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must be restored to Ready and unpinned; got %+v", src)
	}
	if src.RunningRevision != wantRevision {
		t.Errorf("source RunningRevision = %q, want the untouched %q", src.RunningRevision, wantRevision)
	}
	if src.LastFailure != nil {
		t.Errorf("a healthy source must not be stamped with a failure; got %+v", src.LastFailure)
	}
	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	for _, pod := range migPodsForInstance(t, f, sourceIdx) {
		if podreadiness.ContainsNotReadyKey(pod, drainKey) || !podreadiness.IsServing(pod) {
			t.Errorf("source pod %s must be back in rotation with the drain hold released; conditions=%+v",
				pod.Name, pod.Status.Conditions)
		}
	}
}

// assertNoRetryBlocks pins that the close charges no revision.
func assertNoRetryBlocks(t *testing.T, f *migFixture) {
	t.Helper()
	if blocks := f.getIR(t).Status.RetryBlocks; len(blocks) != 0 {
		t.Errorf("a wedged surge blames no revision; got RetryBlocks=%+v", blocks)
	}
}

// A surge pod that cannot pull its image can never take over, so the
// migration is closed on that evidence long before the record's
// Deadline: the record carries the pod and the reason, the source is
// restored to Ready on its untouched revision and back in rotation, the
// surge index is unpinned for the bounded scale-down pipeline, one
// Warning names the wedge and no revision is blamed.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[ImagePullBackOff]/before, instance/Migrate/SurgeTarget/pod.waiting[ImagePullBackOff]/before
func TestMigrate_SurgeWedgedInImagePull_FailsThroughRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-pull"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	if sourceRevision == "" {
		t.Fatal("setup: source has no RunningRevision")
	}
	wedgePod(t, f, surgePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f) // drop the accept event

	done, accepted := f.pass(t, uuid)
	if !done || !accepted {
		t.Fatalf("wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}

	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("record must close Failed with CompletedAt; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, surgePods[0].Name) || !strings.Contains(rec.Message, "ImagePullBackOff") {
		t.Errorf("record Message must name the pod and the reason; got %q", rec.Message)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)

	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Operation != nil {
		t.Fatalf("surge must keep its slot with the pin cleared so scale-down takes it; got %+v", surge)
	}

	events := migWedgeEvents(t, f)
	if len(events) != 1 {
		t.Fatalf("want exactly one event for the close; got %v", events)
	}
	if !strings.Contains(events[0], string(workload.EventReasonMigrationSurgeWedged)) ||
		!strings.Contains(events[0], surgePods[0].Name) ||
		!strings.Contains(events[0], "ImagePullBackOff") {
		t.Errorf("the Warning must name the pod and the reason; got %q", events[0])
	}
}

// A crash-looping surge is only evidence once it has been crash-looping
// for the configured grace: inside it the migration keeps driving, past
// it the record closes. The grace is the operator's, so an unconfigured
// one leaves the record's Deadline as the only consumer.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[CrashLoopBackOff]/before, instance/Migrate/SurgeTarget/pod.waiting[CrashLoopBackOff]/before
func TestMigrate_SurgeCrashLoop_ClosesOnlyPastTheGrace(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-crash"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, surgePods[0], "CrashLoopBackOff")

	clk.Step(wedgeGrace - time.Minute)
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("inside the grace: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("inside the grace the record must stay open; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Operation == nil {
		t.Fatalf("inside the grace the source keeps its pin; got %+v", src)
	}

	clk.Step(2 * time.Minute)
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("past the grace: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, "CrashLoopBackOff") {
		t.Fatalf("past the grace the record must close Failed naming the reason; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)
}

// The record is the single authority and the close is idempotent: a
// second pass over the same evidence re-reads a terminal record and
// writes nothing, and a record already terminal for another cause is
// not reopened by the evidence either.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[ImagePullBackOff]/boundary@T-migrate-surge-terminal, instance/Migrate/SurgeTarget/pod.waiting[ImagePullBackOff]/boundary@T-migratetarget-terminal
func TestMigrate_SurgeWedgeCloseIsIdempotent(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-idem"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "ErrImagePull")
	clk.Step(wedgeGrace + time.Second)
	if done, _ := f.pass(t, uuid); !done {
		t.Fatal("first wedge pass must close the migration")
	}
	closed := *f.record(t, uuid)
	surgeBefore := findInstanceStatusOnIRForFixture(t, f, 1)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("second pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
	}
	after := f.record(t, uuid)
	if after.Message != closed.Message || !after.CompletedAt.Equal(closed.CompletedAt) {
		t.Errorf("second pass rewrote the terminal record: %+v -> %+v", closed, *after)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("second pass must be silent; got %v", events)
	}
	if got := findInstanceStatusOnIRForFixture(t, f, 1); !equalitySafeOperation(surgeBefore, got) {
		t.Errorf("second pass rewrote the surge slot: %+v -> %+v", surgeBefore, got)
	}
}

// A record already closed by another authority is left alone: the
// evidence pass reads the terminal phase and returns without touching
// the message, the pair, or the event stream.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[ImagePullBackOff]/boundary@T-migrate-surge-terminal
func TestMigrate_SurgeWedgeDoesNotReopenTerminalRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-terminal"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "InvalidImageName")
	clk.Step(wedgeGrace + time.Second)
	f.record(t, uuid).Phase = workload.MigrationPhaseFailed
	f.record(t, uuid).Message = "closed by an earlier authority"
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("terminal record: got done=%v accepted=%v, want it handled", done, accepted)
	}
	if msg := f.record(t, uuid).Message; msg != "closed by an earlier authority" {
		t.Errorf("a terminal record must not be reopened; message = %q", msg)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Operation == nil {
		t.Errorf("a terminal record drives no restore; source = %+v", src)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("a terminal record emits nothing; got %v", events)
	}
}

// The evidence read is scoped to the surge. A source pod wedged
// mid-migration is the row's own pod, which no escalation pass owns
// while the Migrate pin is held, so the record keeps driving and its
// Deadline stays the only bound.
//
// Cells: instance/Migrate/CreateSurge/timer.stuckPodGrace/before
func TestMigrate_WedgedSourcePodLeavesTheMigrationDriving(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-source"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	driveToSurgePods(t, f, uuid, 1)
	sourcePods := migPodsForInstance(t, f, 0)
	if len(sourcePods) != 1 {
		t.Fatalf("setup: want one source pod, got %d", len(sourcePods))
	}
	wedgePod(t, f, sourcePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("wedged source: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("a wedged source does not close the record; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Fatalf("a wedged source keeps its Migrate pin; got %+v", src)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("a wedged source announces nothing from the migration pass; got %v", events)
	}
}

// One wedged member is enough for a gang surge: the whole replacement
// can never be admitted, so the record closes on that member's evidence
// and the gang source goes back to serving intact.
//
// Cells: instance/Migrate/SurgeTarget/pod.waiting[ImagePullBackOff]/before
func TestMigrate_GangSurgeMemberWedged_FailsThroughRecord(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-wedge-gang"
	clk := armWedgeFixture(t, f, uuid, 0, "node-b")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	if len(surgePods) != 3 {
		t.Fatalf("setup: want a 3-pod surge gang, got %d", len(surgePods))
	}
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedged := surgePods[len(surgePods)-1]
	wedgePod(t, f, wedged, "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("gang wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, wedged.Name) {
		t.Fatalf("record must close Failed naming the wedged member; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation != nil {
		t.Fatalf("the surge gang index must be unpinned for scale-down; got %+v", surge)
	}
}

// A surge that wedges once the drain is already under way still closes
// through the record, and the close is what puts the source back: the
// drive's drain hold comes off, the pod re-enters rotation, and the
// unpinned surge index leaves through the ordinary scale-down pipeline.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[CrashLoopBackOff]/before, instance/Migrate/SurgeTarget/pod.waiting[CrashLoopBackOff]/before
func TestMigrate_SurgeWedgedWhileDraining_RestoresRotation(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-draining"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	srcPod := migPodsForInstance(t, f, 0)[0]
	if podreadiness.IsServing(srcPod) || !podreadiness.ContainsNotReadyKey(srcPod, drainKey) {
		t.Fatalf("setup: the drive must have drained the source pod; conditions=%+v", srcPod.Status.Conditions)
	}
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("draining wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record must close Failed; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)

	// Once the endpoint controller settles, the un-drained source is
	// routable again in its per-revision Service.
	f.react(t)
	srcPod = migPodsForInstance(t, f, 0)[0]
	srcSvc := query.PerRevisionServiceName(f.isvc.Name, f.component, testRevisionHashLegacy)
	inRotation, rerr := drain.IsPodInRotation(context.Background(), f.c, f.isvc.Namespace, srcSvc, srcPod)
	if rerr != nil {
		t.Fatalf("check source rotation: %v", rerr)
	}
	if !inRotation {
		t.Fatal("the restored source must re-enter rotation")
	}

	// The unpinned surge index is torn down by the ordinary bounded
	// scale-down pipeline, not by anything the migration does.
	removed := false
	for i := 0; i < 8 && !removed; i++ {
		removed = f.deleteExtraBatch(t, 1)
		f.react(t)
	}
	if !removed {
		t.Fatal("the unpinned surge index must converge to gone through scale-down")
	}
	if pods := migPodsForInstance(t, f, 1); len(pods) != 0 {
		t.Errorf("scale-down must leave no surge pods; got %d", len(pods))
	}
}

// A source that is already drained and deleted has nothing to restore
// to rotation, so the evidence does not close the record from here: the
// pair stays as it is and the record's Deadline remains the backstop.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[CrashLoopBackOff]/before
func TestMigrate_SurgeWedgedAfterSourceGone_LeavesTheDeadline(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-postdrain"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	for _, pod := range migPodsForInstance(t, f, 0) {
		if err := f.c.Delete(context.Background(), pod); err != nil {
			t.Fatalf("delete source pod %s: %v", pod.Name, err)
		}
	}
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("post-drain wedge: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseDraining {
		t.Fatalf("the record must stay Draining for its Deadline; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Fatalf("a source with no pods is not restored; got %+v", src)
	}
	if src.LastFailure != nil {
		t.Errorf("a source with no pods is not stamped Failed either; got %+v", src.LastFailure)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("nothing is announced when the evidence does not apply; got %v", events)
	}
}

// The close is re-derived from the record and the surge pods alone, so
// a crash part-way through the unwind resumes: the surge is already
// unpinned, which makes the pair unconfirmable, and the next pass still
// finishes the close instead of waiting out the Deadline.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[CrashLoopBackOff]/before, instance/Migrate/CreateSurge/ctrl.crash/before
func TestMigrate_SurgeWedgeCloseResumesAfterPartialUnwind(t *testing.T) {
	f := newSinglePodMigFixture(t)
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
	const uuid = "mig-wedge-resume"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)

	// The crash window: the surge pin is gone, the source restore and
	// the record write never landed.
	if err := clearMigrateOperation(context.Background(), f.input(t), 1, uuid); err != nil {
		t.Fatalf("simulate a crash after the surge unpin: %v", err)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation != nil {
		t.Fatalf("crash window setup: the surge must be unpinned; got %+v", surge)
	}
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want the close finished", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("the resume pass must close the record; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
}

// equalitySafeOperation reports whether two status slots carry the same
// operation pin, tolerating either being absent.
func equalitySafeOperation(a, b *v1beta1.OMENativeInstanceStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return (a.Operation == nil) == (b.Operation == nil) && a.Phase == b.Phase
}
