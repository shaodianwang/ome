package ops

// A migration is driven by its record, and the record's Deadline is the
// timeout authority for the pair. These tests pin what that ownership
// means at the seams: which apiserver rejections end the request early
// instead of idling to the Deadline, which readings and edits do not
// reach the pair at all, and what a close on the surge's own evidence
// leaves for a re-delivered edge to find.

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// driveToStampedPair runs passes until both rows of the pair exist: the
// source pinned Migrating and the replacement marker at the surge index.
// Every rejection test needs that, because a rejection observed before
// the stamp would only reach the source's state.
func driveToStampedPair(t *testing.T, f *migFixture, uuid string, surgeIdx int32) {
	t.Helper()
	for i := 0; i < 4; i++ {
		if findInstanceStatusOnIRForFixture(t, f, surgeIdx) != nil {
			return
		}
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want the pair still being stamped", i+1, done, accepted)
		}
	}
	if findInstanceStatusOnIRForFixture(t, f, surgeIdx) == nil {
		t.Fatalf("the pair was never stamped; record=%+v", *f.record(t, uuid))
	}
}

// migPassWithRejectedCreates runs one Migrate pass whose pod creates the
// apiserver answers with reject, and reports the RetryBlock writes it
// asked for.
func migPassWithRejectedCreates(t *testing.T, f *migFixture, uuid string, reject error) (done bool, blocks []string) {
	t.Helper()
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      rec.SourceInstance,
		FromNode:      rec.FromNode,
		Reason:        rec.Reason,
	}
	in := f.input(t)
	in.Pacing = &workload.APIPacing{}
	recordBlocks := func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			blocks = append(blocks, rev)
		}
		return nil
	}
	in.MutateRetryBlock = recordBlocks
	deps := f.deps()
	deps.Client = rejectPodCreates(t, f.c, reject)

	// The surge create is not always the pass right after the stamp, so
	// drive until the rejection has been answered one way or the other.
	for i := 0; i < 5; i++ {
		var accepted bool
		var err error
		done, accepted, err = Migrate(context.Background(), deps, in, f.plan, rec.SourceInstance, uuid, req)
		if err != nil {
			t.Fatalf("Migrate over a rejected create: %v", err)
		}
		if !accepted {
			t.Fatalf("Migrate: accepted=false want the record handled")
		}
		if done {
			return done, blocks
		}
		legacyResetExpectations(t)
		in = f.input(t)
		in.Pacing = &workload.APIPacing{}
		in.MutateRetryBlock = recordBlocks
	}
	return done, blocks
}

// TestMigrateSurge_RejectionDispositions: the replacement's create is the
// one write a migration cannot retry its way past. A 422 and a
// terminating namespace are both permanent, so the request is closed on
// the first rejection instead of idling to its record Deadline — and
// neither blames the revision, because the replacement carries the
// request's placement overlay and the rejection may indict that instead.
// A 429 is the apiserver pacing us: nothing is written and the create is
// still polled to the Deadline like any other blocked create.
//
// Cells: instance/Migrate/CreateSurge/api.invalid/before, instance/Migrate/CreateSurge/api.namespaceTerminating/before, instance/Migrate/CreateSurge/api.throttled[TooManyRequests]/before, instance/Migrate/SurgeTarget/api.invalid/before, instance/Migrate/SurgeTarget/api.namespaceTerminating/before, instance/Migrate/SurgeTarget/api.throttled[TooManyRequests]/before
func TestMigrateSurge_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reject   error
		wantDone bool
	}{
		{name: "invalid pod spec", reject: siteInvalidError(), wantDone: true},
		{name: "namespace terminating", reject: siteNamespaceTerminatingError("engine-pod"), wantDone: true},
		{name: "throttled", reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			uuid := "mig-reject-" + t.Name()
			f.records = []workload.MigrationRecord{
				mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
			}
			done, blocks := migPassWithRejectedCreates(t, f, uuid, tc.reject)
			rec := f.record(t, uuid)

			// The pair stamp precedes the create, so the rejection is
			// observed by a row that already exists on both sides.
			if rec.SurgeInstance == nil {
				t.Fatalf("the replacement was never allocated; record=%+v", *rec)
			}
			if findInstanceStatusOnIRForFixture(t, f, *rec.SurgeInstance) == nil {
				t.Fatalf("the replacement row is missing; record=%+v", *rec)
			}

			if done != tc.wantDone {
				t.Fatalf("done: got %v want %v (record=%+v)", done, tc.wantDone, *rec)
			}
			if tc.wantDone {
				if rec.Phase != workload.MigrationPhaseFailed {
					t.Errorf("record phase: got %s want Failed", rec.Phase)
				}
				if rec.CompletedAt == nil {
					t.Errorf("record CompletedAt: got nil want the close timestamp")
				}
			} else {
				if rec.Phase.Terminal() {
					t.Errorf("record phase: got %s want the move still in flight; a 429 is pacing", rec.Phase)
				}
			}
			// Neither disposition charges the revision: an overlay-bearing
			// replacement says nothing about the pod template.
			if len(blocks) != 0 {
				t.Errorf("RetryBlock writes: got %v want none", blocks)
			}
		})
	}
}

// TestMigrate_DeadlineExceededIsNotEarlyFailureEvidence: the terminal
// waiting set every reader of this evidence shares does not contain
// DeadlineExceeded, so a surge parked in it is not closed early however
// long it sits. The pair keeps driving and the record Deadline stays the
// only bound on the move.
//
// Cells: instance/Migrate/CreateSurge/pod.waiting[DeadlineExceeded]/before, instance/Migrate/SurgeTarget/pod.waiting[DeadlineExceeded]/before
func TestMigrate_DeadlineExceededIsNotEarlyFailureEvidence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-deadline-exceeded"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "DeadlineExceeded")
	clk.Step(wedgeGrace + time.Hour)
	migWedgeEvents(t, f)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("DeadlineExceeded closed the record early: %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Errorf("source: got %+v want the Migrate pin held", src)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("replacement: got %+v want it still coming up", surge)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("events: got %v want none; this reading is not evidence", events)
	}
}

// TestMigrate_RevisionRetargetIsDeclinedWhileTheRecordDrives: the update
// trigger declines outright for a migrate-owned status, on both rows —
// the source is mid-move and the replacement is a marker driven by its
// pair, never an independent update target. A migration surge and an
// update surge therefore never stack on one Instance; the new revision
// rolls once the record is terminal.
//
// Cells: instance/Migrate/CreateSurge/spec.revision/before, instance/Migrate/SurgeTarget/spec.revision/before
func TestMigrate_RevisionRetargetIsDeclinedWhileTheRecordDrives(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-revision-retarget"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, uuid, 1)

	// The operator publishes a new revision mid-move.
	nextSpec := legacyTargetSpecImage("llama:v2")
	nextCR := legacyEnsureTargetCR(t, f.c, f.isvc, nextSpec)
	in := f.input(t)
	in.ObservedState.UpdateRevision = nextCR.Name
	in.DesiredSpec.PodSpec = nextSpec

	for _, idx := range []int32{0, 1} {
		row := findInstanceStatusOnIRForFixture(t, f, idx)
		if row == nil || row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationMigrate {
			t.Fatalf("instance %d: got %+v want a Migrate-owned row", idx, row)
		}
		inst := workload.InstancePlan{
			Index:       idx,
			Incarnation: row.Incarnation,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}
		trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), f.deps(), in, f.plan, inst,
			nextCR, migPodsForInstance(t, f, idx))
		if err != nil {
			t.Fatalf("DetectUpdateTriggerWithPods (instance=%d): %v", idx, err)
		}
		if trigger {
			t.Errorf("instance %d: got trigger=true want the update declined while the record drives", idx)
		}
		if retryAfter != 0 {
			t.Errorf("instance %d: got retryAfter=%v want 0 (the record's own cadence drives the move)", idx, retryAfter)
		}
	}

	// The pair is untouched by the decline: no target revision is stamped
	// onto either row.
	for _, idx := range []int32{0, 1} {
		row := findInstanceStatusOnIRForFixture(t, f, idx)
		if row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationMigrate {
			t.Errorf("instance %d Operation: got %+v want the Migrate pin intact", idx, row.Operation)
		}
		if row.Operation != nil && row.Operation.TargetRevision == nextCR.Name {
			t.Errorf("instance %d: the new revision retargeted a pinned row", idx)
		}
	}
}

// TestMigrate_SecondRequestQueuesBehindTheInFlightPair: dispatch is
// serial and oldest-first, so a second request that arrives while a pair
// is in flight sits in status.migrations as a queued Accepted record. It
// allocates no surge and touches neither row, and a re-delivery of the
// uuid already in flight is the same record rather than a second one.
//
// Cells: instance/Migrate/CreateSurge/spec.migrateRequest/before
func TestMigrate_SecondRequestQueuesBehindTheInFlightPair(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const inFlight, queued = "mig-in-flight", "mig-queued"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(inFlight, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, inFlight, 1)
	before := *findInstanceStatusOnIRForFixture(t, f, 0)

	// The operator asks for the same Instance again, and the same uuid is
	// re-delivered alongside it.
	second := mkMigRecordWithDeadline(queued, 0, "node-a", time.Now().Add(4*time.Hour))
	second.StartedAt = metav1.NewTime(time.Now())
	f.records = append(f.records, second)

	if picked := workload.NextManualMigration(f.records); picked == nil || picked.RequestUUID != inFlight {
		t.Fatalf("dispatcher picked %+v, want the record already in flight", picked)
	}
	if done, accepted := f.pass(t, inFlight); done || !accepted {
		t.Fatalf("re-delivering the in-flight uuid: got done=%v accepted=%v, want the same move continuing", done, accepted)
	}

	if got := f.record(t, queued); got.Phase != workload.MigrationPhaseAccepted || got.SurgeInstance != nil {
		t.Errorf("queued record: got %+v want it still Accepted with no surge allocated", *got)
	}
	after := findInstanceStatusOnIRForFixture(t, f, 0)
	if after.Operation == nil || after.Operation.RequestUUID != before.Operation.RequestUUID {
		t.Errorf("source Operation: got %+v want the in-flight request still pinned", after.Operation)
	}
	var uuids []string
	for _, r := range f.records {
		uuids = append(uuids, r.RequestUUID)
	}
	if len(uuids) != 2 {
		t.Errorf("records: got %v want exactly the two requests (a re-delivery is deduped)", uuids)
	}
}

// TestMigrate_StatusConflictCostsAPassAndReDerives: the pair's status
// writes are read-modify-write, so a concurrent writer rejects one and
// the pass retries from a fresh read. The record survives that — it is
// not closed, the pin is not dropped — and the next pass re-evaluates the
// pair preconditions and gets where the rejected one was going.
//
// Cells: instance/Migrate/CreateSurge/api.conflict/before, instance/Migrate/SurgeTarget/api.conflict/before
func TestMigrate_StatusConflictCostsAPassAndReDerives(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-status-conflict"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, uuid, 1)
	before := *findInstanceStatusOnIRForFixture(t, f, 0)

	// One pass whose every row write is rejected as stale.
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	in := f.input(t)
	in.MutateInstance = func(_ context.Context, idx int32, _ func(*workload.InstanceStatus) bool) error {
		return apierrors.NewConflict(v1beta1.Resource("inferencereplicas"), f.irKey().Name,
			errors.New("the object has been modified; please apply your changes to the latest version"))
	}
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      rec.SourceInstance,
		FromNode:      rec.FromNode,
		Reason:        rec.Reason,
	}
	_, _, _ = Migrate(context.Background(), f.deps(), in, f.plan, rec.SourceInstance, uuid, req)

	if got := f.record(t, uuid); got.Phase.Terminal() {
		t.Fatalf("a conflict closed the record: %+v", *got)
	}
	after := findInstanceStatusOnIRForFixture(t, f, 0)
	if after == nil || after.Operation == nil || after.Operation.RequestUUID != before.Operation.RequestUUID {
		t.Fatalf("source: got %+v want the pin intact across the rejected write", after)
	}

	// The next pass, against a client that accepts writes again, re-derives
	// the same work and the move keeps going.
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("recovery pass: got done=%v accepted=%v, want the move still in flight", done, accepted)
	}
	if got := f.record(t, uuid); got.Phase.Terminal() {
		t.Errorf("record: got %+v want the move still driving after the retry", *got)
	}
}

// wedgedSurgePastGrace drives a migration to a surge pod parked in a
// terminal waiting reason past the stuck-pod grace — the evidence the
// record consumes as early failure — and returns the fixture clock.
func wedgedSurgePastGrace(t *testing.T, f *migFixture, uuid string) *clocktesting.FakeClock {
	t.Helper()
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	surgePods := driveToSurgePods(t, f, uuid, 1)
	if findInstanceStatusOnIRForFixture(t, f, 1) == nil {
		t.Fatalf("setup: the replacement row must exist before the close")
	}
	wedgePod(t, f, surgePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)
	return clk
}

// TestMigrate_SourcePodLossAroundTheWedgeClose: the close on the surge's
// own evidence restores the source through the record, and that write is
// preconditioned on both rows carrying the expected identity. A source
// pod that disappears as the write lands therefore rejects it and leaves
// it to be re-derived — the pair is never half-restored. Past the close
// the source is unpinned and the loss belongs to the ordinary repair, so
// the migration reads nothing from it either.
//
// Cells: instance/Migrate/CreateSurge/pod.deleted[source]/boundary@T-migrate-surge-terminal, instance/Migrate/CreateSurge/pod.deleted[source]/after@T-migrate-surge-terminal, instance/Migrate/SurgeTarget/pod.deleted[source]/boundary@T-migratetarget-terminal, instance/Migrate/SurgeTarget/pod.deleted[source]/after@T-migratetarget-terminal
func TestMigrate_SourcePodLossAroundTheWedgeClose(t *testing.T) {
	deleteSourcePods := func(t *testing.T, f *migFixture) {
		t.Helper()
		for _, pod := range migPodsForInstance(t, f, 0) {
			if err := f.c.Delete(context.Background(), pod); err != nil {
				t.Fatalf("delete the source pod: %v", err)
			}
		}
	}

	t.Run("lost as the restore lands, nothing is half-applied", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-source-lost-boundary"
		wedgedSurgePastGrace(t, f, uuid)
		deleteSourcePods(t, f)

		if done, accepted := f.pass(t, uuid); done || !accepted {
			t.Fatalf("boundary pass: got done=%v accepted=%v, want the write re-derived rather than applied", done, accepted)
		}
		if rec := f.record(t, uuid); rec.Phase.Terminal() {
			t.Fatalf("record: got %+v want it not closed over a pair it could not restore", *rec)
		}
		src := findInstanceStatusOnIRForFixture(t, f, 0)
		if src == nil || src.Operation == nil || src.Phase == v1beta1.OMENativeInstanceReady {
			t.Errorf("source: got %+v want the pin still held (a half-applied restore is the failure mode)", src)
		}
		if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation == nil {
			t.Errorf("replacement: got %+v want it still pinned alongside the source", surge)
		}
		assertNoRetryBlocks(t, f)
	})

	t.Run("lost after the close, the repair owns it", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-source-lost-after"
		wedgedSurgePastGrace(t, f, uuid)

		if done, accepted := f.pass(t, uuid); !done || !accepted {
			t.Fatalf("close pass: got done=%v accepted=%v, want the record closed", done, accepted)
		}
		closed := *f.record(t, uuid)
		assertSourceRestored(t, f, 0, uuid, findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision)
		restored := *findInstanceStatusOnIRForFixture(t, f, 0)
		migWedgeEvents(t, f)

		deleteSourcePods(t, f)
		if done, accepted := f.pass(t, uuid); !done || !accepted {
			t.Fatalf("after pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
		}
		if got := f.record(t, uuid); got.Message != closed.Message || !got.CompletedAt.Equal(closed.CompletedAt) {
			t.Errorf("the re-delivered loss rewrote the terminal record: %+v -> %+v", closed, *got)
		}
		if after := findInstanceStatusOnIRForFixture(t, f, 0); !equalitySafeOperation(&restored, after) {
			t.Errorf("source: got %+v want it left Ready and unpinned (%+v)", after, restored)
		}
		if events := migWedgeEvents(t, f); len(events) != 0 {
			t.Errorf("events: got %v want none; the migration reads nothing off an unpinned row", events)
		}
	})
}

// TestMigrate_MigrationDeadlineAroundTheWedgeClose: the record Deadline
// is the backstop the surge's own evidence pre-empts. Elapsing as the
// close lands changes nothing — the record is closed Failed either way
// and carries one terminal write — and elapsing afterwards reopens
// nothing, because expiry declines a record that is already terminal.
//
// Cells: instance/Migrate/CreateSurge/timer.migrationDeadline/boundary@T-migrate-surge-terminal, instance/Migrate/CreateSurge/timer.migrationDeadline/after@T-migrate-surge-terminal, instance/Migrate/SurgeTarget/timer.migrationDeadline/boundary@T-migratetarget-terminal, instance/Migrate/SurgeTarget/timer.migrationDeadline/after@T-migratetarget-terminal
func TestMigrate_MigrationDeadlineAroundTheWedgeClose(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-deadline-edges"
	clk := wedgedSurgePastGrace(t, f, uuid)

	// The boundary: the Deadline is already past on the observation the
	// terminal write is computed from.
	f.record(t, uuid).Deadline = metav1.NewTime(clk.Now().Add(-time.Second))
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("boundary pass: got done=%v accepted=%v, want the record closed", done, accepted)
	}
	closed := *f.record(t, uuid)
	if closed.Phase != workload.MigrationPhaseFailed || closed.CompletedAt == nil {
		t.Fatalf("record: got %+v want Failed with a close timestamp", closed)
	}
	source := findInstanceStatusOnIRForFixture(t, f, 0)
	if source == nil || source.Operation != nil {
		t.Fatalf("source: got %+v want the Migrate pin cleared", source)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge != nil && surge.Operation != nil &&
		surge.Operation.Type == v1beta1.InstanceOperationMigrate {
		t.Errorf("replacement: got %+v want the pin cleared so scale-down owns the index", surge.Operation)
	}
	assertNoRetryBlocks(t, f)
	restored := *source
	migWedgeEvents(t, f)

	// The after window: the same elapsed Deadline against a terminal
	// record.
	clk.Step(time.Hour)
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("after pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
	}
	got := f.record(t, uuid)
	if got.Message != closed.Message || !got.CompletedAt.Equal(closed.CompletedAt) {
		t.Errorf("expiry reopened a terminal record: %+v -> %+v", closed, *got)
	}
	if after := findInstanceStatusOnIRForFixture(t, f, 0); !equalitySafeOperation(&restored, after) {
		t.Errorf("source: got %+v want it left where the close put it (%+v)", after, restored)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("events: got %v want none; a terminal record announces nothing", events)
	}
}
