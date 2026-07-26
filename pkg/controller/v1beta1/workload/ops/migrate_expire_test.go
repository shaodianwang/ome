// Tests for the migration deadline consumer (ExpireMigrations): the
// record's Deadline — not the instance-op deadline — bounds a Manual
// migration; expiry closes the record, unpins the pair, restores the
// source phase from observation, and leaves the surge to the ordinary
// scale-down Delete pipeline. Reuses the entry-lifecycle harness
// (migFixture) with an injected fake clock.
package ops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// expire runs one ExpireMigrations pass the way the dispatcher would
// and returns how many records reached a terminal phase.
func (f *migFixture) expire(t *testing.T) int {
	t.Helper()
	n, err := ExpireMigrations(context.Background(), f.deps(), f.input(t), f.plan)
	if err != nil {
		t.Fatalf("ExpireMigrations: %v", err)
	}
	return n
}

// withFakeClock wires a fake clock into the fixture and returns it.
func (f *migFixture) withFakeClock() *clocktesting.FakeClock {
	clk := clocktesting.NewFakeClock(time.Now())
	f.clk = clk
	return clk
}

// mkMigRecordWithDeadline is mkMigRecord with the Deadline pinned to
// the fake clock (mkMigRecord uses wall time).
func mkMigRecordWithDeadline(uuid string, sourceIdx int32, fromNode string, deadline time.Time) workload.MigrationRecord {
	rec := mkMigRecord(uuid, sourceIdx, fromNode)
	rec.Deadline = metav1.NewTime(deadline)
	return rec
}

// TestMigrateExpiry_GangWedgeRegression is THE wedge regression,
// verbatim: a 3-pod gang migration whose surge never becomes
// schedulable (pods stay Pending) passes its Deadline. Pre-change,
// deadline.go stamped the HEALTHY serving source Phase=Failed with the
// Migrate op preserved and nothing ever consumed it — the migration
// "ran" forever and the capacity slot leaked. Post-change the record
// expires: Failed with the SurgePending blocker, both ops cleared,
// source restored to Ready from live observation, the unpinned surge
// torn down by the standard Delete pipeline, the capacity slot freed,
// and a NEW migration accepted and completed.
func TestMigrateExpiry_GangWedgeRegression(t *testing.T) {
	f := newGangMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-wedge"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-b", clk.Now().Add(30*time.Minute)),
	}

	// Pass 1: allocation + pair stamps (gang PodGroup requeue);
	// pass 2: surge gang pods render; pass 3: still waiting. NO react —
	// the surge gang never becomes ready (unschedulable).
	for i := 0; i < 3; i++ {
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want in-flight", i+1, done, accepted)
		}
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseSurgePending {
		t.Fatalf("wedge setup: record phase = %s, want SurgePending", rec.Phase)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Fatalf("wedge setup: source must be pinned Migrating; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceCreating || surge.Operation == nil {
		t.Fatalf("wedge setup: surge must be pinned Creating; got %+v", surge)
	}

	// Before the deadline: never a premature expiry.
	if n := f.expire(t); n != 0 {
		t.Fatalf("expiry before deadline: got %d, want 0", n)
	}

	clk.Step(30*time.Minute + time.Second)
	if n := f.expire(t); n != 1 {
		t.Fatalf("expiry past deadline: got %d, want 1", n)
	}

	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("record must close Failed with CompletedAt; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, "SurgePending") || !strings.Contains(rec.Message, "surge pods never became ready") {
		t.Errorf("record Message must name the SurgePending blocker; got %q", rec.Message)
	}
	// Source: op cleared, phase restored from observation (its 3 pods
	// are live + runtime-ready — it served throughout the wedge).
	src = findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must be restored to Ready with the op cleared; got %+v", src)
	}
	// Surge: op cleared but the status slot KEPT (unpinned extra).
	surge = findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceCreating || surge.Operation != nil {
		t.Fatalf("surge must keep its slot with the op cleared; got %+v", surge)
	}
	// Ledger mirrors the terminal Failed row.
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger must mirror the terminal Failed row")
	}
	// Idempotent: the terminal record is never re-expired.
	if n := f.expire(t); n != 0 {
		t.Fatalf("re-expiry of a terminal record: got %d, want 0", n)
	}

	// The unpinned surge is an ordinary extra for the dispatcher's
	// step-1 scale-down — tear it down through the standard Delete
	// pipeline (no bespoke teardown).
	deleted := false
	for i := 0; i < 8 && !deleted; i++ {
		legacyResetExpectations(t)
		done, derr := Delete(context.Background(), f.deps(), f.input(t), f.plan, 1)
		if derr != nil {
			t.Fatalf("Delete surge extra: %v", derr)
		}
		deleted = done
		f.react(t)
	}
	if !deleted {
		t.Fatalf("surge Delete pipeline did not converge")
	}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "1" {
			t.Errorf("surge pod %s must be torn down", pod.Name)
		}
	}
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s != nil {
		t.Errorf("surge InstanceStatus must be removed by the Delete pipeline; got %+v", s)
	}

	// Capacity slot freed structurally (terminal records don't count).
	if ok, reason := audit.ValidateCapacity(f.records, "mig-retry", clk.Now()); !ok {
		t.Fatalf("capacity must be free after expiry: %s", reason)
	}

	// A fresh migration on the restored source is accepted and
	// completes end-to-end.
	f.records = append(f.records,
		mkMigRecordWithDeadline("mig-retry", 0, "node-b", clk.Now().Add(30*time.Minute)))
	done, accepted := f.pass(t, "mig-retry")
	if done || !accepted {
		t.Fatalf("retry migration must be accepted: done=%v accepted=%v record=%+v",
			done, accepted, *f.record(t, "mig-retry"))
	}
	f.react(t)
	f.drive(t, "mig-retry", 12)
	if rec := f.record(t, "mig-retry"); rec.Phase != workload.MigrationPhaseCompleted {
		t.Fatalf("retry migration must complete; got %+v", *rec)
	}
}

// TestMigrateExpiry_NoPrematureExpiry_CompletesBeforeDeadline is the
// companion: an in-flight migration whose surge is merely slow is left
// alone right up to the Deadline and completes normally once the surge
// comes up.
func TestMigrateExpiry_NoPrematureExpiry_CompletesBeforeDeadline(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-slow"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}

	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}

	// One second shy of the deadline: no expiry.
	clk.Step(30*time.Minute - time.Second)
	if n := f.expire(t); n != 0 {
		t.Fatalf("expiry before deadline: got %d, want 0", n)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("record must stay non-terminal before deadline; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Operation == nil {
		t.Fatalf("in-flight pair must stay pinned before deadline; got %+v", src)
	}

	// The surge comes up — the migration completes normally.
	f.react(t)
	f.drive(t, uuid, 10)
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseCompleted {
		t.Fatalf("slow migration must complete; got %+v", *rec)
	}
}

// TestMigrateExpiry_SourcePodsDead_SourceFailed pins the
// observation-not-stamp rule for the unhealthy direction: when the
// source pods are NOT runtime-ready at expiry, the source goes Failed
// (with a LastFailure) so its own escalation machinery takes over —
// never a blind Ready restore.
func TestMigrateExpiry_SourcePodsDead_SourceFailed(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-deadsource"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}

	// The source pod dies while the surge is wedged.
	srcPod := &corev1.Pod{}
	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	for i := range srcPod.Status.Conditions {
		if srcPod.Status.Conditions[i].Type == corev1.ContainersReady {
			srcPod.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}
	if err := f.c.Status().Update(context.Background(), srcPod); err != nil {
		t.Fatalf("mark source pod unready: %v", err)
	}
	// A drain key that landed before the pod died: a Failed-observed
	// source must be un-drained too, so a later recovery serves.
	if err := podreadiness.MarkPodNotServing(context.Background(), f.c, srcPod, podreadiness.WriterMigrateSourceDrain, uuid); err != nil {
		t.Fatalf("seed drain key: %v", err)
	}

	clk.Step(31 * time.Minute)
	if n := f.expire(t); n != 1 {
		t.Fatalf("expiry: got %d, want 1", n)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record must close Failed; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceFailed || src.Operation != nil {
		t.Fatalf("dead source must restore to Failed with the op cleared; got %+v", src)
	}
	if src.LastFailure == nil || !strings.Contains(src.LastFailure.Message, "source pods unhealthy") {
		t.Errorf("source LastFailure must name the unhealthy observation; got %+v", src.LastFailure)
	}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("re-get source pod: %v", err)
	}
	if podreadiness.ContainsNotReadyKey(srcPod, podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}) {
		t.Errorf("expiry must remove the drain key from a Failed-observed source too; conditions=%+v", srcPod.Status.Conditions)
	}
}

// TestMigrateExpiry_CrashWindow_DrainKeysAppliedAtSurgeReady_UnDrained
// pins the SurgeReady→Draining crash window: the drive applied the
// source-drain keys but died before the record's Draining advance, so
// the record still reads SurgeReady while every source pod carries
// serving=False. Expiry must un-drain regardless — the fix keys on
// LIVE POD STATE, not the record's phase (a phase-gated un-drain would
// miss exactly this window).
func TestMigrateExpiry_CrashWindow_DrainKeysAppliedAtSurgeReady_UnDrained(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-crashdrain"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	// The real machinery applies the drain keys (drive to Draining),
	// then the record rewinds to SurgeReady — reconstructing the crash
	// window between the MarkPodNotServing loop and the record write.
	driveToDrainingDrainIncomplete(t, f, uuid)
	f.record(t, uuid).Phase = workload.MigrationPhaseSurgeReady

	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	if !podreadiness.ContainsNotReadyKey(srcPod, drainKey) {
		t.Fatalf("setup: drain key must be applied; conditions=%+v", srcPod.Status.Conditions)
	}

	clk.Step(31 * time.Minute)
	if n := f.expire(t); n != 1 {
		t.Fatalf("expiry: got %d, want 1", n)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, "SurgeReady") {
		t.Fatalf("record must close Failed with the SurgeReady blocker; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must restore to Ready; got %+v", src)
	}
	srcPod = &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("re-get source pod: %v", err)
	}
	if podreadiness.ContainsNotReadyKey(srcPod, drainKey) || !podreadiness.IsServing(srcPod) {
		t.Fatalf("mid-loop-crash source must be un-drained despite the record never reaching Draining; conditions=%+v",
			srcPod.Status.Conditions)
	}
}

// TestMigrateExpiry_SurgeReadyButNotInRotation_Expires pins the
// parked-forever strand: a Draining record whose source is fully
// drained but whose surge fell out of rotation. The drive blocks at
// its rotation gate every pass; pre-fix migrationTailReady checked
// only surge ContainersReady + source drained — weaker than the
// drive's gates — so expiry ALSO deferred every pass and the record
// sat non-terminal past its deadline forever. tailReady now shares
// the drive's exact gates (surgeTailGatesPassed), so this state
// expires.
func TestMigrateExpiry_SurgeReadyButNotInRotation_Expires(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-parked"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	driveToDrainingDrainIncomplete(t, f, uuid)
	// Settle the drain: the source endpoint leaves rotation.
	f.react(t)

	// The surge falls out of rotation (its endpoint goes unready —
	// e.g. slice reprogramming) while the record is Draining.
	surgePodName := query.PodName(f.isvc.Name, f.component, 1, "default", 0)
	surgePod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: surgePodName}, surgePod); err != nil {
		t.Fatalf("get surge pod: %v", err)
	}
	surgeSvc := query.PerRevisionServiceName(f.isvc.Name, f.component, surgePod.Labels[query.LabelRevisionHash])
	slice := &discoveryv1.EndpointSlice{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: surgeSvc + "-slice"}, slice); err != nil {
		t.Fatalf("get surge slice: %v", err)
	}
	notReady := false
	for i := range slice.Endpoints {
		slice.Endpoints[i].Conditions.Ready = &notReady
	}
	if err := f.c.Update(context.Background(), slice); err != nil {
		t.Fatalf("knock surge out of rotation: %v", err)
	}

	clk.Step(31 * time.Minute)

	// The drive cannot finish the tail — it blocks at its rotation gate.
	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("drive must block at the rotation gate: done=%v accepted=%v", done, accepted)
	}

	// Pre-fix: n=0 here, every pass, forever. Post-fix the record
	// expires because tailReady fails the same rotation gate.
	if n := f.expire(t); n != 1 {
		t.Fatalf("ready-but-not-in-rotation surge past deadline must expire: got %d, want 0-fix-regression", n)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("record must close Failed; got %+v", *rec)
	}
	// The drained source is un-drained and restored to Ready.
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must restore to Ready; got %+v", src)
	}
	srcPod := &corev1.Pod{}
	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	if !podreadiness.IsServing(srcPod) {
		t.Fatalf("expired source must be serving again; conditions=%+v", srcPod.Status.Conditions)
	}
}

// TestMigrateExpiry_MirrorPersistFails_RetriesUntilTerminal pins the
// hard-mirror rule (the resurrection chain's first link): when the
// terminal ledger mirror cannot persist, the expiry pass ERRORS BEFORE
// the record's terminal write — the record stays non-terminal and the
// next pass re-runs the idempotent steps until the mirror lands. A
// best-effort mirror (pre-fix) closed the record terminal with no
// ledger row; after the 1h status trim, the upgrade import would
// re-synthesize the UUID from its Started row as fresh Accepted work.
func TestMigrateExpiry_MirrorPersistFails_RetriesUntilTerminal(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-mirrorfail"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	// Pass 1 allocates + persists the Started ledger row (CM exists).
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}

	// Every ConfigMap write conflicts while failMirror holds.
	failMirror := true
	f.c = interceptor.NewClient(f.c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok && failMirror {
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("simulated conflict"))
			}
			return c.Update(ctx, obj, opts...)
		},
	})

	clk.Step(31 * time.Minute)

	// Expiry attempt with the failing mirror: hard error, record left
	// non-terminal, no terminal ledger row, no terminal record write.
	n, err := ExpireMigrations(context.Background(), f.deps(), f.input(t), f.plan)
	if err == nil {
		t.Fatalf("mirror persist failure must abort the expiry pass with an error")
	}
	if n != 0 {
		t.Fatalf("no record may go terminal while the mirror cannot land; got %d", n)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("record must stay non-terminal for retry; got %+v", *rec)
	}
	ledger, lerr := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if lerr != nil {
		t.Fatalf("load ledger: %v", lerr)
	}
	if ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("no terminal ledger row may exist while persists fail")
	}

	// Heal: the next pass re-runs the idempotent steps and closes.
	failMirror = false
	if n := f.expire(t); n != 1 {
		t.Fatalf("healed expiry: got %d, want 1", n)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("record must close Failed after the mirror lands; got %+v", *rec)
	}
	ledger, lerr = audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if lerr != nil {
		t.Fatalf("re-load ledger: %v", lerr)
	}
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("terminal ledger row must exist once the expiry closes")
	}
}

// driveToDrainingDrainIncomplete walks the fixture's migration to the
// Draining phase and stops BEFORE the environment reacts to the drain
// start — the source's routed endpoint still shows Ready, so the drain
// is genuinely incomplete.
func driveToDrainingDrainIncomplete(t *testing.T, f *migFixture, uuid string) {
	t.Helper()
	for i := 0; i < 8; i++ {
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want in-flight", i+1, done, accepted)
		}
		if f.record(t, uuid).Phase == workload.MigrationPhaseDraining {
			return
		}
		f.react(t)
	}
	t.Fatalf("fixture never reached Draining; record=%+v", *f.record(t, uuid))
}

// TestMigrateExpiry_DrainingIncomplete_Expired pins the Draining
// precedence rule's expire side: past the Deadline with the source
// drain NOT complete, the record expires — the source is un-drained
// (serving key removed, back in rotation) as well as restored to
// Ready, and the surge, which WAS serving in rotation, is torn down
// through the drain-gated Delete pipeline like any extra.
func TestMigrateExpiry_DrainingIncomplete_Expired(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-drainstuck"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	driveToDrainingDrainIncomplete(t, f, uuid)

	// The drive's real MarkPodNotServing loop drained the source: its
	// pod carries the {Migrate-source-drain, uuid} key, serving=False.
	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	if podreadiness.IsServing(srcPod) || !podreadiness.ContainsNotReadyKey(srcPod, drainKey) {
		t.Fatalf("setup: drive must have drained the source pod; serving=%v conditions=%+v",
			podreadiness.IsServing(srcPod), srcPod.Status.Conditions)
	}

	clk.Step(31 * time.Minute)
	if n := f.expire(t); n != 1 {
		t.Fatalf("Draining with incomplete drain must expire: got %d, want 1", n)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, "drain incomplete") {
		t.Fatalf("record must close Failed with the drain blocker; got %+v", *rec)
	}
	// Source pods are still runtime-ready (only their serving gate was
	// pulled for the drain) — restored to Ready.
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must restore to Ready; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Operation != nil {
		t.Fatalf("surge must be unpinned; got %+v", surge)
	}
	// THE STRANDED-SOURCE REGRESSION: expiry must undo the drive's
	// drain. Pre-fix nothing removed the serving key except source-pod
	// deletion — which an expired-but-kept source never gets — so the
	// "restored Ready" source stayed serving=False on every pod:
	// permanently out of the routed Service and invisible to the
	// availability counters.
	srcPod = &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("re-get source pod: %v", err)
	}
	if podreadiness.ContainsNotReadyKey(srcPod, drainKey) {
		t.Fatalf("expiry must remove the drive's source-drain key; conditions=%+v", srcPod.Status.Conditions)
	}
	if !podreadiness.IsServing(srcPod) {
		t.Fatalf("un-drained source must be serving again; conditions=%+v", srcPod.Status.Conditions)
	}
	// Once the endpoint controller settles, the source is back in
	// rotation in its routed per-revision Service.
	f.react(t)
	srcSvc := query.PerRevisionServiceName(f.isvc.Name, f.component, testRevisionHashLegacy)
	inRotation, rerr := drain.IsPodInRotation(context.Background(), f.c, f.isvc.Namespace, srcSvc, srcPod)
	if rerr != nil {
		t.Fatalf("check source rotation: %v", rerr)
	}
	if !inRotation {
		t.Fatalf("un-drained source must re-enter rotation")
	}

	// The serving surge drains properly through the standard Delete.
	deleted := false
	for i := 0; i < 8 && !deleted; i++ {
		legacyResetExpectations(t)
		done, derr := Delete(context.Background(), f.deps(), f.input(t), f.plan, 1)
		if derr != nil {
			t.Fatalf("Delete serving surge: %v", derr)
		}
		deleted = done
		f.react(t)
	}
	if !deleted {
		t.Fatalf("serving-surge Delete pipeline did not converge")
	}
	surgePod := &corev1.Pod{}
	surgePodName := query.PodName(f.isvc.Name, f.component, 1, "default", 0)
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: surgePodName}, surgePod); !apierrors.IsNotFound(err) {
		t.Errorf("surge pod must be gone; get returned %v", err)
	}
}

// TestMigrateExpiry_DrainingComplete_DrivenToCompleted pins the
// Draining precedence rule's drive side: past the Deadline but with
// the source fully drained AND the surge fully runtime-ready,
// completion is one idempotent tail away — the record is NOT expired
// and the drive pass finishes it Completed.
func TestMigrateExpiry_DrainingComplete_DrivenToCompleted(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-draindone"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	driveToDrainingDrainIncomplete(t, f, uuid)
	// The environment settles the drain: the source endpoint leaves
	// rotation.
	f.react(t)

	clk.Step(31 * time.Minute)
	if n := f.expire(t); n != 0 {
		t.Fatalf("drain-complete Draining must be driven, not expired: got %d expiries", n)
	}
	f.drive(t, uuid, 10)
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("drain-complete Draining must complete; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("source InstanceStatus must be removed on completion; got %+v", src)
	}
}

// TestMigrateExpiry_CompletedTailCrash_ClosedCompleted pins the
// completed-tail crash edge: the completion tail already promoted the
// surge (Phase=Ready, op cleared) and removed the source status, but
// the process died before the record's terminal write. The migration
// SUCCEEDED — expiry must close the record Completed, never Failed,
// and never touch the promoted surge.
func TestMigrateExpiry_CompletedTailCrash_ClosedCompleted(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-tailcrash"

	// Reconstruct the crash window directly: source status gone, surge
	// promoted Ready at the source's revision.
	ir := f.getIR(t)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 1, Incarnation: 1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: "llama-70b-engine-abc123",
	}}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed crash-window IR: %v", err)
	}
	rec := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(-time.Minute))
	surgeIdx := int32(1)
	rec.SurgeInstance = &surgeIdx
	rec.Phase = workload.MigrationPhaseDraining
	f.records = []workload.MigrationRecord{rec}

	if n := f.expire(t); n != 1 {
		t.Fatalf("expiry: got %d, want 1", n)
	}
	got := f.record(t, uuid)
	if got.Phase != workload.MigrationPhaseCompleted || got.CompletedAt == nil {
		t.Fatalf("crash-window record must close COMPLETED; got %+v", *got)
	}
	if !strings.Contains(got.Message, "migrated to instance=1") {
		t.Errorf("record Message must name the promoted surge; got %q", got.Message)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady ||
		surge.RunningRevision != "llama-70b-engine-abc123" || surge.Operation != nil {
		t.Fatalf("promoted surge must be untouched; got %+v", surge)
	}
}

// TestMigrateExpiry_AcceptedNeverAllocated pins the queued-record
// expiry: an Accepted record that never allocated a surge (here
// because the source never left an in-flight Update) expires as a
// record-only transition — no instance ops exist to clear, no surge to
// tear down, and the source is left entirely alone.
func TestMigrateExpiry_AcceptedNeverAllocated(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-queued"

	// The source is mid-Update — the reason the record never allocated.
	ir := f.getIR(t)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationType(workload.InstanceOperationUpdate),
		Step: "Surge",
	}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Updating source: %v", err)
	}
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(-time.Minute)),
	}

	if n := f.expire(t); n != 1 {
		t.Fatalf("expiry: got %d, want 1", n)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("queued record must close Failed; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, "Accepted") || !strings.Contains(rec.Message, "surge never allocated") {
		t.Errorf("record Message must name the Accepted blocker; got %q", rec.Message)
	}
	// The source (and its in-flight Update) is untouched — pre-
	// allocation, the migration never stamped anything.
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceUpdating ||
		src.Operation == nil || src.Operation.Type != v1beta1.InstanceOperationType(workload.InstanceOperationUpdate) {
		t.Fatalf("source must be untouched by an Accepted-phase expiry; got %+v", src)
	}
	// No surge slot was ever created.
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s != nil {
		t.Errorf("no surge status may exist; got %+v", s)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger must mirror the terminal Failed row")
	}
}
