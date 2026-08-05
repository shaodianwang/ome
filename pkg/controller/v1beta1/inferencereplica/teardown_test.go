package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// terminatingIR returns baselineIR stamped Terminating with the
// teardown finalizer, DeletionTimestamp at dt.
func terminatingIR(name, namespace string, replicas int32, dt time.Time) *v1beta1.InferenceReplica {
	ir := baselineIR(name, namespace, replicas)
	ts := metav1.NewTime(dt)
	ir.DeletionTimestamp = &ts
	ir.Finalizers = []string{TeardownFinalizer}
	return ir
}

// withLifecycleConfig wires a fake clientset + zero-TTL config cache
// serving the given lifecycle JSON so the teardown/force-delete config
// resolvers see it.
func withLifecycleConfig(r *Reconciler, lifecycleJSON string) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data:       map[string]string{"lifecycle": lifecycleJSON},
	}
	r.Clientset = kubefake.NewSimpleClientset(cm)
	r.ConfigCache = controllerconfig.NewConfigCache(0)
}

// drainEvents empties a FakeRecorder's channel.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func eventsContaining(events []string, substr string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, substr) {
			out = append(out, e)
		}
	}
	return out
}

// TestReconcile_AddsTeardownFinalizer pins the arm step: the first
// reconcile of a fresh (non-Terminating) IR must add the teardown
// finalizer so a later delete routes through the reconciled teardown
// path.
func TestReconcile_AddsTeardownFinalizer(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	r, c := newReconciler(t, ir)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), got); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(got, TeardownFinalizer) {
		t.Errorf("first reconcile must add finalizer %s; got %v", TeardownFinalizer, got.Finalizers)
	}
}

// TestTeardown_NoFinalizer_LeavesPodsToGC pins the hand-off contract: a
// Terminating IR WITHOUT the teardown finalizer is entirely GC's — no
// Delete op may fire, its pods stay untouched.
func TestTeardown_NoFinalizer_LeavesPodsToGC(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	now := metav1.Now()
	ir.DeletionTimestamp = &now
	ir.Finalizers = []string{"keep-for-test"}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	r, c := newReconciler(t, ir, pod0)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("finalizer-less Terminating IR must be a clean no-op, got %+v", result)
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod0), got); err != nil {
		t.Fatalf("pod must be untouched (GC owns it): %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Errorf("pod must not be deleted by the controller when the finalizer is absent")
	}
}

// TestTeardown_DispatchesDeletes_RetainsFinalizer covers the in-flight
// shape (with the parent ISVC already gone — no ISVC object is seeded,
// so the parent resolve is NotFound → nil): teardown dispatches the
// Delete pipeline against the observed Instance, requeues at the
// Delete cadence, and keeps the finalizer while the pod survives
// Terminating. With no lifecycle.teardown.deadline configured the
// strict hold warns with TeardownBlocked.
func TestTeardown_DispatchesDeletes_RetainsFinalizer(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
	}
	pod0 := podForIR(ir, 0, "default", 0, false, false)
	// Survive the graceful delete as Terminating, like a real pod with
	// a kubelet still shutting containers down.
	pod0.Finalizers = []string{"test.ome.io/hold"}
	r, c := newReconciler(t, ir, pod0)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != workloadops.DeleteRequeueInterval {
		t.Errorf("teardown in flight must requeue at DeleteRequeueInterval, got %+v", result)
	}

	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), gotIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(gotIR, TeardownFinalizer) {
		t.Errorf("finalizer must be retained while pods survive")
	}
	if len(gotIR.Status.InstanceStatuses) != 1 || gotIR.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceDeleting {
		t.Errorf("instance must be stamped Deleting by the dispatched Delete op; got %+v", gotIR.Status.InstanceStatuses)
	}

	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod0), gotPod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if gotPod.DeletionTimestamp == nil {
		t.Errorf("teardown must graceful-delete the pod")
	}

	events := drainEvents(rec)
	if blocked := eventsContaining(events, ReasonTeardownBlocked); len(blocked) != 1 {
		t.Errorf("strict hold (nil deadline) must warn TeardownBlocked once per pass; events=%v", events)
	} else {
		msg := blocked[0]
		for _, want := range []string{pod0.Name, TeardownFinalizer, "manual escape", "no lifecycle.teardown.deadline configured"} {
			if !strings.Contains(msg, want) {
				t.Errorf("TeardownBlocked message must contain %q; got %q", want, msg)
			}
		}
	}
}

// TestTeardown_InvalidDeadlineConfig_WarnsInvalid pins the strict-hold
// diagnostics variant for configured-but-invalid config: a fleet that
// wrote "30" (no unit) gets the strict hold, but the TeardownBlocked
// message must say the deadline is invalid — and why — instead of
// claiming nothing is configured.
func TestTeardown_InvalidDeadlineConfig_WarnsInvalid(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = nil
	orphan := podForIR(ir, 4, "default", 0, false, false)
	r, c := newReconciler(t, ir, orphan)
	withLifecycleConfig(r, `{"teardown":{"deadline":"30"}}`)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), gotIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(gotIR, TeardownFinalizer) {
		t.Errorf("invalid deadline config must strict-hold (finalizer retained), not release")
	}
	blocked := eventsContaining(drainEvents(rec), ReasonTeardownBlocked)
	if len(blocked) != 1 {
		t.Fatalf("expected one TeardownBlocked warning, got %v", blocked)
	}
	msg := blocked[0]
	for _, want := range []string{"configured but invalid", "missing unit"} {
		if !strings.Contains(msg, want) {
			t.Errorf("invalid-config TeardownBlocked message must contain %q; got %q", want, msg)
		}
	}
	if strings.Contains(msg, "no lifecycle.teardown.deadline configured") {
		t.Errorf("invalid-config TeardownBlocked message must not claim the deadline is unconfigured; got %q", msg)
	}
}

// ledgerCMForOwner materializes a migration audit ConfigMap for the
// named ledger owner, the shape audit.PersistLedgerForOwner writes.
func ledgerCMForOwner(t *testing.T, ownerName, namespace string, ledger *audit.Ledger) *corev1.ConfigMap {
	t.Helper()
	raw, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ownerName + audit.ConfigMapNameSuffix, Namespace: namespace},
		Data:       map[string]string{audit.LedgerKey: string(raw)},
	}
}

// startedMigrationEntry is the ledger row an accepted-but-unfinished
// migration leaves behind: Phase=Started, source 0 → surge 7.
func startedMigrationEntry() audit.Entry {
	return audit.Entry{
		RequestUUID:    "mig-uuid-1",
		Component:      string(v1beta1.EngineComponent),
		SourceInstance: 0,
		SurgeInstance:  7,
		Phase:          audit.PhaseStarted,
		Reason:         "fragmentation",
		FromNode:       "node-a",
		StartedAt:      "2026-07-23T00:00:00Z",
	}
}

// TestTeardown_InFlightMigration_CompletesAndClosesLedger is the B1
// end-to-end: an IR deleted mid-migration (Phase=Migrating source +
// Operation.Type=Migrate surge, both with live pods) must still tear
// down — both pods Delete-dispatched, completion reached — and the
// parent-owned ledger's Started entry must be closed terminal so a
// projector-recreated fresh IR cannot resume it as a phantom
// migration (asserted through the real resume-path reader).
func TestTeardown_InFlightMigration_CompletesAndClosesLedger(t *testing.T) {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama", Namespace: "prod", UID: types.UID("llama-isvc-uid"),
	}}
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating},
		{Index: 7, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationMigrate}},
	}
	sourcePod := podForIR(ir, 0, "default", 0, false, false)
	surgePod := podForIR(ir, 7, "default", 0, false, false)
	ledgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})
	r, c := newReconciler(t, ir, parent, sourcePod, surgePod, ledgerCM)

	// Drive to completion: the mid-migration pair must not wedge the
	// teardown, so a handful of passes suffices.
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}}
	done := false
	for i := 0; i < 5 && !done; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("Reconcile pass %d: %v", i, err)
		}
		done = apierrors.IsNotFound(c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}))
	}
	if !done {
		t.Fatalf("teardown over an in-flight migration must complete (IR gone); it is still present")
	}

	// The dangling Started entry is terminal with the teardown outcome.
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("expected the single closed entry, got %+v", ledger.Entries)
	}
	e := ledger.Entries[0]
	if e.Phase != audit.PhaseFailed || e.Outcome != audit.OutcomeOwnerTornDown || e.CompletedAt == "" {
		t.Errorf("Started entry must be closed (Phase=Failed, Outcome=%s, CompletedAt set); got %+v", audit.OutcomeOwnerTornDown, e)
	}

	// Fresh-IR-resume regression: the upgrade import a projector-
	// recreated IR runs against the surviving parent's ledger must NOT
	// synthesize work from the closed row.
	if imports := importableLedgerMigrations(ledger, string(v1beta1.EngineComponent), metav1.Now(), time.Minute); len(imports) != 0 {
		t.Errorf("closed entry must not import as migration work on a fresh IR; got %+v", imports)
	}
}

// TestTeardown_DeadlineRelease_ClosesLedger pins the same dangling-
// entry close on the deadline-release path: survivors past the
// configured deadline release the finalizer to background GC, and the
// Started ledger entry must be closed on the way out — the parent (and
// its ledger) outlive the IR here too.
func TestTeardown_DeadlineRelease_ClosesLedger(t *testing.T) {
	base := time.Now()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama", Namespace: "prod", UID: types.UID("llama-isvc-uid"),
	}}
	ir := terminatingIR("llama-engine", "prod", 1, base.Add(-time.Hour))
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating},
	}
	survivor := podForIR(ir, 0, "default", 0, false, false)
	survivor.Finalizers = []string{"test.ome.io/hold"}
	ledgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})
	r, c := newReconciler(t, ir, parent, survivor, ledgerCM)
	r.Clock = clocktesting.NewFakeClock(base)
	withLifecycleConfig(r, `{"teardown":{"deadline":"30m"}}`)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer must be released past the deadline; get returned %v", err)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].Phase != audit.PhaseFailed || ledger.Entries[0].Outcome != audit.OutcomeOwnerTornDown {
		t.Errorf("deadline release must close the dangling Started entry; got %+v", ledger.Entries)
	}
}

// TestTeardown_LedgerClose_RetriesOnConflict pins the conflict
// tolerance of the dangling-entry close: the ledger ConfigMap is
// shared across the parent's components, so a concurrent writer
// bumping its resourceVersion must not silently drop the close. One
// injected Conflict on the ledger update → the retry re-loads fresh,
// lands the close, and the finalizer still lifts.
func TestTeardown_LedgerClose_RetriesOnConflict(t *testing.T) {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama", Namespace: "prod", UID: types.UID("llama-isvc-uid"),
	}}
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = nil
	ledgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})
	ledgerName := ledgerCM.Name

	conflicted := false
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, parent, ledgerCM).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == ledgerName && !conflicted {
					conflicted = true
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("simulated concurrent ledger write"))
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &Reconciler{Client: c, Log: ctrl.Log.WithName("test"), Expectations: workload.NewExpectations()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !conflicted {
		t.Fatalf("test wiring: no ledger ConfigMap update was intercepted")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("finalizer must be removed despite the ledger conflict; get returned %v", err)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].Phase != audit.PhaseFailed || ledger.Entries[0].Outcome != audit.OutcomeOwnerTornDown {
		t.Errorf("close must land after the conflict retry; got %+v", ledger.Entries)
	}
}

// TestTeardown_LedgerClose_RebasesOverPeerComponentWrite is the
// lost-update regression: under a live parent, a PEER component's
// Migrate machinery can append a fresh Started entry to the shared
// ledger between our load and persist. Simulated as a first-Update
// Conflict whose stored state now also holds the peer entry — the
// retry must re-load fresh and rebase, closing OUR component's entry
// while the peer's Started entry survives (a whole-blob overwrite from
// the stale load would clobber it).
func TestTeardown_LedgerClose_RebasesOverPeerComponentWrite(t *testing.T) {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama", Namespace: "prod", UID: types.UID("llama-isvc-uid"),
	}}
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = nil
	ledgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})
	ledgerName := ledgerCM.Name
	peerEntry := audit.Entry{
		RequestUUID:    "mig-uuid-peer",
		Component:      string(v1beta1.DecoderComponent),
		SourceInstance: 1,
		SurgeInstance:  9,
		Phase:          audit.PhaseStarted,
		Reason:         "fragmentation",
		StartedAt:      "2026-07-23T00:00:01Z",
	}

	injected := false
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, parent, ledgerCM).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				cm, ok := obj.(*corev1.ConfigMap)
				if !ok || cm.Name != ledgerName || injected {
					return cl.Update(ctx, obj, opts...)
				}
				// Peer write lands first: append its Started entry to the
				// STORED object (bumping resourceVersion), then reject the
				// caller's stale-blob update with the Conflict the real
				// apiserver would return.
				injected = true
				stored := &corev1.ConfigMap{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(cm), stored); err != nil {
					return err
				}
				var l audit.Ledger
				if err := json.Unmarshal([]byte(stored.Data[audit.LedgerKey]), &l); err != nil {
					return err
				}
				l.Entries = append(l.Entries, peerEntry)
				raw, err := json.Marshal(&l)
				if err != nil {
					return err
				}
				stored.Data[audit.LedgerKey] = string(raw)
				if err := cl.Update(ctx, stored); err != nil {
					return err
				}
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("peer component wrote the ledger first"))
			},
		}).
		Build()
	r := &Reconciler{Client: c, Log: ctrl.Log.WithName("test"), Expectations: workload.NewExpectations()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !injected {
		t.Fatalf("test wiring: no ledger ConfigMap update was intercepted")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("finalizer must be removed despite the ledger conflict; get returned %v", err)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 2 {
		t.Fatalf("expected our closed entry plus the surviving peer entry, got %+v", ledger.Entries)
	}
	byUUID := map[string]audit.Entry{}
	for _, e := range ledger.Entries {
		byUUID[e.RequestUUID] = e
	}
	if ours := byUUID["mig-uuid-1"]; ours.Phase != audit.PhaseFailed || ours.Outcome != audit.OutcomeOwnerTornDown {
		t.Errorf("our component's entry must be closed after the rebase; got %+v", ours)
	}
	if peer := byUUID["mig-uuid-peer"]; peer.Phase != audit.PhaseStarted {
		t.Errorf("peer component's concurrent Started entry must survive the close (lost update); got %+v", peer)
	}
}

// TestTeardown_StaleParentNotFound_ClosesParentLedger pins the
// teardown parent resolve to the live reader: the cache can serve a
// stale NotFound for a still-live parent, which would aim the close at
// the empty IR-owned ledger and leave the parent-owned Started entry
// to resume as a phantom migration. The live view has the parent → the
// close must land on the parent-named ledger ConfigMap.
func TestTeardown_StaleParentNotFound_ClosesParentLedger(t *testing.T) {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama", Namespace: "prod", UID: types.UID("llama-isvc-uid"),
	}}
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = nil
	// The fake cached client and fake live reader hold separate stores:
	// seed the parent-owned ledger in both (loads go through the live
	// reader; the persist read-modify-writes through the cached client).
	cachedLedgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})
	liveLedgerCM := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{Entries: []audit.Entry{startedMigrationEntry()}})

	// Cache: IR + ledger, parent MISSING (stale NotFound).
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, cachedLedgerCM).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	// Live: parent exists.
	reader := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(parent, liveLedgerCM).
		Build()
	r := &Reconciler{Client: c, APIReader: reader, Log: ctrl.Log.WithName("test"), Expectations: workload.NewExpectations()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("teardown must complete; get returned %v", err)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load parent-owned ledger: %v", err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].Phase != audit.PhaseFailed || ledger.Entries[0].Outcome != audit.OutcomeOwnerTornDown {
		t.Errorf("close must land on the parent-named ledger despite the stale cached NotFound; got %+v", ledger.Entries)
	}
}

// TestReconcile_FinalizerAddDeletionRace_NoError pins the noise fix for
// the deterministic add-vs-delete race: the cached IR shows no
// DeletionTimestamp, the apiserver copy is already Terminating, and the
// finalizer add is rejected ("no new finalizers can be added if the
// object is being deleted"). The reconciler re-reads live and, seeing
// Terminating, requeues without surfacing an error — the next pass
// routes to teardown. A rejection with a NON-Terminating live view is a
// real failure and must still surface.
func TestReconcile_FinalizerAddDeletionRace_NoError(t *testing.T) {
	reject := apierrors.NewInvalid(
		v1beta1.SchemeGroupVersion.WithKind("InferenceReplica").GroupKind(),
		"llama-engine",
		field.ErrorList{field.Forbidden(field.NewPath("metadata", "finalizers"),
			`no new finalizers can be added if the object is being deleted, found new finalizers ["ome.io/ir-teardown"]`)},
	)
	newStaleClient := func(t *testing.T) client.Client {
		return fake.NewClientBuilder().
			WithScheme(testScheme(t)).
			WithObjects(baselineIR("llama-engine", "prod", 1)).
			WithStatusSubresource(&v1beta1.InferenceReplica{}).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if _, ok := obj.(*v1beta1.InferenceReplica); ok {
						return reject
					}
					return cl.Update(ctx, obj, opts...)
				},
			}).
			Build()
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "llama-engine", Namespace: "prod"}}

	// Live view Terminating (held by a foreign finalizer so the fake
	// client keeps the object) → swallowed, requeue.
	live := baselineIR("llama-engine", "prod", 1)
	now := metav1.Now()
	live.DeletionTimestamp = &now
	live.Finalizers = []string{"test.ome.io/hold"}
	r := &Reconciler{
		Client:       newStaleClient(t),
		APIReader:    fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(live).Build(),
		Log:          ctrl.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
	}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("deletion-race finalizer add must not surface an error: %v", err)
	}
	if !result.Requeue {
		t.Errorf("deletion-race finalizer add must requeue onto the teardown path, got %+v", result)
	}

	// Live view NOT Terminating → the rejection is a real failure.
	r = &Reconciler{
		Client:       newStaleClient(t),
		APIReader:    fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(baselineIR("llama-engine", "prod", 1)).Build(),
		Log:          ctrl.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Errorf("finalizer-add rejection with a non-Terminating live view must surface an error")
	}
}

// TestTeardown_StatuslessOrphanBlocksCompletion is the load-bearing
// completion case: a live component pod with NO InstanceStatus entry —
// exactly the orphan teardown exists to prevent — must block finalizer
// removal even though the dispatcher has no Instance to delete.
func TestTeardown_StatuslessOrphanBlocksCompletion(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = nil
	orphan := podForIR(ir, 7, "default", 0, false, false)
	r, c := newReconciler(t, ir, orphan)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != workloadops.DeleteRequeueInterval {
		t.Errorf("orphan pod must keep teardown polling, got %+v", result)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), gotIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(gotIR, TeardownFinalizer) {
		t.Errorf("statusless orphan must block finalizer removal (live list is the completion check)")
	}
	if blocked := eventsContaining(drainEvents(rec), ReasonTeardownBlocked); len(blocked) != 1 || !strings.Contains(blocked[0], orphan.Name) {
		t.Errorf("TeardownBlocked must name the orphan survivor; events=%v", blocked)
	}
}

// TestTeardown_Complete_DeletesServiceAndFinalizer pins completion: no
// live pods → the remaining InstanceStatus is removed, the IR-owned
// headless Service is deleted, the finalizer lifts, and the IR is gone.
func TestTeardown_Complete_DeletesServiceAndFinalizer(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceDeleting},
	}
	svcName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ir.Namespace}}
	r, c := newReconciler(t, ir, svc)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("completed teardown must not requeue, got %+v", result)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: svcName, Namespace: ir.Namespace}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Errorf("headless Service must be deleted on completion; get returned %v", err)
	}
	// Finalizer removed → the fake client (like the apiserver) finishes
	// the deletion.
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("IR must be gone after finalizer removal; get returned %v", err)
	}
}

// TestTeardown_DeadlineExceeded_WarnsAndReleases pins the never-worse
// bound: past lifecycle.teardown.deadline with survivors, teardown
// warns TeardownDeadlineExceeded (naming them + the manual escape) and
// releases the finalizer to background GC — the pod itself is left
// alone.
func TestTeardown_DeadlineExceeded_WarnsAndReleases(t *testing.T) {
	base := time.Now()
	ir := terminatingIR("llama-engine", "prod", 1, base.Add(-time.Hour))
	orphan := podForIR(ir, 3, "default", 0, false, false)
	r, c := newReconciler(t, ir, orphan)
	r.Clock = clocktesting.NewFakeClock(base)
	withLifecycleConfig(r, `{"teardown":{"deadline":"30m"}}`)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("deadline release must return without requeue, got %+v", result)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("finalizer must be released past the deadline (IR gone); get returned %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(orphan), &corev1.Pod{}); err != nil {
		t.Errorf("survivor pod is GC's problem after release, must not be touched: %v", err)
	}
	exceeded := eventsContaining(drainEvents(rec), ReasonTeardownDeadlineExceeded)
	if len(exceeded) != 1 {
		t.Fatalf("expected one TeardownDeadlineExceeded warning, got %v", exceeded)
	}
	for _, want := range []string{orphan.Name, "background GC", "manual escape"} {
		if !strings.Contains(exceeded[0], want) {
			t.Errorf("TeardownDeadlineExceeded message must contain %q; got %q", want, exceeded[0])
		}
	}
}

// recordedPodDelete captures one pod Delete RPC's grace period.
type recordedPodDelete struct {
	name  string
	grace *int64
}

// TestTeardown_ForceDeleteEscalation_UnwedgesDeadNodePod proves the
// dead-node force-delete pairing works under Teardown: a pod wedged
// Terminating on a node whose Node object is gone, with lifecycle.forceDelete
// configured, is grace-0 force-deleted during the teardown Delete
// pass; the audit ledger lands on the IR (parent ISVC gone — ledger
// owner falls back to the IR) and teardown completes.
func TestTeardown_ForceDeleteEscalation_UnwedgesDeadNodePod(t *testing.T) {
	base := time.Now()
	ir := terminatingIR("llama-engine", "prod", 1, base.Add(-time.Minute))
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceDeleting},
	}
	wedged := podForIR(ir, 0, "default", 0, false, false)
	wedged.UID = types.UID("wedged-pod-uid")
	wedged.Spec.NodeName = "dead-node" // no Node object seeded → evidenceNodeGone

	// The fake client cannot store DeletionTimestamp without finalizers,
	// so present the Terminating view via a List interceptor: the stored
	// pod is deletable, the listed view is 10m overdue.
	wedgedDT := metav1.NewTime(base.Add(-10 * time.Minute))
	var deletes []recordedPodDelete
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, wedged).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				if pl, ok := list.(*corev1.PodList); ok {
					for i := range pl.Items {
						if pl.Items[i].Name == wedged.Name {
							pl.Items[i].DeletionTimestamp = &wedgedDT
						}
					}
				}
				return nil
			},
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.Pod); ok {
					do := &client.DeleteOptions{}
					for _, o := range opts {
						o.ApplyToDelete(do)
					}
					deletes = append(deletes, recordedPodDelete{name: obj.GetName(), grace: do.GracePeriodSeconds})
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	r := &Reconciler{
		Client:       c,
		Log:          ctrl.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
		Clock:        clocktesting.NewFakeClock(base),
	}
	withLifecycleConfig(r, `{"forceDelete":{"overdueSlack":"2m","nodeUnreachableThreshold":"5m"}}`)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The escalation must have grace-0 force-deleted the wedged pod.
	var forced *recordedPodDelete
	for i := range deletes {
		if deletes[i].name == wedged.Name && deletes[i].grace != nil && *deletes[i].grace == 0 {
			forced = &deletes[i]
		}
	}
	if forced == nil {
		t.Fatalf("expected a grace-0 force-delete of %s during teardown; recorded deletes=%+v", wedged.Name, deletes)
	}
	if got := eventsContaining(drainEvents(rec), string(workload.EventReasonPodForceDeleted)); len(got) != 1 {
		t.Errorf("expected one PodForceDeleted warning, got %v", got)
	}

	// Ledger owner fell back to the IR (parent NotFound → nil).
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	foundForceDelete := false
	for _, e := range ledger.Entries {
		if e.Reason == audit.ReasonForceDelete && e.RequestUUID == string(wedged.UID) {
			foundForceDelete = true
		}
	}
	if !foundForceDelete {
		t.Errorf("force-delete audit entry must land on the IR-owned ledger; entries=%+v", ledger.Entries)
	}
	cms := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), cms, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatalf("list configmaps: %v", err)
	}
	irOwnedLedger := false
	for _, cm := range cms.Items {
		for _, ref := range cm.OwnerReferences {
			if ref.Kind == "InferenceReplica" && ref.Name == ir.Name {
				irOwnedLedger = true
			}
		}
	}
	if !irOwnedLedger {
		t.Errorf("ledger ConfigMap must be owned by the IR when the parent is gone")
	}

	// With the wedge cleared, the same pass completes teardown: the
	// finalizer lifted and the IR is gone.
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}); !apierrors.IsNotFound(err) {
		t.Errorf("teardown must complete once the wedge is escalated; get returned %v", err)
	}
}
