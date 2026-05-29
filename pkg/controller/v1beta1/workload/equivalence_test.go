package workload_test

// Equivalence diff harness. Asserts that the K8s state produced by the
// legacy ISVC-OMENative path (`omenative.ReconcileComponent`) and the
// workload-package path produce byte-identical pods / CRs / services /
// per-Component status against the same fixture.
//
// Structured as a back-to-back comparison rather than a golden
// snapshot so legitimate evolution that updates both paths in
// lock-step doesn't surface as a false-positive regression.

import (
	"context"
	"reflect"
	"sort"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// fixture is the (ISVC, image, replicas, component) tuple both paths
// reconcile against. Every Test* function in this file constructs a
// fresh fixture, runs both paths against it, and diffs the produced
// cluster state.
type fixture struct {
	// name is the ISVC.Name. Both runs use the same name so the
	// per-Component selectors / Service names / CR names overlap
	// byte-for-byte.
	name string

	// namespace is the ISVC.Namespace.
	namespace string

	// component is the Component to reconcile (engine | decoder |
	// router). PD-disaggregated tests run the harness twice with
	// engine + decoder fixtures sharing the same parent ISVC.
	component v1beta1.ComponentType

	// image is the container image stamped on PodSpec.Containers[0].
	// Tests bump this between iterations to trigger update / surge
	// scenarios.
	image string

	// replicas is the desired MinReplicas count.
	replicas int

	// initialStatus, when non-nil, is set on the ISVC before the
	// first reconcile. Used by the in-place-update / surge scenarios
	// to seed a steady-state InstanceStatus the second iteration
	// will mutate from.
	initialStatus *v1beta1.OMENativeComponentStatus
}

// clusterState is the diffable snapshot both paths converge to.
// Cluster-stamped fields (timestamps, UIDs, resourceVersions) are
// dropped at the snapshot stage rather than at comparison so the diff
// stays grep-readable.
type clusterState struct {
	// crNames is the sorted "namespace/name" of every
	// ControllerRevision in the cluster. Two CRs with the same Data
	// hash to the same name; a divergence in the rendered PodSpec
	// surfaces as a different hash suffix in the name.
	crNames []string

	// crDataBytes maps "namespace/name" to the raw CR.Data bytes. We
	// don't unmarshal — the test compares JSON-bytes equality to
	// catch any per-byte divergence (a Render-time field reorder
	// would otherwise hash to the same name but carry different
	// Data).
	crDataBytes map[string][]byte

	// pods is a name → snapshotted Pod map. The snapshot includes
	// only the fields the workload renderer controls (labels,
	// annotations, ownerRefs, container shape); cluster-generated
	// fields (UID, ResourceVersion, CreationTimestamp) are zeroed.
	pods map[string]podSnapshot

	// services is a sorted "namespace/name" list of every Service in
	// the cluster. Both paths must emit the same per-Component
	// headless Service (and any per-revision routed Services the
	// drain path created).
	services []string

	// podGroups is a sorted "namespace/name" list of every PodGroup
	// emitted. Multi-pod scenarios assert this is non-empty and
	// matches between paths.
	podGroups []string

	// statusInstanceCount is the length of
	// ISVC.Status.Components[component].OMENative.InstanceStatuses
	// after the run. Both paths must produce the same number of
	// per-Instance entries.
	statusInstanceCount int

	// statusCurrentRevision mirrors the post-reconcile
	// OMENativeComponentStatus.CurrentRevision so the test catches
	// any rollout-promote drift.
	statusCurrentRevision string

	// statusUpdateRevision mirrors the post-reconcile
	// OMENativeComponentStatus.UpdateRevision.
	statusUpdateRevision string

	// statusReplicas mirrors the post-reconcile
	// OMENativeComponentStatus.Replicas counter.
	statusReplicas int32
}

// podSnapshot is the per-pod projection both paths must agree on. The
// fields here are workload-controlled — anything the cluster stamps
// (UIDs, resourceVersions, timestamps) is excluded by construction so
// the diff stays focused on render-time output.
type podSnapshot struct {
	// labels is the full pod label set. Both paths must produce the
	// same label-set (workload/ops.Render owns this surface, and
	// the renderer is shared via the dispatch shim).
	labels map[string]string

	// annotations is the full pod annotation set. Same divergence
	// rationale as labels.
	annotations map[string]string

	// containers is the per-container (Name, Image) tuple list. We
	// don't compare the full container spec because apiserver
	// defaulting on a fake client can stamp empty maps / nil pointers
	// inconsistently across runs; (Name, Image) is what the renderer
	// actually produces.
	containers []containerSnapshot

	// ownerControllerName is the .OwnerReferences[].Name where
	// Controller=true. The legacy + workload paths must both stamp
	// the ISVC as the controller owner. nil indicates no controller
	// owner (which is itself a divergence signal).
	ownerControllerName string

	// ownerControllerKind mirrors the controller owner's Kind for
	// the same reason.
	ownerControllerKind string
}

type containerSnapshot struct {
	name  string
	image string
}

// TestEquivalence_ColdStart_SingleComponent is the cold-start
// scenario: empty cluster, ISVC just created, single-pod engine
// Component, 1 replica. Both paths must produce identical pods
// (count, names, labels, owner refs), a single ControllerRevision,
// and one headless Service. Per-instance status should converge to
// exactly one InstanceStatus entry.
//
// This is the most basic scenario the refactor must not regress on —
// it covers the create-from-empty fast path that triggers on every
// fresh ISVC.
func TestEquivalence_ColdStart_SingleComponent(t *testing.T) {
	fix := fixture{
		name:      "eq-cold-engine",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "llm:v1",
		replicas:  1,
	}

	legacy := runLegacy(t, fix)
	wrkld := runWorkload(t, fix)

	assertEquivalent(t, legacy, wrkld)
}

// TestEquivalence_PDDisaggregated runs the cold-start equivalence
// scenario twice with engine + decoder Components against the same
// parent ISVC. PD-disaggregated topologies dispatch one
// ReconcileComponent per Component, so the per-Component cluster
// state for each must match across paths independently.
func TestEquivalence_PDDisaggregated(t *testing.T) {
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		comp := comp
		t.Run(string(comp), func(t *testing.T) {
			fix := fixture{
				name:      "eq-pd",
				namespace: "prod",
				component: comp,
				image:     "llm:v1",
				replicas:  1,
			}
			legacy := runLegacy(t, fix)
			wrkld := runWorkload(t, fix)
			assertEquivalent(t, legacy, wrkld)
		})
	}
}

// TestEquivalence_TwoReplicas exercises the multi-instance branch of
// Create — both paths must materialize 2 distinct pods (one per
// Instance index) with the right per-Instance index labels.
func TestEquivalence_TwoReplicas(t *testing.T) {
	fix := fixture{
		name:      "eq-2r",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "llm:v1",
		replicas:  2,
	}

	legacy := runLegacy(t, fix)
	wrkld := runWorkload(t, fix)

	assertEquivalent(t, legacy, wrkld)
}

// TestEquivalence_TwoPassConvergence drives BOTH paths through the
// reconcile twice in a row. The first pass creates pods (still
// Phase=Creating after one call because pods haven't reached
// runtime-ready yet); the second pass observes the same cluster state
// it just produced and must converge to the same in-flight status
// (no double-creates, no extra revisions). Catches any test-only
// idempotency divergence between the two paths.
func TestEquivalence_TwoPassConvergence(t *testing.T) {
	fix := fixture{
		name:      "eq-2pass",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "llm:v1",
		replicas:  1,
	}

	legacy := runLegacyN(t, fix, 2)
	wrkld := runWorkloadN(t, fix, 2)

	assertEquivalent(t, legacy, wrkld)
}

// TestEquivalence_FivePassSteadyState drives both paths through five
// reconciles to verify steady-state idempotency: no extra pods, no
// extra CRs, no Service flap. Catches any path-specific work the
// renderer would have re-issued (e.g., a CR re-created with a
// different salt, a Service re-Created at a new ClusterIP) that the
// shorter scenarios above wouldn't surface.
func TestEquivalence_FivePassSteadyState(t *testing.T) {
	fix := fixture{
		name:      "eq-5pass",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "llm:v1",
		replicas:  1,
	}

	legacy := runLegacyN(t, fix, 5)
	wrkld := runWorkloadN(t, fix, 5)

	assertEquivalent(t, legacy, wrkld)
}

// SurgeThenDrain equivalence is NOT covered here: the full rollout
// depends on real kubelet behaviour (ContainersReady=True after image
// roll, drained EndpointSlice convergence) that the controller-runtime
// fake client cannot synthesize. The KIND suite under
// tests/integration/singlecluster/controller/coordination_kind/ is
// the authoritative coverage for cross-Component rollout dynamics.

// runLegacy drives `omenative.ReconcileComponent` once against a
// fresh fixture and snapshots the resulting cluster state.
func runLegacy(t *testing.T, fix fixture) clusterState {
	t.Helper()
	return runLegacyN(t, fix, 1)
}

// runLegacyN runs ReconcileComponent N times against a fresh fixture
// and snapshots the post-final-pass cluster state. Used by
// scenarios that need multi-pass convergence (steady-state after
// create, in-place update after promote).
func runLegacyN(t *testing.T, fix fixture, passes int) clusterState {
	t.Helper()
	ctx := context.Background()
	isvc := buildEquivalenceISVC(fix)
	cli := newEquivalenceClient(t, isvc)
	params := buildEquivalenceParams(fix, isvc, cli)
	// Per-run Expectations so the legacy + workload runs don't share
	// the package-level singleton state. Without this isolation, the
	// first run's pending ExpectCreates entry would block the second
	// run's Satisfied() gate and silently skip the create — the diff
	// would surface as a pod-count divergence the second pass.
	params.Expectations = workload.NewExpectations()

	for i := 0; i < passes; i++ {
		// Don't t.Fatalf on errors — for the cold-start /
		// steady-state scenarios both paths return nil-or-requeue
		// in practice. A real error surfaces as a divergence at
		// the snapshot stage (no pods created, etc.).
		_, _ = omenative.ReconcileComponent(ctx, params)
		// The per-Component stable Service reconciliation was moved out
		// of omenative.ReconcileComponent to the per-Component Reconcile()
		// in components/{engine,decoder,router}.go's
		// reconcileOMENativeSubresources method so it fires for both
		// OMENative sub-paths (direct and IR-managed). Mirror that call
		// here so the legacy snapshot includes the same stable Service
		// the workload-path snapshot does — without this mirror, the
		// Services list diverges by one entry per Component.
		if params.PodSpec != nil {
			stableMeta := params.ObjectMeta.DeepCopy()
			stableMeta.OwnerReferences = []metav1.OwnerReference{
				*metav1.NewControllerRef(params.ISVC, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			}
			stableSelector := map[string]string{
				"ome.io/inferenceservice": params.ISVC.Name,
				"component":               string(params.Component),
				query.LabelManagedBy:      query.ManagedByOMENative,
			}
			sr := service.NewServiceReconciler(params.Client, nil, *stableMeta, params.ComponentExt, params.PodSpec, stableSelector)
			_, _ = sr.Reconcile()
		}
	}

	return snapshotClusterState(t, ctx, cli, isvc, fix.component)
}

// runWorkload drives the workload-path entry point once against a
// fresh fixture and snapshots the resulting cluster state. See the
// package comment for the structural breakdown.
func runWorkload(t *testing.T, fix fixture) clusterState {
	t.Helper()
	return runWorkloadN(t, fix, 1)
}

// runWorkloadN is runWorkload's multi-pass variant.
func runWorkloadN(t *testing.T, fix fixture, passes int) clusterState {
	t.Helper()
	ctx := context.Background()
	isvc := buildEquivalenceISVC(fix)
	cli := newEquivalenceClient(t, isvc)
	params := buildEquivalenceParams(fix, isvc, cli)
	params.Expectations = workload.NewExpectations()

	for i := 0; i < passes; i++ {
		if err := runWorkloadOnce(ctx, params); err != nil {
			t.Fatalf("runWorkloadOnce pass %d: %v", i, err)
		}
	}

	return snapshotClusterState(t, ctx, cli, isvc, fix.component)
}

// runWorkloadOnce mirrors `omenative.ReconcileComponent`'s pipeline
// but calls `workloadops.Create` directly instead of going through the
// `omenative/ops` dispatch shim. This is the divergent step the
// harness exists to compare against the legacy path.
func runWorkloadOnce(ctx context.Context, params core.ReconcileParams) error {
	// Step 1: supporting Services. The legacy
	// `ensureHeadlessService` helper is misleadingly named — it
	// emits BOTH the per-Component headless Service AND the per-
	// Component stable Service. The new path mirrors both so the
	// Service-name snapshot matches between paths. The stable Service
	// is now built by the top-level service.ServiceReconciler (same
	// reconciler RawDeployment / MultiNode use); we invoke it here
	// directly so this harness mirrors the legacy path's emission.
	headlessSpec := omenative.BuildHeadlessServiceSpec(params.ISVC, params.Component)
	headless, err := workload.BuildHeadlessService(headlessSpec)
	if err != nil {
		return err
	}
	if err := ensureService(ctx, params.Client, headless); err != nil {
		return err
	}
	if params.PodSpec != nil {
		stableMeta := params.ObjectMeta.DeepCopy()
		stableMeta.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(params.ISVC, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
		}
		// Selector mirrors what omenative.componentSelector emits — the
		// three keys must match componentSelector exactly so the
		// snapshot diff (which doesn't compare selector contents per-se
		// but DOES compare Service names) stays valid across paths.
		stableSelector := map[string]string{
			"ome.io/inferenceservice": params.ISVC.Name,
			"component":               string(params.Component),
			query.LabelManagedBy:      query.ManagedByOMENative,
		}
		sr := service.NewServiceReconciler(params.Client, nil, *stableMeta, params.ComponentExt, params.PodSpec, stableSelector)
		if _, err := sr.Reconcile(); err != nil {
			return err
		}
	}

	// Step 2: ComponentPlan.
	plan, err := core.BuildPlan(params)
	if err != nil {
		return err
	}

	// Step 3: ControllerRevision.
	var target *appsv1.ControllerRevision
	if params.PodSpec != nil {
		var collisionCount *int32
		if cs, ok := params.ISVC.Status.Components[params.Component]; ok && cs.OMENative != nil {
			collisionCount = cs.OMENative.CollisionCount
		}
		t, _, err := core.EnsureControllerRevisionForISVC(ctx, params.Client, params.Reader(), params.ISVC, params.Component, params.PodSpec, &params.ObjectMeta, collisionCount)
		if err != nil {
			return err
		}
		target = t
	}

	// Step 4: workload.ReconcileInput via the canonical adapter
	// projection. This is the production projection the legacy
	// op-shims (omenative/ops/create.go, delete.go, ...) use under
	// the hood; reusing it here keeps the harness honest about the
	// projection's role as the boundary between adapter and
	// workload.
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:          params.ISVC,
		Component:     params.Component,
		ObjectMeta:    params.ObjectMeta,
		PodSpec:       params.PodSpec,
		WorkerPodSpec: params.WorkerPodSpec,
		Client:        params.Client,
		Expectations:  params.ExpectationsCache(),
	})

	deps := workload.Deps{
		Client:       params.Client,
		APIReader:    params.APIReader,
		Recorder:     params.Recorder,
		Expectations: params.ExpectationsCache(),
		RenderHook:   core.ISVCRenderHook(params.ISVC),
	}

	// Step 5: workloadops.Create — the divergent call. Other ops
	// (Delete / Update / Restart / Migrate) aren't exercised in the
	// cold-start / two-pass scenarios this harness covers; those
	// scenarios always converge through the Create state machine.
	if _, err := workloadops.Create(ctx, deps, input, plan, target); err != nil {
		return err
	}

	// Step 6: AggregateAndWriteStatus.
	if err := omenative.AggregateAndWriteStatus(ctx, params, plan, target); err != nil {
		return err
	}
	return nil
}

// assertEquivalent diffs the two cluster snapshots and fails the test
// with a focused error per divergence. We don't short-circuit on the
// first divergence — surfacing every diff in one run keeps the
// triage feedback loop tight when a refactor breaks multiple
// dimensions at once.
func assertEquivalent(t *testing.T, legacy, wrkld clusterState) {
	t.Helper()

	if !equalStringSlices(legacy.crNames, wrkld.crNames) {
		t.Errorf("ControllerRevision names differ:\n  legacy=%v\n  workload=%v", legacy.crNames, wrkld.crNames)
	}
	// Compare CR.Data byte-for-byte. A hash-name match without
	// byte-match is the canonical hash-collision signal.
	for name, lBytes := range legacy.crDataBytes {
		wBytes, ok := wrkld.crDataBytes[name]
		if !ok {
			continue // already reported by the names diff above.
		}
		if string(lBytes) != string(wBytes) {
			t.Errorf("ControllerRevision %q Data bytes differ between paths (same hash, different bytes — indicates a render-time mutation drift)", name)
		}
	}

	if len(legacy.pods) != len(wrkld.pods) {
		t.Errorf("Pod count differs: legacy=%d workload=%d (legacy names=%v, workload names=%v)",
			len(legacy.pods), len(wrkld.pods), podNames(legacy.pods), podNames(wrkld.pods))
	}
	for name, lp := range legacy.pods {
		wp, ok := wrkld.pods[name]
		if !ok {
			t.Errorf("Pod %q present in legacy but absent in workload", name)
			continue
		}
		if !reflect.DeepEqual(lp.labels, wp.labels) {
			t.Errorf("Pod %q labels differ:\n  legacy=%v\n  workload=%v", name, lp.labels, wp.labels)
		}
		if !reflect.DeepEqual(lp.annotations, wp.annotations) {
			t.Errorf("Pod %q annotations differ:\n  legacy=%v\n  workload=%v", name, lp.annotations, wp.annotations)
		}
		if !reflect.DeepEqual(lp.containers, wp.containers) {
			t.Errorf("Pod %q container shape differs:\n  legacy=%v\n  workload=%v", name, lp.containers, wp.containers)
		}
		if lp.ownerControllerName != wp.ownerControllerName {
			t.Errorf("Pod %q controller owner name differs: legacy=%q workload=%q", name, lp.ownerControllerName, wp.ownerControllerName)
		}
		if lp.ownerControllerKind != wp.ownerControllerKind {
			t.Errorf("Pod %q controller owner kind differs: legacy=%q workload=%q", name, lp.ownerControllerKind, wp.ownerControllerKind)
		}
	}
	for name := range wrkld.pods {
		if _, ok := legacy.pods[name]; !ok {
			t.Errorf("Pod %q present in workload but absent in legacy", name)
		}
	}

	if !equalStringSlices(legacy.services, wrkld.services) {
		t.Errorf("Service names differ:\n  legacy=%v\n  workload=%v", legacy.services, wrkld.services)
	}
	if !equalStringSlices(legacy.podGroups, wrkld.podGroups) {
		t.Errorf("PodGroup names differ:\n  legacy=%v\n  workload=%v", legacy.podGroups, wrkld.podGroups)
	}

	if legacy.statusInstanceCount != wrkld.statusInstanceCount {
		t.Errorf("ISVC.Status InstanceStatuses count differs: legacy=%d workload=%d",
			legacy.statusInstanceCount, wrkld.statusInstanceCount)
	}
	if legacy.statusReplicas != wrkld.statusReplicas {
		t.Errorf("ISVC.Status Replicas differ: legacy=%d workload=%d",
			legacy.statusReplicas, wrkld.statusReplicas)
	}
	if legacy.statusCurrentRevision != wrkld.statusCurrentRevision {
		t.Errorf("ISVC.Status CurrentRevision differs: legacy=%q workload=%q",
			legacy.statusCurrentRevision, wrkld.statusCurrentRevision)
	}
	if legacy.statusUpdateRevision != wrkld.statusUpdateRevision {
		t.Errorf("ISVC.Status UpdateRevision differs: legacy=%q workload=%q",
			legacy.statusUpdateRevision, wrkld.statusUpdateRevision)
	}
}

// buildEquivalenceISVC composes the fixture both paths reconcile
// against. Centralized so the two runs receive byte-identical inputs.
func buildEquivalenceISVC(fix fixture) *v1beta1.InferenceService {
	mr := fix.replicas
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fix.name,
			Namespace: fix.namespace,
			UID:       types.UID(fix.name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{},
	}

	switch fix.component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &mr,
			},
		}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &mr,
			},
		}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &mr,
			},
		}
	}

	if fix.initialStatus != nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			fix.component: {OMENative: fix.initialStatus},
		}
	}
	return isvc
}

// buildEquivalenceParams is the ReconcileParams shape both paths
// consume. The PodSpec is identical to what
// omenative/test_helpers_test.go's minimalParams produces, so the
// existing per-op tests' expectations carry over.
func buildEquivalenceParams(fix fixture, isvc *v1beta1.InferenceService, cli client.Client) core.ReconcileParams {
	var ext *v1beta1.ComponentExtensionSpec
	switch fix.component {
	case v1beta1.EngineComponent:
		ext = &isvc.Spec.Engine.ComponentExtensionSpec
	case v1beta1.DecoderComponent:
		ext = &isvc.Spec.Decoder.ComponentExtensionSpec
	case v1beta1.RouterComponent:
		ext = &isvc.Spec.Router.ComponentExtensionSpec
	}
	return core.ReconcileParams{
		ISVC: isvc,
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.StableServiceName(isvc.Name, workload.ComponentType(fix.component)),
			Namespace: isvc.Namespace,
		},
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: fix.image}}},
		Component:    fix.component,
		ComponentExt: ext,
		Client:       cli,
	}
}

// newEquivalenceClient builds a controller-runtime fake client with
// the schemes both paths exercise. Mirrors
// omenative/test_helpers_test.go's newFakeClientWithDiscovery but
// lives in this file so it doesn't reach across the test-package
// boundary.
func newEquivalenceClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	mustAddToScheme(t, scheme, corev1.AddToScheme, "corev1")
	mustAddToScheme(t, scheme, v1beta1.AddToScheme, "v1beta1")
	mustAddToScheme(t, scheme, appsv1.AddToScheme, "appsv1")
	mustAddToScheme(t, scheme, discoveryv1.AddToScheme, "discoveryv1")
	mustAddToScheme(t, scheme, monitoringv1.AddToScheme, "monitoringv1")
	mustAddToScheme(t, scheme, schedulingv1alpha1.AddToScheme, "schedulingv1alpha1")
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

func mustAddToScheme(t *testing.T, s *runtime.Scheme, add func(*runtime.Scheme) error, name string) {
	t.Helper()
	if err := add(s); err != nil {
		t.Fatalf("add %s scheme: %v", name, err)
	}
}

// ensureService creates the Service if absent. Mirrors what
// workload.ReconcileHeadlessService does at the wire level so the
// "new path" produces the same Service shape the legacy path would.
// For the cold-start / two-pass scenarios this harness covers, the
// Service is byte-identical between passes so a present-Service
// branch is a no-op (mirrors the legacy CreateOrUpdate semantics
// without pulling in the heavier controllerutil dependency).
func ensureService(ctx context.Context, cli client.Client, svc *corev1.Service) error {
	current := &corev1.Service{}
	err := cli.Get(ctx, client.ObjectKeyFromObject(svc), current)
	switch {
	case err == nil:
		return nil
	case client.IgnoreNotFound(err) != nil:
		return err
	}
	return cli.Create(ctx, svc)
}

// snapshotClusterState reads the post-reconcile cluster shape into a
// diffable struct. Captures only the surfaces the workload
// refactor could conceivably drift on; cluster-stamped fields (UID,
// ResourceVersion, CreationTimestamp) are intentionally elided so the
// diff stays focused on render-time output.
func snapshotClusterState(t *testing.T, ctx context.Context, cli client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) clusterState {
	t.Helper()
	out := clusterState{
		pods:        map[string]podSnapshot{},
		crDataBytes: map[string][]byte{},
	}

	crs := &appsv1.ControllerRevisionList{}
	if err := cli.List(ctx, crs); err != nil {
		t.Fatalf("snapshot: list ControllerRevisions: %v", err)
	}
	for _, cr := range crs.Items {
		key := cr.Namespace + "/" + cr.Name
		out.crNames = append(out.crNames, key)
		out.crDataBytes[key] = append([]byte(nil), cr.Data.Raw...)
	}
	sort.Strings(out.crNames)

	pods := &corev1.PodList{}
	if err := cli.List(ctx, pods); err != nil {
		t.Fatalf("snapshot: list Pods: %v", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		out.pods[p.Namespace+"/"+p.Name] = newPodSnapshot(p)
	}

	svcs := &corev1.ServiceList{}
	if err := cli.List(ctx, svcs); err != nil {
		t.Fatalf("snapshot: list Services: %v", err)
	}
	for _, s := range svcs.Items {
		out.services = append(out.services, s.Namespace+"/"+s.Name)
	}
	sort.Strings(out.services)

	pgs := &schedulingv1alpha1.PodGroupList{}
	if err := cli.List(ctx, pgs); err == nil {
		for _, g := range pgs.Items {
			out.podGroups = append(out.podGroups, g.Namespace+"/"+g.Name)
		}
		sort.Strings(out.podGroups)
	}
	// Missing PodGroup CRD is fine for the single-pod scenarios; only
	// multi-pod fixtures populate this list.

	fresh := &v1beta1.InferenceService{}
	if err := cli.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("snapshot: re-read ISVC: %v", err)
	}
	if cs, ok := fresh.Status.Components[component]; ok && cs.OMENative != nil {
		out.statusInstanceCount = len(cs.OMENative.InstanceStatuses)
		out.statusCurrentRevision = cs.OMENative.CurrentRevision
		out.statusUpdateRevision = cs.OMENative.UpdateRevision
		out.statusReplicas = cs.OMENative.Replicas
	}

	return out
}

// newPodSnapshot extracts the diffable subset of a Pod's metadata.
// Zeroing cluster-stamped fields here (rather than ignoring them at
// the comparison stage) keeps the per-test diff output focused on the
// fields the workload renderer actually owns.
func newPodSnapshot(p *corev1.Pod) podSnapshot {
	out := podSnapshot{
		labels:      copyStringMap(p.Labels),
		annotations: copyStringMap(p.Annotations),
	}
	for _, c := range p.Spec.Containers {
		out.containers = append(out.containers, containerSnapshot{name: c.Name, image: c.Image})
	}
	for _, ref := range p.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			out.ownerControllerName = ref.Name
			out.ownerControllerKind = ref.Kind
			break
		}
	}
	return out
}

// copyStringMap copies a map[string]string so the snapshot doesn't
// alias the live pod's label map (which the controller may keep
// mutating between snapshots in the same test process).
func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// equalStringSlices is an ordering-sensitive comparator; all callers
// pre-sort their inputs.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// podNames returns the sorted name set of a pod-snapshot map for
// reporting diff context — keeps the error messages grep-friendly.
func podNames(m map[string]podSnapshot) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
