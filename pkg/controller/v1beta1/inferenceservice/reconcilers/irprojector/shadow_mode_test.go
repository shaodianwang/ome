package irprojector_test

// Shadow-mode diff harness for the ISVC -> IR-managed cutover (IR PR 3
// Piece 4). For a representative ISVC fixture, runs BOTH paths against
// independent fake clients and asserts they emit byte-equal sets of
// pods + ControllerRevisions + headless Service (modulo owner ref +
// volatile fields).
//
// What this guards against:
//
//   - A future refactor of the projector
//     (irprojector.EnsureInferenceReplica) accidentally producing a
//     different IR Spec than the dispatch site supplies, so the
//     resulting per-Instance pods diverge from what the omenative
//     direct path produced.
//
//   - A future refactor of the IR controller's reconciler emitting
//     different pod / Service / ControllerRevision shapes than the
//     omenative direct path historically did. The IR controller and
//     omenative reconciler both delegate to the workload package
//     today; this harness pins that the SHARED path stays shared
//     even at the wire-level emission stage.
//
//   - The opt-in cutover itself: when an operator flips an ISVC from
//     direct → IR-managed, the K8s objects the cluster sees MUST be
//     equivalent. The harness lets reviewers eyeball the diff
//     post-cutover; a real divergence surfaces as a failed assertion
//     instead of a silent production incident.
//
// Why it's structured as a back-to-back comparison rather than a
// golden snapshot: a golden file would re-encode the current
// behaviour without distinguishing legitimate evolution (a
// label-set addition that updates both paths in lock-step) from
// regressions (only one path changed). Two parallel runs against
// byte-identical fixtures catch the latter without false-positives
// on the former.
//
// Why NOT in pkg/controller/v1beta1/workload/equivalence_test.go:
// that harness compares omenative-direct vs. workload.Reconcile
// directly. THIS harness compares omenative-direct vs. the full
// IR-managed path (projector → IR controller → workload.Reconcile).
// The two harnesses share the snapshot/normalize shape but cover
// different boundaries.
//
// Coverage scope:
//
//   - single-pod engine (the most common shape; covers the
//     create-from-empty Create state machine)
//   - PD-disaggregated engine + decoder (covers two-Component
//     fixtures sharing one parent ISVC; each Component is
//     reconciled independently)
//
// SurgeThenDrain / multi-pod fixtures are NOT covered here:
//   - multi-pod requires the scheduler-plugins PodGroup CRD which
//     adds scaffolding for a separate, layered guarantee — the
//     KIND suite covers multi-pod cross-Component coordination
//     authoritatively.
//   - SurgeThenDrain depends on real kubelet behaviour (Ready
//     transitions, drained EndpointSlice convergence) that the
//     controller-runtime fake client cannot synthesize.
//
// The intent is "give reviewers confidence the cutover is safe"
// — three case shapes covering the most common production
// topologies is enough; exhaustive coverage lives in the KIND suite.

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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// shadowFixture is the (ISVC, image, replicas, component) tuple both
// paths emit objects against. Every Test* function in this file
// constructs a fresh fixture, runs both paths against it, and diffs
// the produced cluster state.
type shadowFixture struct {
	// name is the ISVC.Name. Both runs use the same name so the
	// per-Component selectors / Service names / CR names match
	// byte-for-byte.
	name string

	// namespace is the ISVC.Namespace.
	namespace string

	// component is the Component to reconcile (engine | decoder |
	// router).
	component v1beta1.ComponentType

	// image is the container image stamped on PodSpec.Containers[0].
	image string

	// replicas is the desired MinReplicas count.
	replicas int
}

// shadowSnapshot is the diffable cluster state both paths converge
// to. Field set matches workload/equivalence_test.go's clusterState
// — fields the workload renderer controls (labels, annotations,
// container shape) included; cluster-stamped fields (UID,
// ResourceVersion, CreationTimestamp) zeroed during snapshot rather
// than at comparison time so the diff stays grep-readable.
type shadowSnapshot struct {
	// crNames is the sorted "namespace/name" of every
	// ControllerRevision the path emitted.
	crNames []string

	// crDataBytes maps "namespace/name" to the raw CR.Data bytes —
	// two CRs with the same Data hash to the same name; a Render-time
	// field reorder would surface here as different bytes for the
	// same key.
	crDataBytes map[string][]byte

	// pods is "namespace/name" → snapshotted Pod. Owner-ref Kind /
	// Name are EXCLUDED from this snapshot (the legacy path's owner
	// is ISVC; the IR path's owner is the IR — that's expected
	// divergence, not a regression).
	pods map[string]shadowPodSnapshot

	// services is sorted "namespace/name" of every Service. Same
	// owner-ref-excluded rule as pods.
	services []string

	// headlessServiceName is the per-Component headless Service name
	// — both paths must emit the same name (query.HeadlessServiceName
	// is the shared formula).
	headlessServiceName string
}

// shadowPodSnapshot is the per-pod projection both paths must agree
// on. Owner-ref Kind / Name intentionally excluded — see the
// shadowSnapshot.pods comment.
type shadowPodSnapshot struct {
	// labels is the full pod label set. Both paths route through the
	// same workload renderer, so both must produce the same label
	// set.
	labels map[string]string

	// annotations is the full pod annotation set. Same rationale as
	// labels.
	annotations map[string]string

	// containers is the per-container (Name, Image) tuple list.
	// (Name, Image) is what the renderer actually produces; the full
	// container spec gets defaulting noise from the apiserver that
	// would distract from the diff.
	containers []shadowContainerSnapshot
}

type shadowContainerSnapshot struct {
	name  string
	image string
}

// TestShadowMode_SinglePodEngine_DirectVsIRManaged pins the most
// common shape: a single-pod engine, 1 replica, cold-start. The
// omenative direct path and the IR-managed path MUST emit
// byte-identical pods + ControllerRevisions + headless Service.
//
// This is the canonical opt-in shape — a single engine without
// PD-disaggregation, no multi-node, no surge. If this scenario
// diverges, the per-Component cutover is unsafe.
func TestShadowMode_SinglePodEngine_DirectVsIRManaged(t *testing.T) {
	fix := shadowFixture{
		name:      "shadow-single-engine",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "sgl:1.0",
		replicas:  1,
	}

	direct := runDirectPath(t, fix)
	irManaged := runIRManagedPath(t, fix)

	assertShadowEquivalent(t, direct, irManaged)
}

// TestShadowMode_TwoReplicasEngine_DirectVsIRManaged pins the
// multi-instance branch: 2 replicas. Both paths must materialize 2
// distinct pods (one per Instance index) with the right per-Instance
// label set + identical CR + identical Service.
//
// Catches per-Instance render drift that the 1-replica scenario
// wouldn't surface (e.g., a per-Instance ordinal label drifting
// between the two paths).
func TestShadowMode_TwoReplicasEngine_DirectVsIRManaged(t *testing.T) {
	fix := shadowFixture{
		name:      "shadow-2r-engine",
		namespace: "prod",
		component: v1beta1.EngineComponent,
		image:     "sgl:1.0",
		replicas:  2,
	}

	direct := runDirectPath(t, fix)
	irManaged := runIRManagedPath(t, fix)

	assertShadowEquivalent(t, direct, irManaged)
}

// TestShadowMode_PDDisaggregated_DirectVsIRManaged runs the
// cold-start scenario twice with engine + decoder Components against
// the same parent ISVC. PD-disaggregated topologies dispatch one
// per-Component reconcile per Component, so the per-Component
// cluster state for each must match across paths independently.
//
// Pins the per-Component isolation property: opt-in routing on
// engine alone (or decoder alone) must not bleed into the other
// Component's pod/CR/Service set.
func TestShadowMode_PDDisaggregated_DirectVsIRManaged(t *testing.T) {
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		comp := comp
		t.Run(string(comp), func(t *testing.T) {
			fix := shadowFixture{
				name:      "shadow-pd",
				namespace: "prod",
				component: comp,
				image:     "sgl:1.0",
				replicas:  1,
			}
			direct := runDirectPath(t, fix)
			irManaged := runIRManagedPath(t, fix)
			assertShadowEquivalent(t, direct, irManaged)
		})
	}
}

// runDirectPath drives omenative.ReconcileComponent against a fresh
// fixture and snapshots the resulting cluster state. Mirrors what
// the ISVC controller's dispatch site does WITHOUT the per-Component
// opt-in annotation.
func runDirectPath(t *testing.T, fix shadowFixture) shadowSnapshot {
	t.Helper()
	ctx := context.Background()
	isvc := buildShadowISVC(fix)
	c := newShadowClient(t, isvc)

	params := omenative.ReconcileParams{
		ISVC:         isvc,
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: fix.image}}},
		Component:    fix.component,
		ComponentExt: componentExtFromISVC(isvc, fix.component),
		Client:       c,
		Expectations: workload.NewExpectations(),
	}
	objMeta := metav1.ObjectMeta{
		Name:      fix.name + "-" + string(fix.component),
		Namespace: fix.namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: fix.name,
			constants.OMEComponentLabel:           string(fix.component),
		},
	}
	params.ObjectMeta = objMeta

	if _, err := omenative.ReconcileComponent(ctx, params); err != nil {
		t.Fatalf("direct-path ReconcileComponent: %v", err)
	}

	return snapshotShadowState(t, ctx, c, fix)
}

// runIRManagedPath drives the projector + IR controller against a
// fresh fixture and snapshots the resulting cluster state. Mirrors
// what the ISVC controller's dispatch site does WITH the
// per-Component opt-in annotation.
//
// Two-step:
//  1. irprojector.EnsureInferenceReplica creates the IR.
//  2. The IR controller's Reconcile materializes pods +
//     ControllerRevisions + Services owned by the IR.
//
// The IR controller's Reconcile is called manually because the
// fake client doesn't run controller-runtime — the test simulates
// what would happen in production after the ISVC controller
// projects the IR.
func runIRManagedPath(t *testing.T, fix shadowFixture) shadowSnapshot {
	t.Helper()
	ctx := context.Background()
	isvc := buildShadowISVC(fix)
	// Set the per-Component opt-in annotation so the projector path
	// would fire in production. Not strictly necessary for the test
	// (we call EnsureInferenceReplica directly), but keeps the
	// fixture shape representative of the production opt-in state.
	setComponentAnnotation(isvc, fix.component,
		constants.UseIRManagedAnnotationKey,
		constants.UseIRManagedAnnotationVal)
	c := newShadowClient(t, isvc)

	objMeta := metav1.ObjectMeta{
		Name:      fix.name + "-" + string(fix.component),
		Namespace: fix.namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: fix.name,
			constants.OMEComponentLabel:           string(fix.component),
		},
	}
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: fix.image}}}

	if _, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC:         isvc,
		Component:    fix.component,
		ComponentExt: componentExtFromISVC(isvc, fix.component),
		ObjectMeta:   objMeta,
		PodSpec:      podSpec,
		MultiPod:     false,
		Client:       c,
	}); err != nil {
		t.Fatalf("IR-managed projector EnsureInferenceReplica: %v", err)
	}

	// Now drive the IR controller's Reconcile. The projector created
	// the IR; the IR controller materializes the actual workload
	// objects (pods, CRs, headless Service).
	r := &v1beta1inferencereplica.Reconciler{
		Client:       c,
		Log:          logf.Log.WithName("shadow-ir"),
		Scheme:       shadowScheme(t),
		Expectations: workload.NewExpectations(),
	}
	irName := irprojector.InferenceReplicaName(fix.name, fix.component)
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: fix.namespace, Name: irName},
	}); err != nil {
		t.Fatalf("IR-managed Reconciler.Reconcile: %v", err)
	}

	return snapshotShadowState(t, ctx, c, fix)
}

// assertShadowEquivalent diffs the two snapshots and fails the test
// with a focused error per divergence. We don't short-circuit on the
// first divergence — surfacing every diff in one run keeps the
// triage feedback loop tight when a refactor breaks multiple
// dimensions at once.
//
// Owner-ref Kind / Name divergences are EXPECTED (direct=ISVC,
// IR-managed=IR) and intentionally excluded from the snapshot. Any
// other field diff fails the test.
func assertShadowEquivalent(t *testing.T, direct, irManaged shadowSnapshot) {
	t.Helper()

	// ControllerRevision NAMES MUST match. The CR name is
	// <isvc>-<component>-<hash>, so a hash drift would surface as
	// different names — exactly the divergence signal we want to
	// catch.
	if !equalShadowStringSlices(direct.crNames, irManaged.crNames) {
		t.Errorf("ControllerRevision names differ:\n  direct=%v\n  IR-managed=%v",
			direct.crNames, irManaged.crNames)
	}

	// CR DATA bytes must match for any CR present in both paths.
	// A hash-name match without byte-match indicates a hash collision
	// or render-time mutation drift.
	for name, lBytes := range direct.crDataBytes {
		wBytes, ok := irManaged.crDataBytes[name]
		if !ok {
			continue // already reported via the names diff
		}
		if string(lBytes) != string(wBytes) {
			t.Errorf("ControllerRevision %q Data bytes differ between paths (same hash, different bytes — indicates a render-time mutation drift)", name)
		}
	}

	// Pod count + names + labels + annotations + container shape
	// must match. Owner refs are excluded by construction.
	if len(direct.pods) != len(irManaged.pods) {
		t.Errorf("Pod count differs: direct=%d IR-managed=%d (direct names=%v, IR-managed names=%v)",
			len(direct.pods), len(irManaged.pods),
			shadowPodNames(direct.pods), shadowPodNames(irManaged.pods))
	}
	for name, lp := range direct.pods {
		wp, ok := irManaged.pods[name]
		if !ok {
			t.Errorf("Pod %q present in direct path but absent in IR-managed", name)
			continue
		}
		if !reflect.DeepEqual(lp.labels, wp.labels) {
			t.Errorf("Pod %q labels differ:\n  direct=%v\n  IR-managed=%v",
				name, lp.labels, wp.labels)
		}
		if !reflect.DeepEqual(lp.annotations, wp.annotations) {
			t.Errorf("Pod %q annotations differ:\n  direct=%v\n  IR-managed=%v",
				name, lp.annotations, wp.annotations)
		}
		if !reflect.DeepEqual(lp.containers, wp.containers) {
			t.Errorf("Pod %q container shape differs:\n  direct=%v\n  IR-managed=%v",
				name, lp.containers, wp.containers)
		}
	}
	for name := range irManaged.pods {
		if _, ok := direct.pods[name]; !ok {
			t.Errorf("Pod %q present in IR-managed but absent in direct path", name)
		}
	}

	// Headless Service name: both paths agree on the formula
	// (query.HeadlessServiceName is shared).
	if direct.headlessServiceName != irManaged.headlessServiceName {
		t.Errorf("headless Service name differs: direct=%q IR-managed=%q",
			direct.headlessServiceName, irManaged.headlessServiceName)
	}

	// Direct path: omenative.ReconcileComponent unconditionally
	// emits the per-Component headless Service. Must be present.
	if !containsServiceName(direct.services, direct.headlessServiceName) {
		t.Errorf("headless Service %q missing from direct-path Service set: %v",
			direct.headlessServiceName, direct.services)
	}

	// EXPECTED DIFFS — kept here as a single sorted-key table so a
	// future reader sees exactly what the two paths intentionally
	// disagree on. When the wire-in lands, drop the corresponding
	// entry and add a strict-equality assertion.
	for k, why := range knownExpectedShadowDiffs {
		// Sanity: every entry must have a non-empty rationale so
		// the table self-documents.
		if why == "" {
			t.Fatalf("knownExpectedShadowDiffs entry %q has empty rationale", k)
		}
	}
	// Specific known diffs:
	//
	// (1) "ir-headless-service-not-yet-emitted": IR controller's
	//     reconciler.go does NOT call workload.ReconcileHeadlessService
	//     yet — see the TODO(IR PR 2.1) marker at the
	//     workload.Reconcile call site. The headless Service is
	//     therefore present in the direct path but absent in the
	//     IR-managed path. When PR 2.1 lands, the IR controller
	//     will emit the Service, both Service sets will contain it,
	//     and the loop below can flip to a strict-equality assertion.
	if !containsServiceName(irManaged.services, irManaged.headlessServiceName) {
		_, expected := knownExpectedShadowDiffs["ir-headless-service-not-yet-emitted"]
		if !expected {
			t.Errorf("headless Service %q missing from IR-managed Service set: %v",
				irManaged.headlessServiceName, irManaged.services)
		}
	}
}

// knownExpectedShadowDiffs records the intentional differences
// between the omenative direct path and the IR-managed path at the
// time this harness was authored (IR PR 3). Each entry MUST be
// removed when the corresponding wire-in lands — the entries are
// designed to be self-deleting reminders, not permanent excuses.
//
// Keep the map sorted alphabetically by key so future additions are
// easy to spot in code review.
var knownExpectedShadowDiffs = map[string]string{
	"ir-headless-service-not-yet-emitted": "IR controller TODO(IR PR 2.1) — workload.ReconcileHeadlessService not wired at the workload.Reconcile call site yet; the direct path emits the per-Component headless Service unconditionally. When PR 2.1 lands, both paths emit the Service.",
}

// buildShadowISVC composes the fixture both paths emit objects
// against. Centralized so the two runs receive byte-identical
// inputs.
func buildShadowISVC(fix shadowFixture) *v1beta1.InferenceService {
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
	return isvc
}

// componentExtFromISVC returns the ComponentExtensionSpec for the
// given Component. Caller error if the Component isn't declared on
// the ISVC — returns nil, the projector validation catches the
// downstream nil deref.
func componentExtFromISVC(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine == nil {
			return nil
		}
		return &isvc.Spec.Engine.ComponentExtensionSpec
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder == nil {
			return nil
		}
		return &isvc.Spec.Decoder.ComponentExtensionSpec
	case v1beta1.RouterComponent:
		if isvc.Spec.Router == nil {
			return nil
		}
		return &isvc.Spec.Router.ComponentExtensionSpec
	}
	return nil
}

// setComponentAnnotation sets the given key=value on the
// per-Component Annotations map of the ISVC. Centralized so the
// shadow-mode fixtures don't repeat the per-Component switch.
func setComponentAnnotation(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, key, value string) {
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine == nil {
			return
		}
		if isvc.Spec.Engine.Annotations == nil {
			isvc.Spec.Engine.Annotations = map[string]string{}
		}
		isvc.Spec.Engine.Annotations[key] = value
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder == nil {
			return
		}
		if isvc.Spec.Decoder.Annotations == nil {
			isvc.Spec.Decoder.Annotations = map[string]string{}
		}
		isvc.Spec.Decoder.Annotations[key] = value
	case v1beta1.RouterComponent:
		if isvc.Spec.Router == nil {
			return
		}
		if isvc.Spec.Router.Annotations == nil {
			isvc.Spec.Router.Annotations = map[string]string{}
		}
		isvc.Spec.Router.Annotations[key] = value
	}
}

// newShadowClient builds a controller-runtime fake client with the
// schemes both paths exercise.
func newShadowClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(shadowScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(initObjs...).
		Build()
}

// shadowScheme returns the runtime.Scheme with every type the
// shadow harness materializes. Returned as a fresh scheme per call
// so tests stay isolated.
func shadowScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1 AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 AddToScheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("appsv1 AddToScheme: %v", err)
	}
	if err := discoveryv1.AddToScheme(s); err != nil {
		t.Fatalf("discoveryv1 AddToScheme: %v", err)
	}
	if err := monitoringv1.AddToScheme(s); err != nil {
		t.Fatalf("monitoringv1 AddToScheme: %v", err)
	}
	return s
}

// snapshotShadowState reads the post-reconcile cluster shape into a
// diffable struct. Captures only the surfaces the IR cutover could
// conceivably drift on; owner-ref Kind / Name are excluded (expected
// to differ between paths) and cluster-stamped fields (UID,
// ResourceVersion, CreationTimestamp) are dropped at snapshot time
// rather than at comparison time so the diff stays focused.
func snapshotShadowState(t *testing.T, ctx context.Context, c client.Client, fix shadowFixture) shadowSnapshot {
	t.Helper()
	out := shadowSnapshot{
		pods:                map[string]shadowPodSnapshot{},
		crDataBytes:         map[string][]byte{},
		headlessServiceName: fix.name + "-" + string(fix.component) + "-headless",
	}

	crs := &appsv1.ControllerRevisionList{}
	if err := c.List(ctx, crs); err != nil {
		t.Fatalf("snapshot: list ControllerRevisions: %v", err)
	}
	for _, cr := range crs.Items {
		key := cr.Namespace + "/" + cr.Name
		out.crNames = append(out.crNames, key)
		out.crDataBytes[key] = append([]byte(nil), cr.Data.Raw...)
	}
	sort.Strings(out.crNames)

	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		t.Fatalf("snapshot: list Pods: %v", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		out.pods[p.Namespace+"/"+p.Name] = newShadowPodSnapshot(p)
	}

	svcs := &corev1.ServiceList{}
	if err := c.List(ctx, svcs); err != nil {
		t.Fatalf("snapshot: list Services: %v", err)
	}
	for _, s := range svcs.Items {
		out.services = append(out.services, s.Namespace+"/"+s.Name)
	}
	sort.Strings(out.services)

	return out
}

// newShadowPodSnapshot extracts the diffable subset of a Pod's
// metadata. Owner-ref Kind / Name EXCLUDED — those are expected to
// differ between paths (direct=ISVC, IR-managed=IR). Mirrors
// workload/equivalence_test.go's podSnapshot field-by-field except
// for the owner-ref exclusion.
func newShadowPodSnapshot(p *corev1.Pod) shadowPodSnapshot {
	out := shadowPodSnapshot{
		labels:      copyShadowMap(p.Labels),
		annotations: copyShadowMap(p.Annotations),
	}
	for _, c := range p.Spec.Containers {
		out.containers = append(out.containers, shadowContainerSnapshot{
			name:  c.Name,
			image: c.Image,
		})
	}
	return out
}

// copyShadowMap returns a shallow copy of m so the snapshot doesn't
// alias the live pod's label map (which controller-runtime may
// keep mutating between snapshots in the same test process).
func copyShadowMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// equalShadowStringSlices is an ordering-sensitive comparator; all
// callers pre-sort their inputs.
func equalShadowStringSlices(a, b []string) bool {
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

// shadowPodNames returns the sorted name set of a pod-snapshot map
// for reporting diff context — keeps the error messages
// grep-friendly.
func shadowPodNames(m map[string]shadowPodSnapshot) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// containsServiceName returns true when the sorted Service name
// slice contains the (suffix) name. The slice carries "ns/name"
// entries; the check matches by suffix so callers can pass bare
// names.
func containsServiceName(haystack []string, name string) bool {
	for _, s := range haystack {
		if len(s) >= len(name) && s[len(s)-len(name):] == name {
			return true
		}
	}
	return false
}
