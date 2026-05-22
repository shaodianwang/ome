package omenative

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// newFakeClientWithApps builds a fake controller-runtime client carrying
// the schemes revision.go reads (corev1 + v1beta1 + appsv1 for
// ControllerRevision).
func newFakeClientWithApps(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initObjs...).
		Build()
}

func basicPodSpecForRevision() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: "llama:v1"},
		},
	}
}

func TestRevisionHash_DeterministicForSameInput(t *testing.T) {
	ps := basicPodSpecForRevision()
	h1, raw1, err := revisionHash(ps, nil, nil)
	if err != nil {
		t.Fatalf("hash 1: %v", err)
	}
	h2, raw2, err := revisionHash(ps, nil, nil)
	if err != nil {
		t.Fatalf("hash 2: %v", err)
	}
	if h1 != h2 {
		t.Errorf("hash differs across calls: %q vs %q", h1, h2)
	}
	if string(raw1) != string(raw2) {
		t.Errorf("raw bytes differ across calls")
	}
}

func TestRevisionHash_CollisionCountChangesHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	h0, _, _ := revisionHash(ps, nil, nil)
	h1, _, _ := revisionHash(ps, nil, ptr.To(int32(1)))
	h2, _, _ := revisionHash(ps, nil, ptr.To(int32(2)))
	if h0 == h1 || h0 == h2 || h1 == h2 {
		t.Errorf("collision-count salt should change the hash: got %q, %q, %q", h0, h1, h2)
	}
}

func TestRevisionHash_CanonicalPayloadIncludesPodMetaField(t *testing.T) {
	// PodMeta is in the canonical payload as a nil pointer for v1; the
	// JSON must carry the "podMeta":null tail so a future PR that
	// populates PodMeta doesn't invalidate existing revisions just by
	// adding the field. If this test breaks because the JSON no longer
	// contains "podMeta", the wrapper struct lost forward-compat.
	ps := basicPodSpecForRevision()
	_, raw, err := revisionHash(ps, nil, nil)
	if err != nil {
		t.Fatalf("revisionHash: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"podMeta":null`)) {
		t.Errorf("canonical JSON should carry podMeta:null; got %s", raw)
	}
}

func TestRevisionHash_NilAndZeroPointerEquivalent(t *testing.T) {
	// CollisionCount in OMENativeComponentStatus is *int32. Callers may
	// legitimately pass nil or ptr.To(int32(0)) for "no salt yet"; the
	// hash must be the same or a defaulter that normalizes the pointer
	// would invalidate every previously-recorded revision name.
	ps := basicPodSpecForRevision()
	hNil, _, _ := revisionHash(ps, nil, nil)
	hZero, _, _ := revisionHash(ps, nil, ptr.To(int32(0)))
	if hNil != hZero {
		t.Errorf("nil and *0 must hash identically: got %q vs %q", hNil, hZero)
	}
}

func TestRevisionHash_DifferentTemplateDifferentHash(t *testing.T) {
	a := basicPodSpecForRevision()
	b := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	ha, _, _ := revisionHash(a, nil, nil)
	hb, _, _ := revisionHash(b, nil, nil)
	if ha == hb {
		t.Errorf("different templates produced same hash: %q", ha)
	}
}

func TestRevisionHash_NilTemplateRejected(t *testing.T) {
	if _, _, err := revisionHash(nil, nil, nil); err == nil {
		t.Fatal("expected error for nil template")
	}
}

func TestEnsureControllerRevision_CreatesNew(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)

	cr, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, basicPodSpecForRevision(), nil, nil)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if collision {
		t.Errorf("first-create should not report collision")
	}
	if cr == nil {
		t.Fatal("nil CR returned")
	}
	if cr.Revision != 1 {
		t.Errorf("Revision: got %d want 1", cr.Revision)
	}

	// Re-read to confirm it landed in the fake API.
	got := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if got.Labels[labelManagedBy] != managedByOMENative {
		t.Errorf("managed-by label missing: %+v", got.Labels)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != "llama-70b" {
		t.Errorf("owner ref not set to ISVC: %+v", got.OwnerReferences)
	}
}

func TestEnsureControllerRevision_ReusesExistingMatchingData(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)

	first, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, basicPodSpecForRevision(), nil, nil)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, basicPodSpecForRevision(), nil, nil)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if collision {
		t.Errorf("identical inputs should not report collision")
	}
	if second.Name != first.Name {
		t.Errorf("Name: got %q want %q (reused)", second.Name, first.Name)
	}
	if second.Revision != first.Revision {
		t.Errorf("Revision should be reused, got %d vs %d", second.Revision, first.Revision)
	}

	// Only one CR should exist.
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 1 {
		t.Errorf("CR count: got %d want 1", len(list.Items))
	}
}

func TestEnsureControllerRevision_DetectsCollision(t *testing.T) {
	// Pre-seed a CR whose Name matches what EnsureControllerRevision would
	// compute, but whose Data deliberately differs. This simulates a hash
	// collision (extremely rare but worth handling).
	isvc := minimalISVC("llama-70b", "prod", 1)
	want := basicPodSpecForRevision()
	hash, _, _ := revisionHash(want, nil, nil)
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revisionName(isvc.Name, v1beta1.EngineComponent, hash),
			Namespace: isvc.Namespace,
			Labels:    revisionLabels(isvc.Name, v1beta1.EngineComponent),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]}}`)},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, want, nil, nil)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if !collision {
		t.Fatalf("expected collision=true when existing CR has same name but different data")
	}
}

func TestEnsureControllerRevision_CollisionCountSaltRetryLands(t *testing.T) {
	// Simulate the recovery loop the caller would run: on collision,
	// bump CollisionCount and retry; the second call should produce a
	// different name and succeed without collision.
	isvc := minimalISVC("llama-70b", "prod", 1)
	want := basicPodSpecForRevision()
	hash0, _, _ := revisionHash(want, nil, nil)
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revisionName(isvc.Name, v1beta1.EngineComponent, hash0),
			Namespace: isvc.Namespace,
			Labels:    revisionLabels(isvc.Name, v1beta1.EngineComponent),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]}}`)},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, want, nil, nil)
	if err != nil || !collision {
		t.Fatalf("expected collision on first call: err=%v collision=%v", err, collision)
	}
	cr2, collision2, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, want, nil, ptr.To(int32(1)))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if collision2 {
		t.Fatalf("retry with bumped collisionCount should not collide")
	}
	if cr2.Name == pre.Name {
		t.Fatalf("retry name should differ from colliding one")
	}
	if cr2.Revision != 2 {
		t.Errorf("Revision: got %d want 2 (max+1)", cr2.Revision)
	}
}

func TestEnsureControllerRevision_NilGuards(t *testing.T) {
	c := newFakeClientWithApps(t)
	isvc := minimalISVC("x", "y", 1)
	template := basicPodSpecForRevision()
	if _, _, err := EnsureControllerRevision(context.Background(), c, c, nil, v1beta1.EngineComponent, template, nil, nil); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
	if _, _, err := EnsureControllerRevision(context.Background(), nil, c, isvc, v1beta1.EngineComponent, template, nil, nil); err == nil {
		t.Fatal("expected error for nil client")
	}
	if _, _, err := EnsureControllerRevision(context.Background(), c, nil, isvc, v1beta1.EngineComponent, template, nil, nil); err == nil {
		t.Fatal("expected error for nil reader")
	}
}

func TestEnsureControllerRevision_RacePathCreateAlreadyExists(t *testing.T) {
	// Simulate two reconciles deciding to create at the same time: the
	// initial Get sees NotFound, the Create returns AlreadyExists (because
	// a concurrent writer landed first). EnsureControllerRevision must
	// re-Get and apply the collision/match check on the freshly-found CR.
	//
	// This is the branch unreachable from the pre-seed collision test —
	// that one exercises the existing-on-Get path, this one exercises the
	// existing-after-Create path.
	isvc := minimalISVC("llama-70b", "prod", 1)
	template := basicPodSpecForRevision()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	// Pre-seed a CR with the same name EnsureControllerRevision will
	// compute, but with mismatching Data. The interceptor below makes
	// the initial Get pretend it doesn't exist, so the function falls
	// through to Create, which the underlying fake will then reject
	// with AlreadyExists because the object IS actually there.
	hash, _, _ := revisionHash(template, nil, nil)
	conflicting := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revisionName(isvc.Name, v1beta1.EngineComponent, hash),
			Namespace: isvc.Namespace,
			Labels:    revisionLabels(isvc.Name, v1beta1.EngineComponent),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]},"podMeta":null}`)},
		Revision: 1,
	}

	hideOnceOnGet := true
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, conflicting).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.ControllerRevision); ok && key.Name == conflicting.Name && hideOnceOnGet {
					hideOnceOnGet = false
					return apierrors.NewNotFound(appsv1.Resource("controllerrevisions"), key.Name)
				}
				return cli.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	cr, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, template, nil, nil)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if !collision {
		t.Fatalf("expected collision=true: Create raced with a conflicting writer")
	}
	if cr == nil {
		t.Fatalf("expected the conflicting CR to be returned for the caller to inspect")
	}
}

func TestRetainControllerRevisions_BelowMaxIsNoOp(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	// Two revisions, both non-live; maxNonLive=10 keeps everything.
	for i, image := range []string{"a", "b"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, ps, nil, nil); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if err := RetainControllerRevisions(context.Background(), c, c, isvc, v1beta1.EngineComponent, 10); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 2 {
		t.Errorf("CRs: got %d want 2 (nothing should be deleted)", len(list.Items))
	}
}

func TestRetainControllerRevisions_DropsOldestNonLive(t *testing.T) {
	// Seed 5 distinct revisions, maxNonLive=2 → keep the 2 newest, drop 3.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	for _, image := range []string{"v1", "v2", "v3", "v4", "v5"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, ps, nil, nil); err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
	}
	if err := RetainControllerRevisions(context.Background(), c, c, isvc, v1beta1.EngineComponent, 2); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 2 {
		t.Errorf("CRs after retain: got %d want 2", len(list.Items))
	}
	// The two survivors must have the highest revision numbers (4 and 5).
	keptRevs := map[int64]bool{}
	for _, cr := range list.Items {
		keptRevs[cr.Revision] = true
	}
	if !keptRevs[4] || !keptRevs[5] {
		t.Errorf("expected survivors to be Revision 4 and 5, got %v", keptRevs)
	}
}

func TestRetainControllerRevisions_NeverDeletesLiveRevisions(t *testing.T) {
	// Seed 4 revisions, maxNonLive=1, mark v1 as live → it must survive
	// even though it's the oldest non-live by revision number.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	names := make(map[string]string, 4) // image -> CR name
	for _, image := range []string{"v1", "v2", "v3", "v4"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		cr, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, ps, nil, nil)
		if err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
		names[image] = cr.Name
	}
	liveName := names["v1"] // oldest, but pinned as live
	if err := RetainControllerRevisions(context.Background(), c, c, isvc, v1beta1.EngineComponent, 1, liveName); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	// 1 live + 1 non-live kept by maxNonLive = 2 survivors.
	if len(list.Items) != 2 {
		t.Errorf("CRs: got %d want 2 (live + newest non-live)", len(list.Items))
	}
	keptNames := map[string]bool{}
	for _, cr := range list.Items {
		keptNames[cr.Name] = true
	}
	if !keptNames[liveName] {
		t.Errorf("live revision %s was deleted", liveName)
	}
}

func TestRetainControllerRevisions_NegativeMaxRejected(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	if err := RetainControllerRevisions(context.Background(), c, c, isvc, v1beta1.EngineComponent, -1); err == nil {
		t.Fatal("expected error for negative maxNonLive")
	}
}

func TestRetainControllerRevisions_ContinuesAfterDeleteError(t *testing.T) {
	// Seed 4 CRs (Revisions 1..4), inject a Delete that fails on the
	// oldest. With maxNonLive=1 the function should still trim the
	// middle non-live CRs (Revisions 2 and 3), report an aggregated
	// error for the failure, and leave the freshest one intact.
	isvc := minimalISVC("llama-70b", "prod", 1)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	failName := revisionName(isvc.Name, v1beta1.EngineComponent, "")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cli client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if cr, ok := obj.(*appsv1.ControllerRevision); ok && cr.Revision == 1 {
					return fmt.Errorf("synthetic transient delete failure on %s", cr.Name)
				}
				return cli.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	_ = failName // silence linter; reserved for future asserts that pin the failing name

	for _, image := range []string{"v1", "v2", "v3", "v4"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, v1beta1.EngineComponent, ps, nil, nil); err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
	}

	err := RetainControllerRevisions(context.Background(), c, c, isvc, v1beta1.EngineComponent, 1)
	if err == nil {
		t.Fatal("expected aggregated error from the failing delete")
	}
	if !strings.Contains(err.Error(), "synthetic transient delete failure") {
		t.Errorf("error should wrap the underlying delete failure: %v", err)
	}

	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	// Expected survivors: Revision 4 (the kept non-live one) plus the
	// Revision 1 the synthetic delete refused to remove.
	gotRevs := map[int64]bool{}
	for _, cr := range list.Items {
		gotRevs[cr.Revision] = true
	}
	if !gotRevs[4] {
		t.Errorf("Revision 4 (newest non-live) should have survived, got %v", gotRevs)
	}
	if !gotRevs[1] {
		t.Errorf("Revision 1 (failed-to-delete) should still be present, got %v", gotRevs)
	}
	if gotRevs[2] || gotRevs[3] {
		t.Errorf("middle non-live Revisions 2 and 3 should have been deleted, got %v", gotRevs)
	}
}

func TestNextRevisionNumber_NoExisting(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	n, err := nextRevisionNumber(context.Background(), c, isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("nextRevisionNumber: %v", err)
	}
	if n != 1 {
		t.Errorf("first revision: got %d want 1", n)
	}
}
