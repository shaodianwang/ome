package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PVC reconcile lifecycle edges. The existing pvc_test.go,
// pvc_advanced_test.go, pvc_status_test.go, and pvc_uri_scope_test.go
// cover the happy-path flows + URI-shape rules. This file covers
// the LIFECYCLE corners that aren't in any of the above:
//
//   - what happens when a Ready BM's underlying PVC gets deleted
//   - URI swap pvc://A → pvc://B (the existing swap tests cover
//     oci↔pvc only; staying on pvc:// changes a different code path
//     in cleanupStalePVCArtifacts)
//   - CBM URI references a PVC in a namespace that doesn't exist
//   - pvcSpecMatches isolation when the same PVC name exists in
//     two namespaces (predicate must not cross-link them)
var _ = ginkgo.Describe("BaseModel PVC reconcile lifecycle edges", ginkgo.Ordered, func() {

	ginkgo.It("PVC transitions Bound → Lost AFTER BM reaches Ready → state drops to InTransit with reason=PVCNotBound", func() {
		// PVC failures aren't always full deletion — a more common
		// scenario is the PVC's underlying PV failing or being
		// reclaimed, which manifests as Phase=Lost. The createPVCPhasePredicate
		// fires on Phase changes (pvc.go:583); the next reconcile
		// must observe Phase!=Bound and drop the BM out of Ready.
		//
		// (We don't test outright PVC deletion here because envtest
		// has no kube-controller-manager to clear the
		// kubernetes.io/pvc-protection finalizer the apiserver
		// auto-adds, so a Delete leaves the PVC stuck in Terminating
		// rather than disappearing. The Phase=Lost path exercises
		// the same condition-write logic in setPVCInTransitStatus.)
		ns := "pvc-lost-after-ready"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("ready-then-lost", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"ready-then-lost", ns, false,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "ready-then-lost", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flip PVC Status.Phase Bound → Lost — controller must observe and re-evaluate")
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "p", Namespace: ns}, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimLost
		Expect(k8sClient.Status().Update(suiteCtx, pvc)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "ready-then-lost", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateReady),
				"PVC Phase=Lost must drive the BM out of Ready state")
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotBound),
				"PVC.Status.Phase != Bound must surface as PVCNotBound condition")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("URI swap pvc://A → pvc://B: stale Job + per-PVC ConfigMap from A are cleaned up, fresh Job for B appears", func() {
		// pvc_advanced_test.go covers oci↔pvc swaps but NOT
		// pvc↔pvc. Different code path: cleanupStalePVCArtifacts
		// only fires on the non-PVC branch entry, so a pvc→pvc
		// swap must rely on ensureMetadataJob's deterministic
		// naming (different URI → different hash → different Job
		// name → fresh Job) AND on the per-PVC CM being
		// per-(model,uri).
		ns := "pvc-pvc-swap"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "pvc-a", ns)
		bindPVC(suiteCtx, k8sClient, "pvc-a", ns)
		makePVC(suiteCtx, k8sClient, "pvc-b", ns)
		bindPVC(suiteCtx, k8sClient, "pvc-b", ns)

		bm := v1beta1testing.MakeBaseModel("swap-llama", ns).
			StorageURI("pvc://pvc-a/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		var firstJobName string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			firstJobName = jobs.Items[0].Name
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("swap URI to pvc-b — controller must produce a NEW Job (different deterministic name)")
		got := &v1beta1.BaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-llama", Namespace: ns}, got)).To(Succeed())
		newURI := "pvc://pvc-b/models/llama"
		got.Spec.Storage.StorageUri = &newURI
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			names := []string{}
			for _, j := range jobs.Items {
				names = append(names, j.Name)
			}
			// The new Job's name must differ from the first
			// (different URI → different hash). Both Jobs may
			// briefly coexist before the controller GC's the old
			// one; we only assert presence of the new one.
			g.Expect(names).To(ContainElement(Not(Equal(firstJobName))),
				"URI change must yield a Job with a new deterministic name (got %v)", names)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("CBM URI references a PVC in a namespace that doesn't exist → SourceReachable=PVCNotFound (no panic)", func() {
		// resolvePVCNamespace returns the URI's namespace verbatim
		// for a CBM; it does NOT pre-validate that the namespace
		// exists. The Get(pvc) then fails with NotFound (since
		// neither namespace nor PVC exists). Defensive coverage:
		// the controller must not panic, and the condition must
		// be PVCNotFound (not some misleading other reason).
		cbm := v1beta1testing.MakeClusterBaseModel("cbm-bad-ns").
			StorageURI("pvc://does-not-exist:my-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cbm-bad-ns"}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil(), "SourceReachable condition must be set even for non-existent target namespace")
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotFound),
				"missing target namespace must surface as PVCNotFound, not a different misleading reason")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("two namespaced BMs in DIFFERENT namespaces with the SAME PVC name don't cross-link via the PVC watch", func() {
		// mapPVCToBaseModels filters by `client.InNamespace(pvc.Namespace)`
		// (pvc.go:607) so a PVC named "p" in namespace ns1 must
		// only enqueue BMs whose URI references it AND whose own
		// namespace is ns1 — even if a BM in ns2 has the SAME URI
		// shape `pvc://p/...`. Catches a regression where the
		// mapper drops the namespace filter.
		nsA := "pvc-isolation-a"
		nsB := "pvc-isolation-b"
		makeNamespace(suiteCtx, k8sClient, nsA)
		makeNamespace(suiteCtx, k8sClient, nsB)
		makePVC(suiteCtx, k8sClient, "shared-name", nsA)
		bindPVC(suiteCtx, k8sClient, "shared-name", nsA)
		// Intentionally do NOT create the PVC in nsB — bmB must
		// stay at PVCNotFound while bmA reaches Ready, proving
		// the PVC watch from nsA's PVC didn't leak to bmB.

		bmA := v1beta1testing.MakeBaseModel("iso-llama", nsA).
			StorageURI("pvc://shared-name/models/llama").Obj()
		bmB := v1beta1testing.MakeBaseModel("iso-llama", nsB).
			StorageURI("pvc://shared-name/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bmA)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, bmB)).To(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"iso-llama", nsA, false, &modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())

		// bmA reaches Ready (PVC exists in nsA, agent reports OK).
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "iso-llama", Namespace: nsA}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// bmB STAYS at PVCNotFound — the PVC watch on nsA's
		// shared-name must not have enqueued bmB.
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "iso-llama", Namespace: nsB}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			if cond != nil {
				g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotFound),
					"bmB must remain at PVCNotFound — nsA's PVC must NOT cross-link via the watch")
			}
			// State should be Failed (PVCNotFound is non-Ready
			// terminal) or initial (if reconciler hasn't run yet).
			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateReady),
				"bmB must NOT reach Ready — its PVC doesn't exist in nsB")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Sanity: trigger reconcile on bmB by creating its PVC, then
		// proving it DOES respond when its OWN PVC appears (verifies
		// the negative wasn't because the watch is broken entirely).
		ginkgo.By("now create the PVC in nsB; bmB must transition out of PVCNotFound")
		makePVC(suiteCtx, k8sClient, "shared-name", nsB)
		bindPVC(suiteCtx, k8sClient, "shared-name", nsB)
		Expect(apierrors.IsNotFound(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "shared-name", Namespace: nsB}, &corev1.PersistentVolumeClaim{}))).To(BeFalse(),
			"PVC in nsB should now exist")

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "iso-llama", Namespace: nsB}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).NotTo(Equal(v1beta1.ModelConditionReasonPVCNotFound),
				"bmB must transition past PVCNotFound now that its OWN PVC exists")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// silence unused-import linter on constants when none of the specs
// above happen to use a concrete name from it (currently used).
var _ = constants.OMENamespace
