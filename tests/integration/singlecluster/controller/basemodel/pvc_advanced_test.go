package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Edge-case PVC reconcile specs that go beyond the happy/sad paths in
// pvc_test.go.
var _ = ginkgo.Describe("BaseModel PVC edge cases", ginkgo.Ordered, func() {

	ginkgo.It("two BaseModels sharing one PVC get separate Jobs and per-PVC ConfigMaps", func() {
		ns := "shared-pvc-multi"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "shared", ns)
		bindPVC(suiteCtx, k8sClient, "shared", ns)

		ginkgo.By("creating two BaseModels with different URIs against the same PVC")
		bm1 := v1beta1testing.MakeBaseModel("multi-llama-7b", ns).
			StorageURI("pvc://shared/models/llama-7b").Obj()
		bm2 := v1beta1testing.MakeBaseModel("multi-llama-13b", ns).
			StorageURI("pvc://shared/models/llama-13b").Obj()
		Expect(k8sClient.Create(suiteCtx, bm1)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, bm2)).To(Succeed())

		ginkgo.By("expecting two distinct metadata Jobs (URI hash differs)")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(2))
			names := map[string]bool{jobs.Items[0].Name: true, jobs.Items[1].Name: true}
			g.Expect(names).To(HaveLen(2), "Job names must be distinct (deterministic per (model,uri))")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulating both agents writing Ready ConfigMaps — one per (model,namespace)")
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "multi-llama-7b", ns, false,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "multi-llama-13b", ns, false,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())

		ginkgo.By("expecting both BaseModels to flip Ready independently")
		Eventually(func(g Gomega) {
			for _, name := range []string{"multi-llama-7b", "multi-llama-13b"} {
				got := &v1beta1.BaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name, Namespace: ns}, got)).To(Succeed())
				g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady), "model %q should be Ready", name)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("two distinct per-PVC ConfigMaps exist (deterministic name from model+ns)")
		cm7 := &corev1.ConfigMap{}
		cm13 := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: constants.OMENamespace,
			Name:      constants.GetPVCMetadataConfigMapName("multi-llama-7b", ns, false),
		}, cm7)).To(Succeed())
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: constants.OMENamespace,
			Name:      constants.GetPVCMetadataConfigMapName("multi-llama-13b", ns, false),
		}, cm13)).To(Succeed())
		Expect(cm7.Name).NotTo(Equal(cm13.Name), "per-PVC ConfigMap names must differ between sibling models")

		ginkgo.By("deleting one BaseModel cleans up only its own ConfigMap; the other stays Ready")
		Expect(k8sClient.Delete(suiteCtx, bm1)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: constants.OMENamespace,
				Name:      constants.GetPVCMetadataConfigMapName("multi-llama-7b", ns, false),
			}, &corev1.ConfigMap{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "deleted BM's ConfigMap must be gone")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		got13 := &v1beta1.BaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "multi-llama-13b", Namespace: ns}, got13)).To(Succeed())
		Expect(got13.Status.State).To(Equal(v1beta1.LifeCycleStateReady), "the surviving BM must remain Ready")

		_ = k8sClient.Delete(suiteCtx, got13)
	})

	ginkgo.It("BaseModel created before PVC exists → SourceReachable=PVCNotFound, then PVC create re-enqueues", func() {
		ns := "pvc-arrives-late"
		makeNamespace(suiteCtx, k8sClient, ns)

		ginkgo.By("creating the BaseModel BEFORE the PVC")
		bm := v1beta1testing.MakeBaseModel("late-llama", ns).
			StorageURI("pvc://late-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("expecting SourceReachable=PVCNotFound")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "late-llama", Namespace: ns}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotFound))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("now creating + binding the PVC — PVC create-watch must re-enqueue the BaseModel")
		makePVC(suiteCtx, k8sClient, "late-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "late-pvc", ns)

		ginkgo.By("expecting the metadata Job to appear (controller transitioned past PVCNotFound)")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1), "PVC arrival should have re-enqueued the BM, producing a Job")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		_ = k8sClient.Delete(suiteCtx, bm)
	})

	ginkgo.It("storage URI swap oci:// → pvc:// switches to PVC branch and reaches Ready", func() {
		ns := "uri-swap-oci-to-pvc"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "swap-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "swap-pvc", ns)

		ginkgo.By("creating the BM with an oci:// URI (per-node flow; no nodes will report ready)")
		bm := v1beta1testing.MakeBaseModel("swap-llama", ns).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		ginkgo.By("waiting for the per-node reconcile to fire (proxy: finalizer added)")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("swapping URI to pvc:// — controller must enter the PVC branch on next reconcile")
		got := &v1beta1.BaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-llama", Namespace: ns}, got)).To(Succeed())
		got.Spec.Storage.StorageUri = ptr.To("pvc://swap-pvc/models/llama")
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		ginkgo.By("expecting a metadata Job to appear (PVC branch active)")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1), "PVC branch must spawn a Job after URI swap")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulating Ready and expecting state→Ready via the new branch")
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "swap-llama", ns, false,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Eventually(func(g Gomega) {
			latest := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-llama", Namespace: ns}, latest)).To(Succeed())
			g.Expect(latest.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("storage URI swap pvc:// → oci:// cleans up PVC artifacts (Job, ConfigMap, stale conditions)", func() {
		ns := "uri-swap-pvc-out"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("swap-out-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		ginkgo.By("waiting for the PVC-mode Job to appear and the agent to report Ready")
		var jobName string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			jobName = jobs.Items[0].Name
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "swap-out-llama", ns, false,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-out-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		cmKey := types.NamespacedName{
			Namespace: constants.OMENamespace,
			Name:      constants.GetPVCMetadataConfigMapName("swap-out-llama", ns, false),
		}

		ginkgo.By("swapping URI to oci:// — controller must clean up PVC artifacts")
		got := &v1beta1.BaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-out-llama", Namespace: ns}, got)).To(Succeed())
		got.Spec.Storage.StorageUri = ptr.To("oci://n/test/b/models/o/llama")
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		ginkgo.By("metadata Job from the old PVC URI must be deleted")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: jobName, Namespace: ns}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "orphaned PVC Job must be cleaned up after URI swap")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("per-PVC ConfigMap from the old PVC URI must be deleted")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, cmKey, &corev1.ConfigMap{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "orphaned per-PVC ConfigMap must be cleaned up after URI swap")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("PVC-specific conditions on the CR must be cleared (no stale PVCMetadataExtracting / PVCMetadataReady)")
		Eventually(func(g Gomega) {
			latest := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "swap-out-llama", Namespace: ns}, latest)).To(Succeed())
			pvcReasons := map[string]bool{
				v1beta1.ModelConditionReasonPVCMetadataExtracting:       true,
				v1beta1.ModelConditionReasonPVCMetadataReady:            true,
				v1beta1.ModelConditionReasonPVCMetadataExtractionFailed: true,
				v1beta1.ModelConditionReasonPVCNotBound:                 true,
				v1beta1.ModelConditionReasonPVCNotFound:                 true,
				v1beta1.ModelConditionReasonPVCValidated:                true,
				v1beta1.ModelConditionReasonPVCInvalid:                  true,
				v1beta1.ModelConditionReasonPVCConfigMissing:            true,
			}
			for _, c := range latest.Status.Conditions {
				g.Expect(pvcReasons[c.Reason]).To(BeFalse(),
					"condition %q has stale PVC reason %q after URI swap to non-PVC", c.Type, c.Reason)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ConfigMap with bad ModelEntry JSON → controller stays InTransit (doesn't crash)", func() {
		ns := "bad-json"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("badjson-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		ginkgo.By("waiting for finalizer (proxy: reconciler ran at least once)")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "badjson-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a per-PVC ConfigMap with malformed JSON in the model entry")
		cmName := constants.GetPVCMetadataConfigMapName("badjson-llama", ns, false)
		modelKey := constants.GetModelConfigMapKey(ns, "badjson-llama", false)
		badCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: constants.OMENamespace,
				Labels: map[string]string{
					constants.PVCStorageConfigMapLabel:  "true",
					constants.PVCMetadataModelNameLabel: "badjson-llama",
					constants.PVCMetadataScopeLabel:     "namespaced",
				},
			},
			Data: map[string]string{modelKey: "{not valid json"},
		}
		Expect(k8sClient.Create(suiteCtx, badCM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, badCM) })

		ginkgo.By("expecting controller to stay InTransit with reason=PVCMetadataExtracting (NOT crash, NOT Ready)")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "badjson-llama", Namespace: ns}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCMetadataExtracting))
			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ConfigMap exists but missing the model-key entry → InTransit", func() {
		ns := "missing-entry"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("noentry-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "noentry-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a ConfigMap with no entry under the per-model key")
		cmName := constants.GetPVCMetadataConfigMapName("noentry-llama", ns, false)
		emptyCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: constants.OMENamespace,
				Labels: map[string]string{
					constants.PVCStorageConfigMapLabel:  "true",
					constants.PVCMetadataModelNameLabel: "noentry-llama",
					constants.PVCMetadataScopeLabel:     "namespaced",
				},
			},
			Data: map[string]string{"unrelated-key": "{}"},
		}
		Expect(k8sClient.Create(suiteCtx, emptyCM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, emptyCM) })

		ginkgo.By("expecting InTransit + Ready=False with PVCMetadataExtracting (controller logged + waited)")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "noentry-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
