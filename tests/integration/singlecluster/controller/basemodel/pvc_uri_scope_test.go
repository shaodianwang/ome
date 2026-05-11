package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PVC reconcile path: URI handling and cross-scope behavior. Specs
// here exercise GetPVCMetadataConfigMapName scope encoding,
// long-subpath Job naming, and sticky condition behavior when the
// PVC simply never exists.
var _ = ginkgo.Describe("BaseModel PVC URI and scope handling", ginkgo.Ordered, func() {

	ginkgo.It("BM + CBM with same name targeting the same PVC: scope-isolated CMs and Jobs", func() {
		// The two share underlying storage but have distinct
		// per-PVC ConfigMap names (scope is part of the name) and
		// distinct ConfigMap keys (clusterbasemodel.name vs
		// {ns}.basemodel.name). Each Job carries its own owner-ref
		// and each CR's status flips Ready independently.
		ns := "pvc-bmcbm"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "shared-bmcbm", ns)
		bindPVC(suiteCtx, k8sClient, "shared-bmcbm", ns)

		bm := v1beta1testing.MakeBaseModel("bmcbm-shared", ns).
			StorageURI("pvc://shared-bmcbm/models/llama-bm").Obj()
		cbm := v1beta1testing.MakeClusterBaseModel("bmcbm-shared").
			StorageURI("pvc://" + ns + ":shared-bmcbm/models/llama-cbm").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("expecting two distinct Jobs in the PVC's namespace, one per scope")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(2))
			kinds := []string{
				jobs.Items[0].OwnerReferences[0].Kind,
				jobs.Items[1].OwnerReferences[0].Kind,
			}
			g.Expect(kinds).To(ConsistOf("BaseModel", "ClusterBaseModel"),
				"each Job must own up to its respective scope")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulate both agents Ready — each writes its own scope-named ConfigMap")
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"bmcbm-shared", ns, false, &modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"bmcbm-shared", ns, true, &modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())

		ginkgo.By("both CRs reach Ready independently and their per-PVC CMs have different names")
		Eventually(func(g Gomega) {
			gotBM := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "bmcbm-shared", Namespace: ns}, gotBM)).To(Succeed())
			g.Expect(gotBM.Status.State).To(Equal(v1beta1.LifeCycleStateReady))

			gotCBM := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "bmcbm-shared"}, gotCBM)).To(Succeed())
			g.Expect(gotCBM.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		bmCMName := constants.GetPVCMetadataConfigMapName("bmcbm-shared", ns, false)
		cbmCMName := constants.GetPVCMetadataConfigMapName("bmcbm-shared", ns, true)
		Expect(bmCMName).NotTo(Equal(cbmCMName),
			"BM and CBM with the same display name must produce different per-PVC CM names")
	})

	ginkgo.It("long nested PVC subpath: pvc://name/very/deep/path/model still resolves to a Job + CM", func() {
		// Verify the controller doesn't choke on long subpaths and
		// the Job naming hash absorbs the path into a DNS-1123 label.
		ns := "pvc-deep"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "deep-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "deep-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("deep-llama", ns).
			StorageURI("pvc://deep-pvc/very/deep/nested/path/to/the/model/llama-13b").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("Job is created with a DNS-1123-compliant name (≤63 chars)")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			g.Expect(len(jobs.Items[0].Name)).To(BeNumerically("<=", 63),
				"Job name must fit DNS-1123 label limit even with long subpath")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"deep-llama", ns, false, &modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())

		ginkgo.By("agent Ready propagates through the long-path Job")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "deep-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PVC never created → SourceReachable=PVCNotFound stays sticky (no flapping)", func() {
		// pvc_advanced_test.go's "BM created before PVC" covers the
		// transient case (PVC arrives later). This variant proves
		// the condition stays put when the PVC simply never exists.
		ns := "pvc-never"
		makeNamespace(suiteCtx, k8sClient, ns)

		bm := v1beta1testing.MakeBaseModel("never-llama", ns).
			StorageURI("pvc://never-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "never-llama", Namespace: ns}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotFound))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Stays stuck — Consistently confirms no wandering to a
		// different reason without intervention.
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "never-llama", Namespace: ns}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotFound))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
