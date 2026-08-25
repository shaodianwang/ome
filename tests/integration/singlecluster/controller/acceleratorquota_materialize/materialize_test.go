package acceleratorquota_materialize

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	kueuebackend "github.com/ome-projects/ome/pkg/quota/backend/kueue"
)

// getCQ fetches a rendered ClusterQueue once it exists.
func getCQ(name string) *kueuev1beta2.ClusterQueue {
	ginkgo.GinkgoHelper()
	var cq kueuev1beta2.ClusterQueue
	gomega.Eventually(func() error {
		return k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, &cq)
	}, timeout, interval).Should(gomega.Succeed())
	return &cq
}

var _ = ginkgo.Describe("AcceleratorQuota materialization", func() {

	ginkgo.It("renders a tree Kueue accepts and marks active", func() {
		root := uniqueName("root-active")
		leaf := uniqueName("leaf-active")
		ns := uniqueName("ns-active")
		mustCreateNamespace(ns)

		// The reserved root cannot be renamed, so a suite that needs a parent
		// cohort authors one under it rather than instead of it.
		mustCreateQuota(cohortNode(v1beta1.AcceleratorQuotaRootName, ""))
		mustCreateQuota(cohortNode(root, v1beta1.AcceleratorQuotaRootName))
		mustCreateQuota(leafNode(leaf, root, []string{ns}, gpuFlavor, "8"))

		cq := getCQ(leaf)

		// Active is Kueue's own verdict that this queue can admit. It is the
		// single assertion no unit test can make.
		gomega.Eventually(func() metav1.ConditionStatus {
			var got kueuev1beta2.ClusterQueue
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &got); err != nil {
				return metav1.ConditionUnknown
			}
			c := apimeta.FindStatusCondition(got.Status.Conditions, kueuev1beta2.ClusterQueueActive)
			if c == nil {
				return metav1.ConditionUnknown
			}
			return c.Status
		}, timeout, interval).Should(gomega.Equal(metav1.ConditionTrue),
			"Kueue did not mark the rendered ClusterQueue active")

		gomega.Expect(string(cq.Spec.CohortName)).To(gomega.Equal(root))

		// Exactly one resource group, funding the cover and the accelerator
		// together. Kueue scopes resource and flavor uniqueness across all
		// groups, so any split leaves flavors that cannot serve a pod asking
		// for cpu.
		gomega.Expect(cq.Spec.ResourceGroups).To(gomega.HaveLen(1))
		covered := cq.Spec.ResourceGroups[0].CoveredResources
		gomega.Expect(covered).To(gomega.ConsistOf(
			corev1.ResourceName("cpu"), corev1.ResourceName("memory"),
			corev1.ResourceName(gpuResource)))

		// A null namespaceSelector admits nothing, silently, so its presence is
		// as load-bearing as the quota itself.
		gomega.Expect(cq.Spec.NamespaceSelector).NotTo(gomega.BeNil())

		// One LocalQueue per bound namespace, under Kueue's own default name so
		// Kueue's defaulting stamps workloads onto it.
		var lq kueuev1beta2.LocalQueue
		gomega.Eventually(func() error {
			return k8sClient.Get(suiteCtx,
				client.ObjectKey{Name: kueuebackend.LocalQueueName, Namespace: ns}, &lq)
		}, timeout, interval).Should(gomega.Succeed())
		gomega.Expect(string(lq.Spec.ClusterQueue)).To(gomega.Equal(leaf))

		// The intermediate node is pure topology.
		var cohort kueuev1beta2.Cohort
		gomega.Eventually(func() error {
			return k8sClient.Get(suiteCtx, client.ObjectKey{Name: root}, &cohort)
		}, timeout, interval).Should(gomega.Succeed())
		gomega.Expect(string(cohort.Spec.ParentName)).To(gomega.Equal(v1beta1.AcceleratorQuotaRootName))
		gomega.Expect(cohort.Spec.ResourceGroups).To(gomega.BeEmpty())
	})

	ginkgo.It("marks the node Materialized once its objects are written", func() {
		leaf := uniqueName("leaf-cond")
		ns := uniqueName("ns-cond")
		mustCreateNamespace(ns)

		mustCreateQuota(cohortNode(v1beta1.AcceleratorQuotaRootName, ""))
		mustCreateQuota(leafNode(leaf, v1beta1.AcceleratorQuotaRootName, []string{ns}, gpuFlavor, "4"))

		gomega.Eventually(func() metav1.ConditionStatus {
			var q v1beta1.AcceleratorQuota
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &q); err != nil {
				return metav1.ConditionUnknown
			}
			c := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaMaterialized)
			if c == nil {
				return metav1.ConditionUnknown
			}
			return c.Status
		}, timeout, interval).Should(gomega.Equal(metav1.ConditionTrue))
	})

	ginkgo.It("reports a budget naming an absent flavor instead of rendering it inactive", func() {
		leaf := uniqueName("leaf-flavor")
		ns := uniqueName("ns-flavor")
		mustCreateNamespace(ns)

		mustCreateQuota(cohortNode(v1beta1.AcceleratorQuotaRootName, ""))
		mustCreateQuota(leafNode(leaf, v1beta1.AcceleratorQuotaRootName, []string{ns}, "no-such-flavor", "8"))

		// FlavorMissing rather than a queue Kueue silently deactivates: an
		// inactive queue admits nothing and explains itself only in Kueue's
		// status, where nobody looking at the AcceleratorQuota would find it.
		gomega.Eventually(func() string {
			var q v1beta1.AcceleratorQuota
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &q); err != nil {
				return ""
			}
			c := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaMaterialized)
			if c == nil {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonFlavorMissing))
	})

	ginkgo.It("refuses to adopt a ClusterQueue it does not own", func() {
		leaf := uniqueName("leaf-conflict")
		ns := uniqueName("ns-conflict")
		mustCreateNamespace(ns)

		// A queue an operator authored by hand, carrying no ownership label.
		foreign := &kueuev1beta2.ClusterQueue{
			ObjectMeta: metav1.ObjectMeta{Name: leaf},
			Spec: kueuev1beta2.ClusterQueueSpec{
				NamespaceSelector: &metav1.LabelSelector{},
			},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, foreign)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, foreign)
		})

		mustCreateQuota(cohortNode(v1beta1.AcceleratorQuotaRootName, ""))
		mustCreateQuota(leafNode(leaf, v1beta1.AcceleratorQuotaRootName, []string{ns}, gpuFlavor, "8"))

		gomega.Eventually(func() string {
			var q v1beta1.AcceleratorQuota
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &q); err != nil {
				return ""
			}
			c := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaMaterialized)
			if c == nil {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonObjectConflict))

		// The operator's object is untouched. Server-side apply would have
		// taken it over, so this is the check that the explicit ownership read
		// is doing its job.
		gomega.Consistently(func() map[string]string {
			var got kueuev1beta2.ClusterQueue
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &got); err != nil {
				return map[string]string{"error": err.Error()}
			}
			return got.Labels
		}, "2s", interval).ShouldNot(gomega.HaveKey(v1beta1.AcceleratorQuotaManagedByLabel))
	})

	ginkgo.It("reaps a deleted leaf's Kueue objects", func() {
		leaf := uniqueName("leaf-reap")
		ns := uniqueName("ns-reap")
		mustCreateNamespace(ns)

		mustCreateQuota(cohortNode(v1beta1.AcceleratorQuotaRootName, ""))
		q := leafNode(leaf, v1beta1.AcceleratorQuotaRootName, []string{ns}, gpuFlavor, "8")
		gomega.Expect(k8sClient.Create(suiteCtx, q)).To(gomega.Succeed())

		getCQ(leaf)

		gomega.Expect(k8sClient.Delete(suiteCtx, q)).To(gomega.Succeed())

		// The ClusterQueue goes, and so does the CR: the finalizer is released
		// only after the reap, so the CR outliving its queue would mean the
		// reap never ran.
		gomega.Eventually(func() bool {
			var cq kueuev1beta2.ClusterQueue
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &cq)
			return client.IgnoreNotFound(err) == nil && err != nil
		}, timeout, interval).Should(gomega.BeTrue(), "the ClusterQueue was not reaped")

		gomega.Eventually(func() bool {
			var got v1beta1.AcceleratorQuota
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: leaf}, &got)
			return client.IgnoreNotFound(err) == nil && err != nil
		}, timeout, interval).Should(gomega.BeTrue(), "the finalizer was never released")
	})
})
