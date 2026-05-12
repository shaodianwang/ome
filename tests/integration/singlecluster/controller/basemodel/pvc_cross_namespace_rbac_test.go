package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PVC-backed BaseModels in non-OME namespaces hit a deployment gap: the
// chart provisions ServiceAccount + ClusterRole + ClusterRoleBinding
// only in the OME namespace, so a Job spawned in any other namespace
// fails at pod creation with "serviceaccount not found" and the agent
// (even if it ran) would be denied write access to the per-model status
// ConfigMap in the OME namespace. The controller now closes both holes
// on demand. These specs pin the on-demand RBAC creation end-to-end.
var _ = ginkgo.Describe("BaseModel PVC reconcile cross-namespace RBAC", ginkgo.Ordered, func() {

	ginkgo.It("BaseModel in non-OME namespace: controller ensures SA in user ns + RoleBinding in OME ns", func() {
		// Use a namespace name that doesn't equal constants.OMENamespace
		// so the on-demand RBAC path actually fires (the in-OME-ns case
		// is a documented no-op).
		const ns = "rbac-cross-ns"
		// SA name matches the suite_test.go OmeAgentConfig setup
		// (suite_test.go:64 ServiceAccount: "ome-model-metadata"),
		// which mirrors the chart default.
		const saName = "ome-model-metadata"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "shared", ns)
		bindPVC(suiteCtx, k8sClient, "shared", ns)

		bm := v1beta1testing.MakeBaseModel("rbac-cross-ns-bm", ns).
			StorageURI("pvc://shared/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		// The BM has no agent in envtest so it requeues every 5s
		// forever. Without cleanup it permanently inflates the
		// reconciler queue for every spec that runs after this one,
		// and queue pressure starves timing-sensitive specs (e.g.,
		// the URI-change spec in sharded_test.go).
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		// SA in the user namespace — kubelet needs this BEFORE it
		// will start the Job pod. Without it, the Job sits at 0/1
		// ready forever and the BaseModel never leaves InTransit.
		ginkgo.By("ensuring the metadata SA appears in the user namespace")
		Eventually(func(g Gomega) {
			sa := &corev1.ServiceAccount{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: saName, Namespace: ns}, sa)).To(Succeed())
			g.Expect(sa.Labels["app.kubernetes.io/component"]).To(Equal("ome-model-metadata-job"),
				"controller-created SA must carry the same component label as the chart-installed one")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// RoleBinding in the OME namespace, name = "<sa>-<source-ns>",
		// subject = the user-ns SA, roleRef = the cluster-scoped
		// ClusterRole. The agent's writeStatus
		// (model-metadata/metadata.go:179) targets constants.OMENamespace,
		// so without this RoleBinding the agent would be denied even
		// after kubelet successfully started its pod.
		ginkgo.By("ensuring a per-source-ns RoleBinding appears in the OME namespace")
		Eventually(func(g Gomega) {
			rb := &rbacv1.RoleBinding{}
			rbName := saName + "-" + ns
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: rbName, Namespace: constants.OMENamespace}, rb)).To(Succeed())

			g.Expect(rb.RoleRef.Kind).To(Equal("ClusterRole"))
			g.Expect(rb.RoleRef.Name).To(Equal(saName),
				"RoleRef must point at the chart-installed cluster-scoped ClusterRole (same name as the SA by chart convention)")
			g.Expect(rb.Subjects).To(HaveLen(1))
			g.Expect(rb.Subjects[0].Kind).To(Equal("ServiceAccount"))
			g.Expect(rb.Subjects[0].Name).To(Equal(saName))
			g.Expect(rb.Subjects[0].Namespace).To(Equal(ns),
				"Subject must point at the user-ns SA the Job pod actually runs as")
			g.Expect(rb.Labels["models.ome/metadata-source-namespace"]).To(Equal(ns),
				"source-ns label lets operators audit which user namespaces have OME-ns write access")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
