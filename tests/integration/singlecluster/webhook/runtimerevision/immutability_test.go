package runtimerevision

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("ControllerRevision immutability webhook (OEP-0014)", ginkgo.Ordered, func() {

	mkRevision := func(name string, omeOwned bool, dataPayload string) *appsv1.ControllerRevision {
		labels := map[string]string{}
		annotations := map[string]string{}
		if omeOwned {
			annotations[constants.RuntimeRevisionCreatedByKey] = constants.RuntimeRevisionCreatedByOMEValue
			labels[constants.RuntimeRevisionOfLabelKey] = "test-runtime"
		}
		return &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Annotations: annotations,
				Labels:      labels,
			},
			Data:     runtime.RawExtension{Raw: []byte(dataPayload)},
			Revision: 1,
		}
	}

	ginkgo.It("rejects Update on an OME-created revision (Data change)", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rev-imm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		rev := mkRevision("ome-cr-1", true, `{"original":true}`)
		rev.Namespace = ns.Name
		gomega.Expect(k8sClient.Create(suiteCtx, rev)).To(gomega.Succeed())

		// Mutate Data and try to Update.
		fetched := &appsv1.ControllerRevision{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(rev), fetched)).To(gomega.Succeed())
		fetched.Data = runtime.RawExtension{Raw: []byte(`{"original":false,"tampered":true}`)}
		err = k8sClient.Update(suiteCtx, fetched)
		gomega.Expect(err).To(gomega.HaveOccurred(), "Update on an OME-created revision must be denied")
		gomega.Expect(strings.ToLower(err.Error())).To(gomega.ContainSubstring("immutable"))
	})

	ginkgo.It("rejects Update on an OME-created revision even when only labels/annotations change", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rev-imm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		rev := mkRevision("ome-cr-meta", true, `{"x":1}`)
		rev.Namespace = ns.Name
		gomega.Expect(k8sClient.Create(suiteCtx, rev)).To(gomega.Succeed())

		fetched := &appsv1.ControllerRevision{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(rev), fetched)).To(gomega.Succeed())
		fetched.Labels["test"] = "meta-edit"
		err = k8sClient.Update(suiteCtx, fetched)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			"webhook is intentionally strict — any Update on an OME revision is denied")
	})

	ginkgo.It("allows metadata Update on a revision NOT marked as OME-created (StatefulSet/DaemonSet behavior preserved)", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rev-imm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		rev := mkRevision("foreign-cr", false, `{"sts":true}`)
		rev.Namespace = ns.Name
		gomega.Expect(k8sClient.Create(suiteCtx, rev)).To(gomega.Succeed())

		fetched := &appsv1.ControllerRevision{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(rev), fetched)).To(gomega.Succeed())
		// K8s built-in validation makes .data immutable on ALL
		// ControllerRevisions — so we test a metadata-only change
		// instead: labels/annotations are mutable on non-OME revisions
		// (e.g., StatefulSet/DaemonSet adding owner refs, finalizers,
		// retention labels). Our webhook must NOT interfere.
		if fetched.Labels == nil {
			fetched.Labels = map[string]string{}
		}
		fetched.Labels["test/added-by-test"] = "ok"
		gomega.Expect(k8sClient.Update(suiteCtx, fetched)).To(gomega.Succeed(),
			"non-OME revisions must accept metadata Updates")
	})

	ginkgo.It("allows Create on any revision (including OME-marked) — the webhook only fires on Update", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rev-imm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		rev := mkRevision("ome-cr-create", true, `{"created":true}`)
		rev.Namespace = ns.Name
		gomega.Expect(k8sClient.Create(suiteCtx, rev)).To(gomega.Succeed())
	})

	ginkgo.It("allows Delete on an OME-created revision (GC path; not denied)", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rev-imm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		rev := mkRevision("ome-cr-del", true, `{"d":1}`)
		rev.Namespace = ns.Name
		gomega.Expect(k8sClient.Create(suiteCtx, rev)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Delete(suiteCtx, rev)).To(gomega.Succeed(),
			"GC controller (Phase 3) needs to be able to delete OME-owned revisions")
	})
})
