package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Per-node deletion handshake (handleModelDeletion in controller.go).
// When a non-PVC, non-Sharded BaseModel is deleted, the controller
// must wait for every per-node agent ConfigMap entry to either:
//
//   - be absent from the CM (agent removed it after model uninstall), OR
//   - have Status=Deleted (agent acknowledged the deletion)
//
// before the finalizer is removed and the apiserver GCs the object.
// This is the per-node analog of the PVC deletion path; it has zero
// integration coverage today.
var _ = ginkgo.Describe("BaseModel per-node deletion handshake", func() {
	var nsName string

	ginkgo.BeforeEach(func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pn-del-")
		Expect(err).NotTo(HaveOccurred())
		nsName = ns.Name
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, nsName)).To(Succeed())
	})

	ginkgo.It("BM with no per-node ConfigMaps → finalizer clears immediately on Delete", func() {
		// No agent ever ran for this BM (no Node, no per-node CM). The
		// controller's deletion list returns 0 candidate CMs, so the
		// finalizer is removed on the first deletion-driven reconcile.
		bm := v1beta1testing.MakeBaseModel("pn-del-noagent", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		// Wait for the controller to add the finalizer. Without this
		// guard the Delete below could race the controller's
		// finalizer-add reconcile and the spec would be testing
		// vacuous "delete a finalizer-less object" semantics.
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-noagent", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-noagent", Namespace: nsName}, &v1beta1.BaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"BM should be fully gone (no agent CMs to wait for)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BM with non-Deleted entry on a per-node CM → controller waits, finalizer clears once agent flips entry to Deleted", func() {
		// Pre-create the agent's CM entry as Status=Ready, so when we
		// Delete the BM the controller's handshake finds an entry
		// that isn't Deleted, sees it as "agent still has the model
		// installed", and requeues. We then simulate the agent
		// updating the entry to Status=Deleted, which triggers the
		// CM watch → re-reconcile → finalizer clear.
		nodeName := "pn-del-wait-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-del-wait", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "pn-del-wait", nsName, false,
			modelagent.ModelStatusReady, nil)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-wait", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
			g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Delete the BM — finalizer prevents actual removal while agent CM entry is still Ready")
		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-wait", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.DeletionTimestamp).NotTo(BeNil(), "BM must have DeletionTimestamp set")
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer),
				"finalizer must still be present while non-Deleted entries exist")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Agent flips entry to Status=Deleted → CM watch fires → controller clears finalizer")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "pn-del-wait", nsName, false,
			modelagent.ModelStatusDeleted, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-wait", Namespace: nsName}, &v1beta1.BaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "BM should be fully gone after agent acknowledges deletion")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BM with already-Deleted entry → finalizer clears immediately on Delete (no waiting)", func() {
		// Symmetric to the wait case but the CM entry is already
		// Status=Deleted before we Delete the BM, so the first
		// deletion-driven reconcile finds the handshake satisfied.
		nodeName := "pn-del-fast-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-del-fast", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-fast", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "pn-del-fast", nsName, false,
			modelagent.ModelStatusDeleted, nil)).To(Succeed())

		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-fast", Namespace: nsName}, &v1beta1.BaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BM with malformed agent entry → controller treats as non-Deleted (safety: do NOT GC the model)", func() {
		// handleModelDeletion's safety branch: if a CM entry can't
		// be unmarshaled, the controller adds it to modelsNotDeleted
		// "for safety" rather than assuming the model is gone. This
		// is the bias-toward-safety check operators rely on when
		// agents publish corrupt data — we must not silently GC a
		// model whose actual installed state we can't read.
		nodeName := "pn-del-bad-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-del-bad", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-bad", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Inject a CM entry that's labeled correctly + has the right
		// model key, but the value is malformed JSON.
		modelKey := constants.GetModelConfigMapKey(nsName, "pn-del-bad", false)
		Expect(k8sClient.Create(suiteCtx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: constants.OMENamespace,
				Labels:    map[string]string{constants.ModelStatusConfigMapLabel: "true"},
			},
			Data: map[string]string{modelKey: `{ this is not valid json`},
		})).To(Succeed())

		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())

		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-bad", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.DeletionTimestamp).NotTo(BeNil())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer),
				"controller MUST keep finalizer when entry parsing fails — never silently GC")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Replace bad entry with a valid Deleted entry → finalizer can clear")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "pn-del-bad", nsName, false,
			modelagent.ModelStatusDeleted, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-del-bad", Namespace: nsName}, &v1beta1.BaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
