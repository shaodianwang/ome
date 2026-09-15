package basemodel

import (
	"context"
	"encoding/json"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Per-node download flow (controller.go:processModelStatus): the
// model-agent DaemonSet runs on each GPU node, downloads the model
// using the storage URI in the BaseModel/ClusterBaseModel, and
// publishes a per-node ConfigMap (one CM per node, labeled
// ModelStatusConfigMapLabel="true", living in the OME namespace).
// The reconciler walks every such ConfigMap, looks up the
// {ns}.basemodel.{name} or clusterbasemodel.{name} key, parses the
// ModelEntry, and rolls up Status.NodesReady / NodesFailed plus a
// LifecycleState computed by calculateLifecycleState (Ready if any
// node ready, Failed if no ready and any failed, else InTransit).
//
// These specs cover the per-node branch end-to-end. envtest has no
// kubelet so the agent is faked: tests Create the Node object, then
// write directly to the per-node ConfigMap to simulate what the
// real agent would publish.
var _ = ginkgo.Describe("BaseModel per-node download flow", func() {
	var nsName string

	ginkgo.BeforeEach(func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pn-")
		Expect(err).NotTo(HaveOccurred())
		nsName = ns.Name
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, nsName)).To(Succeed())
	})

	ginkgo.It("single-node Ready ConfigMap → state=Ready, NodesReady=[node]", func() {
		nodeName := "pn-single-node-1"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-single-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("agent writes Ready ConfigMap for the single node")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "pn-single-llama", nsName, false,
			shared.ModelStatusReady, nil)).To(Succeed())
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-single-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName))
			g.Expect(got.Status.NodesFailed).To(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("two Ready nodes → NodesReady is sorted lexicographically", func() {
		// Intentionally insert in reverse order so we catch the case
		// where the controller forgets to sort. processModelStatus
		// calls slices.Sort on both arrays before writing.
		nodes := []string{"pn-twonodes-z", "pn-twonodes-a"}
		for _, n := range nodes {
			makeNode(suiteCtx, k8sClient, n)
			ginkgo.DeferCleanup(deleteNode(k8sClient, n))
		}

		bm := v1beta1testing.MakeBaseModel("pn-twonodes-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		for _, n := range nodes {
			Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
				n, "pn-twonodes-llama", nsName, false,
				shared.ModelStatusReady, nil)).To(Succeed())
			ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, n))
		}

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-twonodes-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(Equal([]string{"pn-twonodes-a", "pn-twonodes-z"}),
				"NodesReady must be sorted, not insertion order")
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Ready + Failed mix → state=Ready (any-ready wins per calculateLifecycleState)", func() {
		readyNode := "pn-mixed-ready"
		failedNode := "pn-mixed-failed"
		for _, n := range []string{readyNode, failedNode} {
			makeNode(suiteCtx, k8sClient, n)
			ginkgo.DeferCleanup(deleteNode(k8sClient, n))
			ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, n))
		}

		bm := v1beta1testing.MakeBaseModel("pn-mixed-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, readyNode,
			"pn-mixed-llama", nsName, false, shared.ModelStatusReady, nil)).To(Succeed())
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, failedNode,
			"pn-mixed-llama", nsName, false, shared.ModelStatusFailed, nil)).To(Succeed())

		ginkgo.By("calculateLifecycleState: any Ready wins over Failed — state=Ready, both arrays populated")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-mixed-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(ConsistOf(readyNode))
			g.Expect(got.Status.NodesFailed).To(ConsistOf(failedNode))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("only Failed nodes → state=Failed, NodesReady empty", func() {
		nodeName := "pn-failed-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-failed-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-failed-llama", nsName, false, shared.ModelStatusFailed, nil)).To(Succeed())
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-failed-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			g.Expect(got.Status.NodesReady).To(BeEmpty())
			g.Expect(got.Status.NodesFailed).To(ConsistOf(nodeName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("only Updating nodes → state=InTransit, both arrays empty (Updating doesn't count toward either)", func() {
		nodeName := "pn-updating-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-updating-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-updating-llama", nsName, false, shared.ModelStatusUpdating, nil)).To(Succeed())
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		ginkgo.By("Updating nodes are not added to either array → both empty → state=InTransit")
		// Use Consistently to make sure the controller doesn't drift
		// the state — InTransit is the resting state for "still
		// downloading, no terminal status yet".
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-updating-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-updating-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(BeEmpty())
			g.Expect(got.Status.NodesFailed).To(BeEmpty())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Updating → Ready transition: state flips from InTransit to Ready when agent flips entry", func() {
		nodeName := "pn-flip-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-flip-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("agent reports Updating first")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-flip-llama", nsName, false, shared.ModelStatusUpdating, nil)).To(Succeed())
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-flip-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("agent flips entry to Ready → controller must transition state to Ready")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-flip-llama", nsName, false, shared.ModelStatusReady, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-flip-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("node deletion: per-node ConfigMap orphaned without its Node → entry pruned from NodesReady", func() {
		stayNode := "pn-nodedel-stay"
		goNode := "pn-nodedel-go"
		for _, n := range []string{stayNode, goNode} {
			makeNode(suiteCtx, k8sClient, n)
		}
		ginkgo.DeferCleanup(deleteNode(k8sClient, stayNode))
		// goNode is deleted by the spec body itself; cleanup is the
		// belt-and-suspenders no-op-on-NotFound version.
		ginkgo.DeferCleanup(deleteNode(k8sClient, goNode))

		bm := v1beta1testing.MakeBaseModel("pn-nodedel-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		for _, n := range []string{stayNode, goNode} {
			Expect(writeNodeAgentEntry(suiteCtx, k8sClient, n,
				"pn-nodedel-llama", nsName, false, shared.ModelStatusReady, nil)).To(Succeed())
			ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, n))
		}

		ginkgo.By("both nodes initially Ready")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-nodedel-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(ConsistOf(stayNode, goNode))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("delete one node — its CM is orphaned; processModelStatus garbage-collects ConfigMaps whose Node is confirmed absent, so the entry must drop from NodesReady")
		Expect(k8sClient.Delete(suiteCtx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: goNode}})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-nodedel-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(ConsistOf(stayNode))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("spec auto-fill from agent ModelConfig: empty Spec fields populated from CM Config payload", func() {
		nodeName := "pn-autofill-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		// Create BM with NO ModelType / ModelArchitecture set — agent
		// must fill them via updateSpecWithConfig.
		bm := v1beta1testing.MakeBaseModel("pn-autofill-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(bm.Spec.ModelType).To(BeNil(), "precondition: spec must start empty so we can verify auto-fill")
		Expect(bm.Spec.ModelArchitecture).To(BeNil())
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-autofill-llama", nsName, false, shared.ModelStatusReady,
			&shared.ModelConfig{
				ModelType:          "llama",
				ModelArchitecture:  "LlamaForCausalLM",
				ModelParameterSize: "7B",
				MaxTokens:          4096,
			})).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-autofill-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("llama"))
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil())
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("LlamaForCausalLM"))
			g.Expect(got.Spec.ModelParameterSize).NotTo(BeNil())
			g.Expect(*got.Spec.ModelParameterSize).To(Equal("7B"))
			g.Expect(got.Spec.MaxTokens).NotTo(BeNil())
			g.Expect(*got.Spec.MaxTokens).To(Equal(int32(4096)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ClusterBaseModel + BaseModel keys coexist in one node ConfigMap without cross-talk", func() {
		// One node, two models with same display name but different
		// scopes. The keys are clusterbasemodel.shared-name vs
		// {ns}.basemodel.shared-name (constants.GetModelConfigMapKey).
		// Each reconciler must pick only its own key — no leakage.
		nodeName := "pn-isolation-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		// Cluster-scope model is Ready; namespaced model is Failed.
		// If the keys leak, the Failed one would also see Ready=True
		// (or the Ready one would get NodesFailed populated).
		cbm := v1beta1testing.MakeClusterBaseModel("pn-isolation-shared").
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		bm := v1beta1testing.MakeBaseModel("pn-isolation-shared", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-isolation-shared", "", true, shared.ModelStatusReady, nil)).To(Succeed())
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"pn-isolation-shared", nsName, false, shared.ModelStatusFailed, nil)).To(Succeed())

		ginkgo.By("ClusterBaseModel sees Ready, BaseModel sees Failed — keys are isolated by GetModelConfigMapKey")
		Eventually(func(g Gomega) {
			gotCBM := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-isolation-shared"}, gotCBM)).To(Succeed())
			g.Expect(gotCBM.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(gotCBM.Status.NodesReady).To(ConsistOf(nodeName))
			g.Expect(gotCBM.Status.NodesFailed).To(BeEmpty())

			gotBM := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-isolation-shared", Namespace: nsName}, gotBM)).To(Succeed())
			g.Expect(gotBM.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			g.Expect(gotBM.Status.NodesReady).To(BeEmpty())
			g.Expect(gotBM.Status.NodesFailed).To(ConsistOf(nodeName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ConfigMap exists but missing this model's key → arrays stay empty (no false-positive)", func() {
		// A CM that's labeled ModelStatusConfigMapLabel="true" but
		// has only OTHER models' entries in it. The reconciler must
		// not panic, must not add the node to NodesReady/Failed, and
		// state stays at the initial empty value.
		nodeName := "pn-otherkey-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		bm := v1beta1testing.MakeBaseModel("pn-otherkey-llama", nsName).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		// Write an entry for a DIFFERENT model name — not ours.
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient, nodeName,
			"some-other-model", nsName, false, shared.ModelStatusReady, nil)).To(Succeed())

		// Give the controller a few reconciles, then assert the rollup
		// for OUR model is still empty. Consistently here is the right
		// shape: a passing assertion now doesn't prove the negative,
		// we need it to STAY empty.
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pn-otherkey-llama", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(BeEmpty())
			g.Expect(got.Status.NodesFailed).To(BeEmpty())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// --- per-node helpers -----------------------------------------------------

// makeNode creates a Node object. Used as a stand-in for a real GPU
// node — envtest has no kubelet, so processModelStatus's Get(node)
// must succeed for the per-node ConfigMap to count.
func makeNode(ctx context.Context, kc client.Client, name string) {
	ginkgo.GinkgoHelper()
	err := kc.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

// deleteNode returns a DeferCleanup-friendly closure that deletes a
// Node by name. NotFound is treated as success so the helper is safe
// to call even when the spec body already deleted the Node.
func deleteNode(kc client.Client, name string) func() {
	return func() {
		err := kc.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	}
}

// deleteNodeConfigMap returns a DeferCleanup closure that removes the
// per-node ConfigMap from the OME namespace. The CM name equals the
// Node name (per modelagent convention). NotFound = success.
func deleteNodeConfigMap(kc client.Client, nodeName string) func() {
	return func() {
		err := kc.Delete(context.Background(), &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: constants.OMENamespace},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	}
}

// writeNodeAgentEntry simulates the model-agent on `nodeName`
// publishing a status entry for one model into its per-node ConfigMap.
// Creates the ConfigMap on first call, merges additional model entries
// on subsequent calls (multiple models per node share one CM).
//
// modelNamespace is "" for ClusterBaseModel.
func writeNodeAgentEntry(ctx context.Context, c client.Client,
	nodeName, modelName, modelNamespace string, isClusterScoped bool,
	status shared.ModelStatus, cfg *shared.ModelConfig) error {

	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)
	entry := shared.ModelEntry{
		Name:   modelName,
		Status: status,
		Config: cfg,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{}
	getErr := c.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: constants.OMENamespace}, cm)
	if apierrors.IsNotFound(getErr) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: constants.OMENamespace,
				Labels:    map[string]string{constants.ModelStatusConfigMapLabel: "true"},
			},
			Data: map[string]string{modelKey: string(data)},
		}
		return c.Create(ctx, cm)
	}
	if getErr != nil {
		return getErr
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[modelKey] = string(data)
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}
	cm.Labels[constants.ModelStatusConfigMapLabel] = "true"
	return c.Update(ctx, cm)
}
