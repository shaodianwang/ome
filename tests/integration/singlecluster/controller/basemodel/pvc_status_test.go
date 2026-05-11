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
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PVC reconcile path: status condition + state behavior beyond the
// happy/sad cases in pvc_test.go.
//
// Covers: in-progress (Updating) status, status flicker across
// multiple agent updates, full spec auto-fill from the agent's
// ModelConfig payload, and propagation of the agent's last-error
// annotation into status condition messages.
var _ = ginkgo.Describe("BaseModel PVC status conditions", ginkgo.Ordered, func() {

	ginkgo.It("PVC ConfigMap with Status=Updating (mid-extraction) → state stays InTransit", func() {
		// SimulatePVCAgentReady covers the terminal Ready case; no
		// helper exists for the in-progress Updating case. Write the
		// ConfigMap directly with status=Updating.
		ns := "pvc-updating"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "updating-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "updating-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("updating-llama", ns).
			StorageURI("pvc://updating-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(writePVCAgentEntry(suiteCtx, k8sClient,
			"updating-llama", ns, false, modelagent.ModelStatusUpdating, nil)).To(Succeed())

		ginkgo.By("Updating status keeps the model in InTransit, not Ready")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "updating-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("status flicker Updating → Ready → Updating → Ready: controller tracks each transition", func() {
		ns := "pvc-flicker"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "flicker-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "flicker-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("flicker-llama", ns).
			StorageURI("pvc://flicker-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		// 1. Updating
		Expect(writePVCAgentEntry(suiteCtx, k8sClient,
			"flicker-llama", ns, false, modelagent.ModelStatusUpdating, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "flicker-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// 2. Ready
		Expect(writePVCAgentEntry(suiteCtx, k8sClient,
			"flicker-llama", ns, false, modelagent.ModelStatusReady,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "flicker-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// 3. Back to Updating (e.g., agent recomputes after a model
		//    file change). State must drop back to InTransit.
		Expect(writePVCAgentEntry(suiteCtx, k8sClient,
			"flicker-llama", ns, false, modelagent.ModelStatusUpdating, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "flicker-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateInTransit))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// 4. Ready again
		Expect(writePVCAgentEntry(suiteCtx, k8sClient,
			"flicker-llama", ns, false, modelagent.ModelStatusReady,
			&modelagent.ModelConfig{ModelType: "llama"})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "flicker-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("spec auto-fill: every ModelConfig field maps to its Spec counterpart via PVC path", func() {
		// pvc_test.go's happy path covers ModelType + ModelArchitecture.
		// This spec exercises the rest: ModelParameterSize, MaxTokens,
		// ModelCapabilities, ModelFramework, ModelFormat — all
		// populated from one agent ConfigMap.
		ns := "pvc-autofill"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "autofill-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "autofill-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("autofill-llama", ns).
			StorageURI("pvc://autofill-pvc/models/llama").Obj()
		Expect(bm.Spec.ModelType).To(BeNil())
		Expect(bm.Spec.ModelParameterSize).To(BeNil())
		Expect(bm.Spec.ModelCapabilities).To(BeEmpty())
		Expect(bm.Spec.ModelFramework).To(BeNil())
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"autofill-llama", ns, false,
			&modelagent.ModelConfig{
				ModelType:          "llama",
				ModelArchitecture:  "LlamaForCausalLM",
				ModelParameterSize: "70B",
				MaxTokens:          8192,
				ModelCapabilities:  []string{"TEXT_GENERATION", "TEXT_EMBEDDINGS"},
				ModelFramework:     map[string]string{"name": "transformers", "version": "4.34.0"},
				ModelFormat:        map[string]string{"name": "safetensors", "version": "1.0.0"},
				Quantization:       "fp16",
			})).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "autofill-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))

			g.Expect(got.Spec.ModelParameterSize).NotTo(BeNil())
			g.Expect(*got.Spec.ModelParameterSize).To(Equal("70B"))

			g.Expect(got.Spec.MaxTokens).NotTo(BeNil())
			g.Expect(*got.Spec.MaxTokens).To(Equal(int32(8192)))

			g.Expect(got.Spec.ModelCapabilities).To(ConsistOf("TEXT_GENERATION", "TEXT_EMBEDDINGS"))

			g.Expect(got.Spec.ModelFramework).NotTo(BeNil())
			g.Expect(got.Spec.ModelFramework.Name).To(Equal("transformers"))
			g.Expect(got.Spec.ModelFramework.Version).NotTo(BeNil())
			g.Expect(*got.Spec.ModelFramework.Version).To(Equal("4.34.0"))

			g.Expect(got.Spec.ModelFormat.Name).To(Equal("safetensors"))
			g.Expect(got.Spec.ModelFormat.Version).NotTo(BeNil())
			g.Expect(*got.Spec.ModelFormat.Version).To(Equal("1.0.0"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("agent Failed entry surfaces the last-error annotation in some status condition message", func() {
		// SimulatePVCAgentFailed sets PVCMetadataLastErrorAnnotation
		// on the per-PVC ConfigMap. The controller must propagate
		// that error message into one of the status conditions so
		// operators can see what went wrong without spelunking CMs.
		ns := "pvc-failed-msg"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "failed-msg-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "failed-msg-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("failed-msg-llama", ns).
			StorageURI("pvc://failed-msg-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		errMsg := "agent crashed parsing config.json: unexpected EOF"
		Expect(v1beta1testing.SimulatePVCAgentFailed(suiteCtx, k8sClient,
			"failed-msg-llama", ns, false, errMsg)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "failed-msg-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))

			// Don't be picky about which condition carries the error
			// — Ready or PVCMetadataExtracting are both reasonable —
			// just confirm the message is somewhere visible.
			joined := ""
			for _, c := range got.Status.Conditions {
				joined += c.Message + "|"
			}
			g.Expect(joined).To(ContainSubstring(errMsg),
				"agent's last-error annotation must surface in some status condition message")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// --- file-local helpers ----------------------------------------------------

// writePVCAgentEntry is the lower-level cousin of
// SimulatePVCAgentReady / SimulatePVCAgentFailed: lets a spec write
// any ModelStatus value (notably Updating, which the helpers don't
// expose) into the per-PVC ConfigMap. Idempotent: updates if exists.
func writePVCAgentEntry(ctx context.Context, c client.Client,
	modelName, modelNamespace string, isClusterScoped bool,
	status modelagent.ModelStatus, cfg *modelagent.ModelConfig) error {

	cmName := constants.GetPVCMetadataConfigMapName(modelName, modelNamespace, isClusterScoped)
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	entry := modelagent.ModelEntry{Name: modelName, Status: status, Config: cfg}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	scope := "namespaced"
	if isClusterScoped {
		scope = "cluster"
	}

	cm := &corev1.ConfigMap{}
	getErr := c.Get(ctx, types.NamespacedName{Name: cmName, Namespace: constants.OMENamespace}, cm)
	if apierrors.IsNotFound(getErr) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: constants.OMENamespace,
				Labels: map[string]string{
					constants.PVCStorageConfigMapLabel:  "true",
					constants.PVCMetadataModelNameLabel: modelName,
					constants.PVCMetadataScopeLabel:     scope,
				},
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
	return c.Update(ctx, cm)
}
