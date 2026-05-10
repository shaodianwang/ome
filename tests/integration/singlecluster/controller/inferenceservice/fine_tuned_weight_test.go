package inferenceservice

import (
	"encoding/json"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// FineTunedWeight (FTW) integration with ISVC. There is no FTW
// reconciler — FTW is a passive cluster-scoped CRD consumed by the
// ISVC modelconfig reconciler at reconcile time. The reconciler
// resolves an ISVC's model reference into a per-ISVC ConfigMap
// (modelconfig-<isvc>) whose data contains a JSON-encoded
// []ModelConfigEntry. Each entry has a ModelSpec block (the actual
// base model) and an optional FineTunedWeightSpec block (the FTW's
// own storage + modelType).
//
// Two ways an ISVC can pull in a FTW
// (modelconfig_reconciler.go:generateModelConfigEntry):
//
//  1. Spec.Model.Kind=FineTunedWeight, Name=<ftw>:
//     reconciler fetches the FTW, follows FTW.BaseModelRef to find
//     the actual base model, populates both ModelSpec (from base
//     model) AND FineTunedWeightSpec (from the FTW).
//
//  2. Spec.Model.Kind=BaseModel/ClusterBaseModel,
//     FineTunedWeights=[<ftw>]:
//     reconciler fetches the base model directly, then layers the
//     first FTW in the array on top.
//
// Both paths must produce a ConfigMap with FineTunedWeightSpec
// populated; failures (missing FTW, FTW with no BaseModelRef.Name)
// must surface as the controller failing reconcile (no Deployment).
var _ = ginkgo.Describe("InferenceService + FineTunedWeight", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-ftw-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Spec.Model.Kind=FineTunedWeight + FTW → ClusterBaseModel: ConfigMap entry has both ModelSpec + FineTunedWeightSpec", func() {
		ginkgo.By("creating the underlying ClusterBaseModel")
		cbm := v1beta1testing.MakeClusterBaseModel("ftw-base-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a FineTunedWeight pointing at the CBM")
		ftw := v1beta1testing.MakeFineTunedWeight("ftw-lora-customer-a").
			BaseModelRef("ftw-base-llama", ""). // empty NS → ClusterBaseModel
			ModelType("LoRA").
			StorageURI("oci://n/test/b/models/o/lora-customer-a").
			Obj()
		Expect(k8sClient.Create(suiteCtx, ftw)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ftw) })

		ginkgo.By("creating a ClusterServingRuntime that supports the underlying model")
		csr := v1beta1testing.MakeClusterServingRuntime("ftw-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC pointing directly at the FTW")
		isvc := v1beta1testing.MakeInferenceService("ftw-isvc-direct", ns.Name).
			FineTunedWeightModel("ftw-lora-customer-a").
			Runtime("ftw-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the per-ISVC modelconfig ConfigMap with both ModelSpec (from CBM) and FineTunedWeightSpec (from FTW)")
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      constants.ModelConfigName("ftw-isvc-direct"),
				Namespace: ns.Name,
			}, cm)).To(Succeed())

			entries := unmarshalModelConfigEntries(g, cm)
			g.Expect(entries).To(HaveLen(1))
			e := entries[0]

			// ModelName comes from the FTW's BaseModelRef target
			// (modelconfig_reconciler.go:277).
			g.Expect(e.ModelName).To(Equal("ftw-base-llama"))

			// ModelSpec is populated from the underlying CBM.
			g.Expect(e.ModelSpec.StorageURI).NotTo(BeNil())
			g.Expect(*e.ModelSpec.StorageURI).To(Equal("oci://n/test/b/models/o/llama-7b"))

			// FineTunedWeightSpec is populated from the FTW itself.
			g.Expect(e.FineTunedWeightSpec).NotTo(BeNil())
			g.Expect(e.FineTunedWeightSpec.StorageURI).NotTo(BeNil())
			g.Expect(*e.FineTunedWeightSpec.StorageURI).To(Equal("oci://n/test/b/models/o/lora-customer-a"))
			g.Expect(e.FineTunedWeightSpec.ModelType).NotTo(BeNil())
			g.Expect(*e.FineTunedWeightSpec.ModelType).To(Equal("LoRA"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// NOTE: a Deployment is NOT produced today even though the
		// modelconfig ConfigMap is correctly populated. Root cause:
		// utils.ReconcileBaseModel (reconciliation.go:51-66) calls
		// GetBaseModel(isvc.Spec.Model.Name) directly without
		// checking Spec.Model.Kind, so when Kind=FineTunedWeight
		// the lookup tries to find a BaseModel/ClusterBaseModel
		// with the FTW's name and fails:
		//
		//   "No BaseModel or ClusterBaseModel with the name: <ftw>"
		//
		// The modelconfig sub-reconciler has the proper FTW branch
		// (modelconfig_reconciler.go:260-302); the main reconcile
		// loop does not. Filing separately. Spec asserts current
		// behavior (no Deployment) so it doesn't lie about coverage.
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(),
				"controller bug: ReconcileBaseModel doesn't follow Kind=FineTunedWeight, so no Deployment is created today")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Spec.Model.Kind=FineTunedWeight + FTW → namespaced BaseModel: ModelSpec resolves via FTW.BaseModelRef.Namespace", func() {
		// FTW.BaseModelRef.Namespace is populated → reconciler treats
		// the target as a namespaced BaseModel in that specific
		// namespace, NOT the ISVC's namespace
		// (modelconfig_reconciler.go:283-287).
		ginkgo.By("creating a namespaced BaseModel in a DIFFERENT namespace from the ISVC")
		bmNs, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ftw-bm-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, bmNs.Name)).To(Succeed())
		})

		bm := v1beta1testing.MakeBaseModel("ftw-ns-base", bmNs.Name).
			StorageURI("oci://n/test/b/models/o/llama-7b-ns").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ftw := v1beta1testing.MakeFineTunedWeight("ftw-cross-ns").
			BaseModelRef("ftw-ns-base", bmNs.Name). // non-empty NS → namespaced BaseModel
			ModelType("LoRA").
			StorageURI("oci://n/test/b/models/o/lora-cross-ns").
			Obj()
		Expect(k8sClient.Create(suiteCtx, ftw)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ftw) })

		csr := v1beta1testing.MakeClusterServingRuntime("ftw-cross-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("ftw-isvc-cross", ns.Name).
			FineTunedWeightModel("ftw-cross-ns").
			Runtime("ftw-cross-rt").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("ConfigMap entry's ModelSpec must come from the BaseModel in the FTW's referenced namespace, not the ISVC's namespace")
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      constants.ModelConfigName("ftw-isvc-cross"),
				Namespace: ns.Name,
			}, cm)).To(Succeed())

			entries := unmarshalModelConfigEntries(g, cm)
			g.Expect(entries).To(HaveLen(1))
			g.Expect(entries[0].ModelSpec.StorageURI).NotTo(BeNil())
			g.Expect(*entries[0].ModelSpec.StorageURI).To(Equal("oci://n/test/b/models/o/llama-7b-ns"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Spec.Model.Kind=FineTunedWeight + FTW does not exist → no ConfigMap, no Deployment (controller stays stuck on the missing reference)", func() {
		// The reconciler returns an error before producing any
		// children, and the modelconfig ConfigMap is never written.
		csr := v1beta1testing.MakeClusterServingRuntime("ftw-missing-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("ftw-isvc-missing", ns.Name).
			FineTunedWeightModel("ftw-does-not-exist").
			Runtime("ftw-missing-rt").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		// Use Consistently to prove the negative — these resources
		// must STAY absent across multiple reconcile attempts, not
		// just be slow to appear.
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      constants.ModelConfigName("ftw-isvc-missing"),
				Namespace: ns.Name,
			}, &corev1.ConfigMap{})
			g.Expect(err).To(HaveOccurred(),
				"modelconfig ConfigMap must NOT exist when the FTW reference is unresolvable")

			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "no Deployment should be created when the FTW is missing")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ModelRef.FineTunedWeights array path: ClusterBaseModel + FTW name in array → both populated in ConfigMap entry", func() {
		// The OTHER way to attach a FTW: keep Spec.Model.Kind as
		// BaseModel/CBM, list the FTW name in the FineTunedWeights
		// array. Reconciler picks the first element only
		// (modelconfig_reconciler.go:316).
		cbm := v1beta1testing.MakeClusterBaseModel("array-base-llama").
			StorageURI("oci://n/test/b/models/o/llama-array").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ftw := v1beta1testing.MakeFineTunedWeight("ftw-array-lora").
			BaseModelRef("array-base-llama", "").
			ModelType("LoRA").
			StorageURI("oci://n/test/b/models/o/lora-array").
			Obj()
		Expect(k8sClient.Create(suiteCtx, ftw)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ftw) })

		csr := v1beta1testing.MakeClusterServingRuntime("array-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("array-isvc", ns.Name).
			Model("array-base-llama").
			WithFineTunedWeights("ftw-array-lora").
			Runtime("array-rt").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      constants.ModelConfigName("array-isvc"),
				Namespace: ns.Name,
			}, cm)).To(Succeed())

			entries := unmarshalModelConfigEntries(g, cm)
			g.Expect(entries).To(HaveLen(1))
			e := entries[0]

			// Direct CBM reference → ModelName is the CBM's own name.
			g.Expect(e.ModelName).To(Equal("array-base-llama"))

			g.Expect(e.ModelSpec.StorageURI).NotTo(BeNil())
			g.Expect(*e.ModelSpec.StorageURI).To(Equal("oci://n/test/b/models/o/llama-array"))

			g.Expect(e.FineTunedWeightSpec).NotTo(BeNil())
			g.Expect(e.FineTunedWeightSpec.StorageURI).NotTo(BeNil())
			g.Expect(*e.FineTunedWeightSpec.StorageURI).To(Equal("oci://n/test/b/models/o/lora-array"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ModelRef.FineTunedWeights array with missing FTW → no ConfigMap, no Deployment", func() {
		// Same negative check as direct-reference, but exercising the
		// array branch so a regression in either branch surfaces.
		cbm := v1beta1testing.MakeClusterBaseModel("array-missing-base").
			StorageURI("oci://n/test/b/models/o/llama-base").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("array-missing-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("array-missing-isvc", ns.Name).
			Model("array-missing-base").
			WithFineTunedWeights("ftw-also-missing").
			Runtime("array-missing-rt").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      constants.ModelConfigName("array-missing-isvc"),
				Namespace: ns.Name,
			}, &corev1.ConfigMap{})
			g.Expect(err).To(HaveOccurred())
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("CRD rejects FTW Create when BaseModelRef.Name is nil — defensive controller check is dead code in practice", func() {
		// The reconciler has a defensive nil-check at
		// modelconfig_reconciler.go:272 ("FineTunedWeight %s has no
		// BaseModelRef.Name specified"), but the CRD's
		// baseModelRef.required=[name] AND spec.required=[baseModelRef]
		// schema means a nil-name FTW never reaches the apiserver. We
		// pin the schema check here so a future CRD-relaxation
		// regression surfaces — if someone removes the required marker,
		// this spec starts passing FTW Create AND the dead controller
		// branch becomes reachable.
		ftw := &v1beta1.FineTunedWeight{}
		ftw.Name = "ftw-no-baseref-direct"
		ftw.Spec.HyperParameters.Raw = []byte(`{}`)
		uri := "oci://n/test/b/models/o/lora-noref"
		ftw.Spec.Storage = &v1beta1.StorageSpec{StorageUri: &uri}
		mt := "LoRA"
		ftw.Spec.ModelType = &mt
		jobName := "ftw-test-training-job"
		ftw.Spec.TrainingJobRef.Name = &jobName
		// BaseModelRef intentionally left zero.

		err := k8sClient.Create(suiteCtx, ftw)
		Expect(err).To(HaveOccurred(),
			"CRD must reject Create when baseModelRef.name is missing")
		Expect(err.Error()).To(ContainSubstring("baseModelRef.name"),
			"error must specifically point at the missing baseModelRef.name field")
	})
})

// modelConfigEntry mirrors the unexported ModelConfigEntry shape from
// pkg/.../modelconfig_reconciler.go so tests can decode the ConfigMap
// data without importing the reconciler's internal types.
type modelConfigEntry struct {
	ModelName           string         `json:"modelName"`
	ModelSpec           modelSpecInfo  `json:"modelSpec"`
	FineTunedWeightSpec *modelSpecInfo `json:"fineTunedWeightSpec,omitempty"`
}

type modelSpecInfo struct {
	StorageURI         *string `json:"storageUri,omitempty"`
	ModelType          *string `json:"modelType,omitempty"`
	ModelFormatName    *string `json:"modelFormatName,omitempty"`
	ModelFormatVersion *string `json:"modelFormatVersion,omitempty"`
}

// unmarshalModelConfigEntries reads ConfigMap.Data[ModelConfigKey]
// (the JSON-encoded []ModelConfigEntry) and returns the decoded
// slice. Fails the spec via the supplied Gomega assertion if the
// payload is missing or malformed.
func unmarshalModelConfigEntries(g Gomega, cm *corev1.ConfigMap) []modelConfigEntry {
	raw, ok := cm.Data[constants.ModelConfigKey]
	g.Expect(ok).To(BeTrue(), "ConfigMap.Data[%q] missing", constants.ModelConfigKey)
	var entries []modelConfigEntry
	g.Expect(json.Unmarshal([]byte(raw), &entries)).To(Succeed())
	return entries
}
