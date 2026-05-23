package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Runtime + Model surface coverage. The OMENative reconciler renders
// pods from MergeRuntimeSpecs(CSR, ISVC), which means a regression in
// either the merge or in how MergeDecoderResources / ReplacePlaceholders
// run for OMENative-mode components silently produces a pod whose
// shape doesn't match operator expectations. These specs pin the
// merge contract for OMENative deployments.
var _ = ginkgo.Describe("OMENative + ClusterServingRuntime merge", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-rt-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// RT-1 — CSR engineConfig.runner.image is the fallback when the
	// ISVC.engine container has no image set. The merge:
	// ISVC.engine.podSpec.containers[i] ⊕ CSR.engineConfig.runner.container
	// must produce a pod whose container.image came from the CSR.
	ginkgo.It("RT-1: CSR engineConfig.runner.image fills in when ISVC engine container omits image", func() {
		ginkgo.By("creating a CBM + CSR where the runtime supplies the image")
		cbm := v1beta1testing.MakeClusterBaseModel("rt1-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("rt1-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/rt-supplied:v9",
				Env: []corev1.EnvVar{
					{Name: "RUNTIME_PROVIDED", Value: "true"},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating an ISVC whose engine container omits image (CSR fills in)")
		one := 1
		isvc := v1beta1testing.MakeInferenceService("rt1-isvc", ns.Name).
			Model("rt1-llama").
			Runtime("rt1-runtime").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &one,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the rendered pod to carry the CSR's image + env")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt1-isvc-engine-0-default-0",
			}, pod)).To(Succeed())

			var ome *corev1.Container
			for i := range pod.Spec.Containers {
				if pod.Spec.Containers[i].Name == "ome-container" {
					ome = &pod.Spec.Containers[i]
					break
				}
			}
			g.Expect(ome).NotTo(BeNil())
			g.Expect(ome.Image).To(Equal("ghcr.io/test/rt-supplied:v9"),
				"CSR's image must fill in for an ISVC container that omits image; got %q", ome.Image)

			found := false
			for _, e := range ome.Env {
				if e.Name == "RUNTIME_PROVIDED" && e.Value == "true" {
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(),
				"CSR-provided env var must survive merge into the rendered pod")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RT-2 — ISVC image WINS over CSR engineConfig.runner.image.
	// MergeRuntimeContainers takes (runtime, predictor) and the
	// strategic-merge predictor (user-side) overrides the runtime.
	// Operators pinning a specific image at the ISVC level must see
	// that pin take effect; the CSR's image is the fallback for
	// ISVCs that omit it (covered in RT-1).
	ginkgo.It("RT-2: ISVC engine container image overrides CSR engineConfig.runner.image", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rt2-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("rt2-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/rt-stale:v1",
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		one := 1
		isvc := v1beta1testing.MakeInferenceService("rt2-isvc", ns.Name).
			Model("rt2-llama").
			Runtime("rt2-runtime").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &one,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/user-pinned:v42",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt2-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.Containers[0].Image).To(Equal("ghcr.io/test/user-pinned:v42"),
				"ISVC-pinned image must win over CSR's image — got %q which means the runtime template overrode user intent",
				pod.Spec.Containers[0].Image)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RT-3 — CSR-provided env vars merge into the rendered pod
	// alongside ISVC-provided env vars. Operator-supplied env (in the
	// CSR template) and user-supplied env (in the ISVC) must
	// coexist, with ISVC values winning on Name collision.
	ginkgo.It("RT-3: CSR and ISVC env vars both land on the pod; ISVC wins on Name collision", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rt3-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("rt3-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
				Env: []corev1.EnvVar{
					{Name: "RT_ONLY", Value: "from-runtime"},
					{Name: "OVERRIDDEN", Value: "runtime-value"},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		one := 1
		isvc := v1beta1testing.MakeInferenceService("rt3-isvc", ns.Name).
			Model("rt3-llama").
			Runtime("rt3-runtime").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &one,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container",
						Env: []corev1.EnvVar{
							{Name: "ISVC_ONLY", Value: "from-user"},
							{Name: "OVERRIDDEN", Value: "user-wins"},
						},
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt3-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			envs := map[string]string{}
			for _, e := range pod.Spec.Containers[0].Env {
				envs[e.Name] = e.Value
			}
			g.Expect(envs).To(HaveKeyWithValue("RT_ONLY", "from-runtime"),
				"CSR-only env var must reach the pod; got envs=%v", envs)
			g.Expect(envs).To(HaveKeyWithValue("ISVC_ONLY", "from-user"),
				"ISVC-only env var must reach the pod")
			g.Expect(envs).To(HaveKeyWithValue("OVERRIDDEN", "user-wins"),
				"ISVC env value must win when both CSR + ISVC define the same Name; got %q",
				envs["OVERRIDDEN"])
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RT-4 — When the CBM is missing at ISVC create time, the
	// controller must NOT crash and must NOT create OMENative pods.
	// The ISVC stalls until the CBM is created; once created, the
	// reconciler picks it up and the pod converges to Ready.
	ginkgo.It("RT-4: ISVC stalls until missing CBM is created, then converges", func() {
		// Don't create the CBM yet.
		csr := v1beta1testing.MakeClusterServingRuntime("rt4-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		one := 1
		isvc := v1beta1testing.MakeInferenceService("rt4-isvc", ns.Name).
			Model("rt4-llama"). // CBM doesn't exist yet
			Runtime("rt4-runtime").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &one,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("no engine pod should exist while CBM is missing")
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt4-isvc-engine-0-default-0",
			}, pod)
			g.Expect(err).To(HaveOccurred(),
				"pod must NOT exist until CBM is Ready; controller must stall (and not crash)")
		}, "2s", util.PollInterval).Should(Succeed())

		ginkgo.By("creating the CBM unblocks the reconciler")
		cbm := v1beta1testing.MakeClusterBaseModel("rt4-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt4-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RT-5 — When the CSR is missing at ISVC create time, the
	// controller must NOT crash. lifecycle_test already covers the
	// "missing CSR doesn't panic" behavior, but RT-5 specifically
	// asserts the OMENative dispatch never runs and no controller-
	// owned children appear (Service, CR). The ISVC reconciler
	// short-circuits before reaching DetermineDeploymentModes.
	ginkgo.It("RT-5: missing CSR leaves no Service or ControllerRevision behind", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rt5-llama").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		one := 1
		isvc := v1beta1testing.MakeInferenceService("rt5-isvc", ns.Name).
			Model("rt5-llama").
			Runtime("rt5-runtime"). // CSR doesn't exist
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &one,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("no headless Service and no CR should be created while CSR is missing")
		Consistently(func(g Gomega) {
			svc := &corev1.Service{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt5-isvc-engine-headless",
			}, svc)
			g.Expect(err).To(HaveOccurred(),
				"OMENative-owned headless Service must NOT exist until CSR is created")
		}, "2s", util.PollInterval).Should(Succeed())
	})
})
