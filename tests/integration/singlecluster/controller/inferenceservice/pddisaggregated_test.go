package inferenceservice

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PDDisaggregated-mode InferenceService specs.
//
// Trigger condition: BOTH Engine AND Decoder are non-nil
// (controller.go:280). Each component independently picks its mode
// (RawDeployment / OMENative).
//
// In the simplest case (both engine + decoder are RawDeployment), we
// expect TWO Deployments to materialize — one per component, each
// using the matching runner image declared on the runtime. The
// multi-node (Leader/Worker → OMENative) PD topologies are covered by
// the KIND suite, where a real kubelet drives the InferenceReplica
// lifecycle (see CLAUDE.md — envtest is not used for OMENative).
var _ = ginkgo.Describe("InferenceService PDDisaggregated", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Engine + Decoder → 2 distinct Deployments using engine vs decoder runner images from the runtime", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (engineConfig.runner + decoderConfig.runner with DISTINCT images)")
		cbm := v1beta1testing.MakeClusterBaseModel("pd-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// The canonical PD runtime pattern puts the engine container on
		// .spec.engineConfig.runner and the decoder container on
		// .spec.decoderConfig.runner. The runtime, not the ISVC, decides
		// what images each component runs.
		csr := v1beta1testing.MakeClusterServingRuntime("pd-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:engine"}).
			DecoderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:decoder"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC with Engine{} + Decoder{} stubs to opt into PD-Disaggregated")
		// The Engine/Decoder stubs only exist to opt the ISVC into the
		// engine + decoder components; the runtime supplies the actual
		// containers via engineConfig.runner / decoderConfig.runner.
		isvc := v1beta1testing.MakeInferenceService("pd-isvc", ns.Name).
			Model("pd-llama-7b").
			Runtime("pd-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).
			Decoder(&v1beta1.DecoderSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting EXACTLY 2 Deployments — one running the engine image, one running the decoder image")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(2),
				"PDDisaggregated must produce exactly 2 Deployments (engine + decoder)")

			// Map: image -> the Deployment that uses it. We assert the
			// image set rather than positional ordering because the
			// list order is not guaranteed.
			imgToDep := map[string]*appsv1.Deployment{}
			for i := range deps.Items {
				dep := &deps.Items[i]
				containers := dep.Spec.Template.Spec.Containers
				g.Expect(containers).NotTo(BeEmpty(),
					"every Deployment must have at least one container")
				imgToDep[containers[0].Image] = dep
			}
			g.Expect(imgToDep).To(HaveKey("ghcr.io/test/runtime:engine"),
				"one Deployment must run the engine runner image declared on the runtime")
			g.Expect(imgToDep).To(HaveKey("ghcr.io/test/runtime:decoder"),
				"the other Deployment must run the decoder runner image declared on the runtime")

			// Cross-check the naming contract: engine + decoder
			// Deployments are distinguished by suffix derived from
			// constants.EngineComponent / constants.DecoderComponent.
			engineDep := imgToDep["ghcr.io/test/runtime:engine"]
			decoderDep := imgToDep["ghcr.io/test/runtime:decoder"]
			g.Expect(engineDep.Name).NotTo(Equal(decoderDep.Name),
				"engine + decoder Deployments must have distinct names")
			g.Expect(strings.Contains(decoderDep.Name, "decoder")).To(BeTrue(),
				"decoder Deployment name should be suffixed with 'decoder' (got %q)", decoderDep.Name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
