package inferenceservice

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PDDisaggregated-mode InferenceService specs.
//
// Trigger condition: BOTH Engine AND Decoder are non-nil
// (controller.go:280). Each component independently picks its mode
// (RawDeployment / MultiNode).
//
// In the simplest case (both engine + decoder are RawDeployment), we
// expect TWO Deployments to materialize — one per component, each
// using the matching runner image declared on the runtime.
var _ = ginkgo.Describe("InferenceService PDDisaggregated", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		// Best-effort LWS cleanup before namespace deletion (the
		// mixed-mode spec produces an LWS for the engine).
		lwss := &lwsv1.LeaderWorkerSetList{}
		_ = k8sClient.List(suiteCtx, lwss, client.InNamespace(ns.Name))
		for i := range lwss.Items {
			_ = k8sClient.Delete(suiteCtx, &lwss.Items[i])
		}
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

		// The canonical runtime pattern (config/runtimes/srt/meta/*-pd-rt.yaml)
		// puts the engine container on .spec.engineConfig.runner and the
		// decoder container on .spec.decoderConfig.runner. The runtime, not
		// the ISVC, decides what images each component runs.
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

	ginkgo.It("Mixed: Engine MultiNode + Decoder Raw → 1 LWS (engine) + 1 Deployment (decoder)", func() {
		// Validates that engine + decoder pick their MODE INDEPENDENTLY.
		// determineComponentDeploymentMode runs separately on each
		// component spec (utils/deployment.go); MultiNode for engine
		// because Leader/Worker is set; RawDeployment for decoder
		// because Leader/Worker is NOT set.
		ginkgo.By("creating BaseModel + Runtime baseline (engine carries leader/worker runners; decoder carries a single runner)")
		cbm := v1beta1testing.MakeClusterBaseModel("pdmix-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("pdmix-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			// Engine in MultiNode: per-role runners on the runtime
			EngineLeaderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:engine-leader"}).
			EngineWorkerRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:engine-worker"}).
			// Decoder in Raw: single runner on the runtime
			DecoderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:decoder"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC: engine has Leader+Worker (MultiNode trigger), decoder is empty (Raw)")
		isvc := v1beta1testing.MakeInferenceService("pdmix-isvc", ns.Name).
			Model("pdmix-llama").
			Runtime("pdmix-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				// MultiNode trigger: Leader/Worker non-nil. Empty
				// stubs delegate to runtime's leader/worker runners.
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptr.To(2)},
			}).
			Decoder(&v1beta1.DecoderSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting 1 LWS for engine + 1 Deployment for decoder, with the right images")
		Eventually(func(g Gomega) {
			lwss := &lwsv1.LeaderWorkerSetList{}
			g.Expect(k8sClient.List(suiteCtx, lwss, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(lwss.Items).To(HaveLen(1),
				"engine in MultiNode mode must produce exactly one LWS")
			lws := &lwss.Items[0]
			g.Expect(lws.Spec.LeaderWorkerTemplate.LeaderTemplate).NotTo(BeNil())
			leaderC := lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Containers
			g.Expect(leaderC).NotTo(BeEmpty())
			g.Expect(leaderC[0].Image).To(Equal("ghcr.io/test/runtime:engine-leader"),
				"engine LWS leader must use the runtime's engine-leader image")

			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1),
				"decoder in RawDeployment mode must produce exactly one Deployment (engine produced an LWS, not a Deployment)")
			decoderDep := &deps.Items[0]
			g.Expect(strings.Contains(decoderDep.Name, "decoder")).To(BeTrue(),
				"the lone Deployment should be the decoder's (got %q)", decoderDep.Name)
			decoderC := decoderDep.Spec.Template.Spec.Containers
			g.Expect(decoderC).NotTo(BeEmpty())
			g.Expect(decoderC[0].Image).To(Equal("ghcr.io/test/runtime:decoder"),
				"decoder Deployment must use the runtime's decoder image")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
