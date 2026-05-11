package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// MultiNode-mode InferenceService specs.
//
// Trigger condition (utils/deployment.go:DetermineEngineDeploymentMode):
//
//	engine.Leader != nil OR engine.Worker != nil  →  MultiNode
//
// In MultiNode mode the controller produces a leaderworkerset.x-k8s.io
// LeaderWorkerSet child instead of a regular Deployment. The LWS
// bundle MUST be installed (suite_test.go opts in via OptionalCRDs).
//
// LWS shape contract (reconcilers/lws/lws_reconciler.go):
//
//	LWS.Spec.Replicas                  = ComponentExt.MinReplicas (default 1)
//	LWS.Spec.LeaderWorkerTemplate.Size = Worker.Size + 1 (the +1 is the leader)
var _ = ginkgo.Describe("InferenceService MultiNode", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-multinode-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		// Best-effort cleanup of any LWSes in the test namespace
		// before the namespace itself is dropped.
		lwss := &lwsv1.LeaderWorkerSetList{}
		_ = k8sClient.List(suiteCtx, lwss, client.InNamespace(ns.Name))
		for i := range lwss.Items {
			_ = k8sClient.Delete(suiteCtx, &lwss.Items[i])
		}
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Engine.Leader+Worker → LWS with Replicas=MinReplicas and Size=Worker.Size+1", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (engineConfig.runner is the canonical engine container)")
		cbm := v1beta1testing.MakeClusterBaseModel("multinode-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// MultiNode: each role (leader/worker) carries its own runner
		// on the runtime. config/runtimes/srt/deepseek-rdma-rt.yaml is
		// the canonical example: engineConfig.leader.runner +
		// engineConfig.worker.runner, NOT engineConfig.runner.
		csr := v1beta1testing.MakeClusterServingRuntime("multinode-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineLeaderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:multinode-leader"}).
			EngineWorkerRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:multinode-worker"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.Leader + Engine.Worker(Size=3) + MinReplicas=2 (triggers MultiNode)")
		// Pick non-default values so a regression that hardcodes
		// Replicas=1 or Size=Worker.Size (without the leader+1) shows up.
		const minReplicas, workerSize = 2, 3
		isvc := v1beta1testing.MakeInferenceService("multinode-isvc", ns.Name).
			Model("multinode-llama-7b").
			Runtime("multinode-runtime").
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(minReplicas),
				},
				// Leader + Worker stubs are the multi-node trigger;
				// the runner image still comes from the runtime's
				// engineConfig.runner via MergeRuntimeSpecs.
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptr.To(workerSize)},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting EXACTLY one LeaderWorkerSet with Replicas=2 and Size=4 (workerSize+1 leader)")
		Eventually(func(g Gomega) {
			lwss := &lwsv1.LeaderWorkerSetList{}
			g.Expect(k8sClient.List(suiteCtx, lwss, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(lwss.Items).To(HaveLen(1),
				"MultiNode mode must produce exactly one LeaderWorkerSet")

			lws := &lwss.Items[0]
			g.Expect(lws.Spec.Replicas).NotTo(BeNil())
			g.Expect(*lws.Spec.Replicas).To(BeEquivalentTo(minReplicas),
				"LWS.Spec.Replicas must reflect Engine.MinReplicas (got %d, want %d)",
				*lws.Spec.Replicas, minReplicas)

			g.Expect(lws.Spec.LeaderWorkerTemplate.Size).NotTo(BeNil())
			g.Expect(*lws.Spec.LeaderWorkerTemplate.Size).To(BeEquivalentTo(workerSize+1),
				"LWS.Spec.LeaderWorkerTemplate.Size must be Worker.Size+1 (got %d, want %d)",
				*lws.Spec.LeaderWorkerTemplate.Size, workerSize+1)

			// Cross-check that the leader template's first container
			// inherited the runtime's engineConfig.runner image.
			g.Expect(lws.Spec.LeaderWorkerTemplate.LeaderTemplate).NotTo(BeNil(),
				"LWS must have a leader template")
			leaderContainers := lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Containers
			g.Expect(leaderContainers).NotTo(BeEmpty(),
				"LWS leader template must contain at least one container")
			g.Expect(leaderContainers[0].Image).To(Equal("ghcr.io/test/runtime:multinode-leader"),
				"LWS leader container must come from the runtime's engineConfig.leader.runner image")

			// Also verify the worker template inherited the runtime's
			// engineConfig.worker.runner image — confirms per-role
			// runner declarations flow through MergeEngineSpec.
			workerContainers := lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers
			g.Expect(workerContainers).NotTo(BeEmpty(),
				"LWS worker template must contain at least one container")
			g.Expect(workerContainers[0].Image).To(Equal("ghcr.io/test/runtime:multinode-worker"),
				"LWS worker container must come from the runtime's engineConfig.worker.runner image")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Worker.Size=0 edge case → LWS Size=1 (just the leader)", func() {
		// When the worker count is zero, the LWS is essentially a
		// single-pod set containing only the leader. The reconciler
		// adds 1 (for the leader) → Size=1.
		ginkgo.By("creating BaseModel + Runtime baseline (leader runner only)")
		cbm := v1beta1testing.MakeClusterBaseModel("mn-w0-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("mn-w0-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineLeaderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:leader-only"}).
			EngineWorkerRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:worker-unused"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.Worker.Size=0 (leader-only)")
		isvc := v1beta1testing.MakeInferenceService("mn-w0-isvc", ns.Name).
			Model("mn-w0-llama").
			Runtime("mn-w0-runtime").
			Engine(&v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptr.To(0)},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting LWS with Size=1 (0 workers + 1 leader)")
		Eventually(func(g Gomega) {
			lwss := &lwsv1.LeaderWorkerSetList{}
			g.Expect(k8sClient.List(suiteCtx, lwss, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(lwss.Items).To(HaveLen(1))
			lws := &lwss.Items[0]
			g.Expect(lws.Spec.LeaderWorkerTemplate.Size).NotTo(BeNil())
			g.Expect(*lws.Spec.LeaderWorkerTemplate.Size).To(BeEquivalentTo(1),
				"Worker.Size=0 + leader = Size=1")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
