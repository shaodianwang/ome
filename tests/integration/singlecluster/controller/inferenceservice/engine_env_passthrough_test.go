package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// ISVC pass-through specs. The merge contract (utils/merging.go:
// MergeEngineSpec → mergeSpec via strategic merge patch) says: the
// runtime supplies the BASE container; the ISVC's overrides are
// merged on top. For containers, the merge strategy is "merge by
// name", so custom env vars on the ISVC's Engine.Runner.Env are
// appended to the runtime's runner Env list and surface in the final
// Deployment's pod template.
var _ = ginkgo.Describe("InferenceService Engine.Runner pass-through", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pass-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ISVC Engine.Runner.Env vars merge into the runtime container's env on the resulting Deployment", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (runtime container has 1 env)")
		cbm := v1beta1testing.MakeClusterBaseModel("pass-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("pass-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
				Env: []corev1.EnvVar{
					{Name: "RUNTIME_LEVEL_VAR", Value: "from-runtime"},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.Runner that adds a CUSTOM env var on top of the runtime's runner")
		// Strategic merge patch on container env merges by name. Both
		// vars should survive the merge: the runtime-provided one and
		// the ISVC-provided one.
		isvc := v1beta1testing.MakeInferenceService("pass-isvc", ns.Name).
			Model("pass-llama").
			Runtime("pass-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{
						Name: "ome-container", // must match the runtime runner's name to merge
						Env: []corev1.EnvVar{
							{Name: "ISVC_LEVEL_VAR", Value: "from-isvc"},
						},
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the resulting Deployment to carry BOTH env vars on the runner container")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())

			// Find the ome-container by name (other sidecars may exist).
			var runner *corev1.Container
			for i := range containers {
				if containers[i].Name == "ome-container" {
					runner = &containers[i]
					break
				}
			}
			g.Expect(runner).NotTo(BeNil(), "ome-container must be present on the Deployment")

			envByKey := map[string]string{}
			for _, e := range runner.Env {
				envByKey[e.Name] = e.Value
			}
			g.Expect(envByKey).To(HaveKeyWithValue("RUNTIME_LEVEL_VAR", "from-runtime"),
				"runtime-supplied env var must survive the merge")
			g.Expect(envByKey).To(HaveKeyWithValue("ISVC_LEVEL_VAR", "from-isvc"),
				"ISVC-supplied env var must be present after the merge")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
