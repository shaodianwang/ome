package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Pod-spec field passthrough from ISVC.Spec.Engine into the
// produced Deployment's pod template. The merging logic
// (utils/merging.go) blends the runtime's base spec with the ISVC's
// overrides; pod-level fields like Tolerations, NodeSelector, and
// ImagePullSecrets are user-only (the runtime doesn't typically
// supply them) so they should appear verbatim on the Deployment.
//
// Catches a real class of regressions: someone changes a spec field
// but the controller silently drops it because the merge function
// doesn't propagate that field.
var _ = ginkgo.Describe("InferenceService Engine pod-spec passthrough", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-passthrough-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// makeBaseModelAndRuntime sets up the cluster-scope BaseModel +
	// matching ClusterServingRuntime every spec needs to even reach
	// the Deployment-creation step. Returned BM/CSR are auto-cleaned
	// via DeferCleanup so each spec gets a fresh pair.
	makeBaseModelAndRuntime := func(suffix string) (string, string) {
		cbmName := "passthrough-cbm-" + suffix
		runtimeName := "passthrough-rt-" + suffix
		cbm := v1beta1testing.MakeClusterBaseModel(cbmName).
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		return cbmName, runtimeName
	}

	// firstDeployment polls for a Deployment in the ISVC's namespace
	// and returns it once present. Reused by every spec since each
	// asserts on a different field of the same shape.
	firstDeployment := func() *appsv1.Deployment {
		var dep *appsv1.Deployment
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty())
			dep = &deps.Items[0]
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		return dep
	}

	ginkgo.It("Tolerations on Engine.PodSpec propagate to the Deployment's pod template", func() {
		cbm, rt := makeBaseModelAndRuntime("toleration")
		isvc := v1beta1testing.MakeInferenceService("passthrough-toleration", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
					Tolerations: []corev1.Toleration{{
						Key:      "nvidia.com/gpu",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		Expect(dep.Spec.Template.Spec.Tolerations).To(ContainElement(corev1.Toleration{
			Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
		}))
	})

	ginkgo.It("NodeSelector on Engine.PodSpec propagates to the Deployment's pod template", func() {
		cbm, rt := makeBaseModelAndRuntime("nodesel")
		isvc := v1beta1testing.MakeInferenceService("passthrough-nodesel", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers:   []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
					NodeSelector: map[string]string{"node-class": "gpu-h100"},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		Expect(dep.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("node-class", "gpu-h100"))
	})

	ginkgo.It("Affinity (NodeAffinity required) on Engine.PodSpec propagates", func() {
		cbm, rt := makeBaseModelAndRuntime("affinity")
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "topology.kubernetes.io/zone",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"us-east-1a", "us-east-1b"},
						}},
					}},
				},
			},
		}
		isvc := v1beta1testing.MakeInferenceService("passthrough-affinity", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
					Affinity:   affinity,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		Expect(dep.Spec.Template.Spec.Affinity).NotTo(BeNil())
		Expect(dep.Spec.Template.Spec.Affinity.NodeAffinity).NotTo(BeNil())
		Expect(dep.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).NotTo(BeNil())
		got := dep.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		Expect(got).To(HaveLen(1))
		Expect(got[0].MatchExpressions).To(HaveLen(1))
		Expect(got[0].MatchExpressions[0].Values).To(ConsistOf("us-east-1a", "us-east-1b"))
	})

	ginkgo.It("ImagePullSecrets on Engine.PodSpec propagate to the Deployment's pod template", func() {
		cbm, rt := makeBaseModelAndRuntime("ips")
		isvc := v1beta1testing.MakeInferenceService("passthrough-ips", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers:       []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-creds"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		Expect(dep.Spec.Template.Spec.ImagePullSecrets).To(ContainElement(corev1.LocalObjectReference{Name: "registry-creds"}))
	})

	ginkgo.It("Container Resources (requests + limits) on Engine.Runner propagate to the Deployment container", func() {
		// Resources go on the container, not the pod. utils/merging.go
		// has a non-trivial path for resource merging — runtime
		// supplies a base, ISVC overrides per-key, and any
		// AcceleratorClass injects extended resources on top.
		cbm, rt := makeBaseModelAndRuntime("resources")
		isvc := v1beta1testing.MakeInferenceService("passthrough-resources", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("4Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4"),
								corev1.ResourceMemory: resource.MustParse("8Gi"),
							},
						},
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		var ome *corev1.Container
		for i := range dep.Spec.Template.Spec.Containers {
			if dep.Spec.Template.Spec.Containers[i].Name == "ome-container" {
				ome = &dep.Spec.Template.Spec.Containers[i]
				break
			}
		}
		Expect(ome).NotTo(BeNil(), "Deployment must contain the ome-container we configured")
		Expect(ome.Resources.Requests.Cpu().String()).To(Equal("2"))
		Expect(ome.Resources.Requests.Memory().String()).To(Equal("4Gi"))
		Expect(ome.Resources.Limits.Cpu().String()).To(Equal("4"))
		Expect(ome.Resources.Limits.Memory().String()).To(Equal("8Gi"))
	})

	ginkgo.It("InitContainers on Engine.PodSpec propagate to the Deployment's pod template", func() {
		cbm, rt := makeBaseModelAndRuntime("init")
		isvc := v1beta1testing.MakeInferenceService("passthrough-init", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
					InitContainers: []corev1.Container{{
						Name:    "user-init",
						Image:   "busybox:1.36",
						Command: []string{"sh", "-c", "echo init"},
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		dep := firstDeployment()
		names := []string{}
		for _, c := range dep.Spec.Template.Spec.InitContainers {
			names = append(names, c.Name)
		}
		Expect(names).To(ContainElement("user-init"),
			"user-supplied InitContainer must appear on the Deployment's pod template (got %v)", names)
	})
})
