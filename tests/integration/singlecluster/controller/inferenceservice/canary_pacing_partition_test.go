package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// The canary step partition is a rollout-control value: it travels to the
// InferenceReplica on spec.pacing.partition, and the user's
// spec.lifecycle.updateStrategy.rollingUpdate.partition is a verbatim copy the
// canary never rewrites. This spec drives the full ISVC reconcile against a
// real apiserver and reads the projected IR. envtest has no kubelet, so the
// IR's Ready=True/Staged condition for the held step is asserted by the KIND
// canary specs; here the projected fields are the observable truth.
var _ = ginkgo.Describe("InferenceService canary partition projection", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-pacing-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("projects a held step onto spec.pacing.partition and preserves the user's rollingUpdate.partition", func() {
		const prefix = "canary-pacing"
		modelName := prefix + "-model"
		runtimeName := prefix + "-runtime"
		isvcName := prefix + "-isvc"

		model := v1beta1testing.MakeClusterBaseModel(modelName).
			StorageURI("oci://n/example/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, model) })

		runtime := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineConfig(&v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{
						Name:  constants.MainContainerName,
						Image: "example.com/runtime:v1",
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

		// The user's own strategy carries a partition of 1; a canary must
		// not overwrite it.
		minReplicas := 4
		isvc := v1beta1testing.MakeInferenceService(isvcName, ns.Name).
			Model(modelName).
			Runtime(runtimeName).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
					MaxReplicas: 4,
					Lifecycle: &v1beta1.LifecycleSpec{
						UpdateStrategy: &v1beta1.UpdateStrategy{
							RollingUpdate: &v1beta1.RollingUpdate{Partition: ptr.To(int32(1))},
						},
					},
				},
			}).Obj()
		omeNativeMode := constants.OMENative
		isvc.Spec.DeploymentMode = &omeNativeMode
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		irKey := types.NamespacedName{Namespace: ns.Name, Name: isvcName + "-engine"}
		userPartition := func(ir *v1beta1.InferenceReplica) *int32 {
			lc := ir.Spec.Lifecycle
			if lc == nil || lc.UpdateStrategy == nil || lc.UpdateStrategy.RollingUpdate == nil {
				return nil
			}
			return lc.UpdateStrategy.RollingUpdate.Partition
		}

		ginkgo.By("projecting the user's partition verbatim with no pacing block before any canary")
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			g.Expect(userPartition(ir)).To(HaveValue(Equal(int32(1))))
			g.Expect(ir.Spec.Pacing).To(BeNil(), "no canary must project no pacing block")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("adding a manual canary plan (50% capacity, bare pause) and a new revision")
		Eventually(func(g Gomega) {
			cur := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: isvcName}, cur)).To(Succeed())
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			cur.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:v2",
			}}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("holding step 0 as spec.pacing.partition=2 while the user's partition stays 1")
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			g.Expect(ir.Spec.Pacing).NotTo(BeNil(), "a held canary step must project a pacing block")
			g.Expect(ir.Spec.Pacing.Partition).To(HaveValue(Equal(int32(2))),
				"50%% of 4 replicas holds 2 Instances on the stable revision")
			g.Expect(userPartition(ir)).To(HaveValue(Equal(int32(1))),
				"the canary must not rewrite the user's rollingUpdate.partition")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("keeping both values stable for as long as the step is held")
		Consistently(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			g.Expect(ir.Spec.Pacing).NotTo(BeNil())
			g.Expect(ir.Spec.Pacing.Partition).To(HaveValue(Equal(int32(2))))
			g.Expect(userPartition(ir)).To(HaveValue(Equal(int32(1))))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
