package inferenceservice_defaulted

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Precedence of an unset field, pinned end to end on
// terminationGracePeriodSeconds: the InferenceService's value, then the
// ServingRuntime's, then the deploy block of inferenceservice-config (this
// suite configures the chart's 600). The rendered Deployment is the
// observable; the stored InferenceService is never written.
var _ = ginkgo.Describe("InferenceService defaults resolve after the runtime merge", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "precedence-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	expectGracePeriod := func(name string, seconds int64) {
		ginkgo.GinkgoHelper()
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: constants.EngineServiceName(name)}, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Template.Spec.TerminationGracePeriodSeconds).To(HaveValue(Equal(seconds)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	rawEngine := func(gracePeriod *int64) *v1beta1.EngineSpec {
		return &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			},
			PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: gracePeriod},
		}
	}

	expectStoredUnset := func(name string) {
		ginkgo.GinkgoHelper()
		stored := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, stored)).To(Succeed())
		Expect(stored.Spec.Engine.TerminationGracePeriodSeconds).To(BeNil(), "the stored object keeps the field as authored")
	}

	ginkgo.It("uses the InferenceService's value over the runtime's", func() {
		createModelAndRuntime("prec-isvc", ptr.To(int64(900)))
		isvc := v1beta1testing.MakeInferenceService("prec-isvc", ns.Name).
			Model("prec-isvc-cbm").Runtime("prec-isvc-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(rawEngine(ptr.To(int64(1200)))).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		expectGracePeriod(isvc.Name, 1200)
	})

	ginkgo.It("uses the runtime's value over the cluster default when the InferenceService sets none", func() {
		createModelAndRuntime("prec-runtime", ptr.To(int64(900)))
		isvc := v1beta1testing.MakeInferenceService("prec-runtime", ns.Name).
			Model("prec-runtime-cbm").Runtime("prec-runtime-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(rawEngine(nil)).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		expectGracePeriod(isvc.Name, 900)
		expectStoredUnset(isvc.Name)
	})

	ginkgo.It("uses the cluster default when neither the InferenceService nor the runtime sets one", func() {
		createModelAndRuntime("prec-config", nil)
		isvc := v1beta1testing.MakeInferenceService("prec-config", ns.Name).
			Model("prec-config-cbm").Runtime("prec-config-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(rawEngine(nil)).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		expectGracePeriod(isvc.Name, configuredGracePeriodSeconds)
		expectStoredUnset(isvc.Name)
	})
})
