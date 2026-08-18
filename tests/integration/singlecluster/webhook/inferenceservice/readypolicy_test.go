package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Admission specs for the multi-pod readyPolicy=None rejection. The
// tolerated-stored-object half of the ratchet (an object already stored
// with the violating combination keeps admitting unchanged writes) lives
// in the unit tests: this suite's admission chain rejects the violating
// CREATE, so no such object can be stored here in the first place.
var _ = ginkgo.Describe("InferenceService readyPolicy admission", func() {
	ginkgo.BeforeEach(func() {
		ensureNamespace()
	})

	readyPolicy := func(p v1beta1.InstanceReadyPolicy) *v1beta1.InstanceReadyPolicy { return &p }
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}}
	}
	multiPodEngine := func(policy *v1beta1.InstanceReadyPolicy) *v1beta1.EngineSpec {
		size := 2
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{Runner: runner()},
			Worker: &v1beta1.WorkerSpec{Size: &size, Runner: runner()},
		}
		if policy != nil {
			engine.Lifecycle = &v1beta1.LifecycleSpec{ReadyPolicy: policy}
		}
		return engine
	}
	readyPolicyISVC := func(name string, engine *v1beta1.EngineSpec) *v1beta1.InferenceService {
		return v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Runtime("rp-runtime").
			AutoSync(false).
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Obj()
	}

	ginkgo.It("rejects CREATE of a multi-pod OMENative engine with readyPolicy None", func() {
		isvc := readyPolicyISVC("rp-multi-none", multiPodEngine(readyPolicy(v1beta1.InstanceReadyPolicyNone)))
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("engine.lifecycle.readyPolicy"))
		Expect(err.Error()).To(ContainSubstring("per-pod readiness reporting is not yet supported"))
	})

	ginkgo.It("accepts CREATE of a multi-pod OMENative engine with readyPolicy AllPodReady", func() {
		isvc := readyPolicyISVC("rp-multi-allpod", multiPodEngine(readyPolicy(v1beta1.InstanceReadyPolicyAllPodReady)))
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("accepts CREATE of a single-pod OMENative engine with readyPolicy None", func() {
		engine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}},
			},
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Lifecycle: &v1beta1.LifecycleSpec{ReadyPolicy: readyPolicy(v1beta1.InstanceReadyPolicyNone)},
			},
		}
		isvc := readyPolicyISVC("rp-single-none", engine)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("rejects UPDATE that introduces readyPolicy None on a multi-pod OMENative engine", func() {
		isvc := readyPolicyISVC("rp-ratchet", multiPodEngine(readyPolicy(v1beta1.InstanceReadyPolicyAllPodReady)))
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		isvc.Spec.Engine.Lifecycle.ReadyPolicy = readyPolicy(v1beta1.InstanceReadyPolicyNone)
		err := k8sClient.Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("engine.lifecycle.readyPolicy"))
		Expect(err.Error()).To(ContainSubstring("per-pod readiness reporting is not yet supported"))
	})
})
