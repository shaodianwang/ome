package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// failedRepairHealLifecycleJSON is the seconds-scale retry ladder, which
// paces a parked repair's re-arms, plus the short stuck-pod grace that
// parks the repair inside the suite budget.
const failedRepairHealLifecycleJSON = `{"updateRetry":{"maxAttempts":3,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"10s"}`

// A repair parked at Failed gets an exit of its own when its cause
// disappears. The engine pod takes an env value from a ConfigMap key;
// with the key gone, the rebuild the restart pass runs after a pod loss
// cannot create its container (CreateContainerConfigError), the stuck-pod
// grace parks the repair at Failed with its Restart preserved, and once
// the key is back the Instance returns to Ready and serving on the same
// revision: no spec bump, no reset mailbox.
var _ = ginkgo.Describe("failed repair — the cause disappears", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "repair-heal-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("a ConfigMap key recreated after the repair parked returns the Instance to serving with no bump and no mailbox", func() {
		const name = "repair-heal"
		const cmName = "repair-heal-env"
		configureLifecycleConfig(failedRepairHealLifecycleJSON)

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: ns.Name},
			Data:       map[string]string{"mode": "serve"},
		}
		Expect(k8sClient.Create(suiteCtx, cm)).To(Succeed())

		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		policy := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &policy}
		engine := &fix.ISVC.Spec.Engine.PodSpec.Containers[0]
		engine.Env = append(engine.Env, corev1.EnvVar{
			Name: "ENGINE_MODE",
			ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				Key:                  "mode",
			}},
		})
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var runningRevision string
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].RunningRevision).NotTo(BeEmpty())
			runningRevision = insts[0].RunningRevision
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The cause: the key the container needs is gone. The running pod
		// keeps its value; only a rebuild feels it.
		Eventually(func(g Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(cm), fresh)).To(Succeed())
			fresh.Data = map[string]string{}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The repair: losing the pod opens a Restart, whose rebuild cannot
		// create its container.
		for _, pod := range listOMENativePods(Default, ns.Name, name) {
			pod := pod
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &pod))).To(Succeed())
		}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceFailed),
				"the rebuild must park at Failed once its pod is wedged past the stuck-pod grace")
			g.Expect(insts[0].Operation).NotTo(BeNil())
			g.Expect(insts[0].Operation.Type).To(Equal(v1beta1.InstanceOperationRestart),
				"a parked repair keeps its Restart on the row")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The cause disappears.
		Eventually(func(g Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(cm), fresh)).To(Succeed())
			fresh.Data = map[string]string{"mode": "serve"}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The exit: Ready and serving on the revision it ran, with no
		// operator action. Either the kubelet's own retry starts the parked
		// pod and the repair resumes its promote, or the ladder rebuilds it;
		// both end here.
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Operation).To(BeNil())
			g.Expect(insts[0].RunningRevision).To(Equal(runningRevision), "the Instance heals on the revision it ran")
			pods := listOMENativePods(g, ns.Name, name)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(podIsReady(&pods[0])).To(BeTrue(), "the healed pod must be Ready")
			g.Expect(podIsServing(&pods[0])).To(BeTrue(), "the healed pod must carry the serving gate")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// No bump: the roll target never moved. No mailbox: the operator
		// wrote nothing.
		ir := freshInferenceReplica(Default, ns.Name, name, v1beta1.EngineComponent)
		Expect(ir.Status.UpdateRevision).To(Equal(runningRevision))
		isvc := freshISVC(Default, ns.Name, name)
		Expect(isvc.Annotations).NotTo(HaveKey(constants.ResetInstancesAnnotationKey))

		Consistently(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindSteadyStateWindow).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// podIsServing reports whether the controller-owned serving gate is True
// on the pod.
func podIsServing(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == query.ServingConditionType {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
