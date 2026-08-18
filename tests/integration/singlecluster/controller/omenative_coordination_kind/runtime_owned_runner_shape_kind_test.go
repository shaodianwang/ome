package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Runtime-owned runner shape", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "runtime-runner-shape-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("preserves runtime lifecycle and uses its runner shape for per-revision routing", func() {
		const isvcName = "runtime-runner-shape"
		workerSize := 2
		restartPolicy := v1beta1.InstanceRestartPolicyRecreateInstance
		readyPolicy := v1beta1.InstanceReadyPolicyAllPodReady
		runtimePodSpec := func() v1beta1.PodSpec {
			return v1beta1.PodSpec{Containers: []corev1.Container{{
				Name:  constants.MainContainerName,
				Image: "fake-serving:v1",
				Ports: fakeServingPorts(),
			}}}
		}

		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithEngineImage("fake-serving:v1"),
			WithoutDecoder())
		fix.CSR.Spec.EngineConfig = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Lifecycle: &v1beta1.LifecycleSpec{
					RestartPolicy: &restartPolicy,
					ReadyPolicy:   &readyPolicy,
				},
			},
			Leader: &v1beta1.LeaderSpec{PodSpec: runtimePodSpec()},
			Worker: &v1beta1.WorkerSpec{
				Size:    &workerSize,
				PodSpec: runtimePodSpec(),
			},
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var revisionHash string
		Eventually(func(g Gomega) {
			stored := freshISVC(g, ns.Name, isvcName)
			g.Expect(stored.Spec.Engine).NotTo(BeNil())
			if stored.Spec.Engine == nil {
				return
			}
			g.Expect(stored.Spec.Engine.Leader).To(BeNil())
			g.Expect(stored.Spec.Engine.Worker).To(BeNil())
			g.Expect(stored.Spec.Engine.Lifecycle).NotTo(BeNil())
			if stored.Spec.Engine.Lifecycle == nil {
				return
			}
			g.Expect(stored.Spec.Engine.Lifecycle.RestartPolicy).To(BeNil())
			g.Expect(stored.Spec.Engine.Lifecycle.ReadyPolicy).To(BeNil())

			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Runners).To(HaveLen(2))
			if len(ir.Spec.Runners) != 2 {
				return
			}
			g.Expect(ir.Spec.Runners[0].Name).To(Equal(v1beta1.RunnerNameLeader))
			g.Expect(ir.Spec.Runners[0].Size).To(Equal(int32(1)))
			g.Expect(ir.Spec.Runners[1].Name).To(Equal(v1beta1.RunnerNameWorker))
			g.Expect(ir.Spec.Runners[1].Size).To(Equal(int32(workerSize)))
			g.Expect(ir.Spec.Lifecycle).NotTo(BeNil())
			if ir.Spec.Lifecycle == nil {
				return
			}
			g.Expect(ir.Spec.Lifecycle.RestartPolicy).NotTo(BeNil())
			g.Expect(ir.Spec.Lifecycle.ReadyPolicy).NotTo(BeNil())
			if ir.Spec.Lifecycle.RestartPolicy != nil {
				g.Expect(*ir.Spec.Lifecycle.RestartPolicy).To(Equal(restartPolicy))
			}
			if ir.Spec.Lifecycle.ReadyPolicy != nil {
				g.Expect(*ir.Spec.Lifecycle.ReadyPolicy).To(Equal(readyPolicy))
			}
			g.Expect(ir.Spec.Lifecycle.UpdateStrategy).NotTo(BeNil())
			if ir.Spec.Lifecycle.UpdateStrategy != nil {
				g.Expect(ir.Spec.Lifecycle.UpdateStrategy.Type).To(Equal(v1beta1.UpdateStrategySurgeThenDrain))
			}
			g.Expect(ir.Status.InstanceStatuses).To(HaveLen(1))
			if len(ir.Status.InstanceStatuses) == 1 {
				status := ir.Status.InstanceStatuses[0]
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(status.PodCount).To(Equal(int32(3)))
				g.Expect(status.ReadyPodCount).To(BeZero())
				g.Expect(status.ScheduledPodCount).To(BeZero())
				g.Expect(status.NodesOccupied).To(BeNil())
			}

			pods := listOMENativePods(g, ns.Name, isvcName)
			enginePods := make([]corev1.Pod, 0, 3)
			for _, pod := range pods {
				if pod.Labels[constants.OMEComponentLabel] == string(v1beta1.EngineComponent) {
					enginePods = append(enginePods, pod)
				}
			}
			g.Expect(enginePods).To(HaveLen(3))
			for _, pod := range enginePods {
				hash := pod.Labels[query.LabelRevisionHash]
				g.Expect(hash).NotTo(BeEmpty())
				if revisionHash == "" {
					revisionHash = hash
				}
				g.Expect(hash).To(Equal(revisionHash))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		component := v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)
		routingName := query.PerRevisionServiceName(isvcName, component, revisionHash)
		headlessName := query.PerRevisionHeadlessServiceName(isvcName, component, revisionHash)

		// Workers may not expose the customer endpoint, so revision routing
		// selects the leader while peer discovery remains gang-wide.
		Eventually(func(g Gomega) {
			routing := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: ns.Name, Name: routingName}, routing)).To(Succeed())
			g.Expect(routing.Spec.Selector).To(HaveKeyWithValue(
				query.LabelRunner, string(v1beta1.RunnerNameLeader)))
			g.Expect(routing.Spec.Selector).To(HaveKeyWithValue(query.LabelPodOrdinal, "0"))

			headless := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: ns.Name, Name: headlessName}, headless)).To(Succeed())
			g.Expect(headless.Spec.Selector).NotTo(HaveKey(query.LabelRunner))
			g.Expect(headless.Spec.Selector).NotTo(HaveKey(query.LabelPodOrdinal))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		endpointCount := func(g Gomega, serviceName string) int {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(k8sClient.List(suiteCtx, slices,
				client.InNamespace(ns.Name),
				client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
			)).To(Succeed())
			g.Expect(slices.Items).NotTo(BeEmpty())
			total := 0
			for _, slice := range slices.Items {
				total += len(slice.Endpoints)
			}
			return total
		}
		Eventually(func(g Gomega) {
			g.Expect(endpointCount(g, routingName)).To(Equal(1))
			g.Expect(endpointCount(g, headlessName)).To(Equal(3))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
