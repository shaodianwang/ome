package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A service over a replica the user created (spec.replicaRefs) keeps the
// role's stable Service in front of the replica's pods and mirrors the
// replica's counters onto its own status; it renders nothing, projects no
// replica of its own and writes nothing on the replica. The seeded ingress
// config disables ingress creation, so IngressReady holds and the top-level
// Ready condition follows EngineReady.
var _ = ginkgo.Describe("InferenceService over a referenced replica", ginkgo.Ordered, func() {
	const (
		replicaName = "pool-a"
		serviceName = "svc"
		// engineService is the stable Service of the engine role, named for
		// the service that fronts it, not for the replica.
		engineService = serviceName + "-engine"
	)
	var (
		ns   *corev1.Namespace
		ir   *v1beta1.InferenceReplica
		isvc *v1beta1.InferenceService
		// wantSpec is the replica spec as the user wrote it: the created spec,
		// then the same spec with the scaled replica count.
		wantSpec v1beta1.InferenceReplicaSpec
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "referenced-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
	})

	// expectReplicaPods asserts the replica runs n Ready pods, each controlled
	// by the replica itself. ReadyReplicas counts ContainersReady pods, but
	// PodReady also waits for the kubelet to fold in the serving gate, so the
	// pods get their own assertion.
	expectReplicaPods := func(g Gomega, n int) {
		pods := listOMENativePods(g, ns.Name, replicaName)
		g.Expect(pods).To(HaveLen(n))
		for i := range pods {
			pod := &pods[i]
			g.Expect(pod.DeletionTimestamp).To(BeNil(), "pod %s is terminating", pod.Name)
			g.Expect(podIsReady(pod)).To(BeTrue(), "pod %s should be Ready", pod.Name)
			owner := metav1.GetControllerOf(pod)
			g.Expect(owner).NotTo(BeNil(), "pod %s has no controller", pod.Name)
			g.Expect(owner.UID).To(Equal(ir.UID), "pod %s must be controlled by the replica", pod.Name)
		}
	}

	// expectReplicaUntouched asserts the service wrote nothing on the replica:
	// its spec is the user's, and nothing owns or annotates it.
	expectReplicaUntouched := func(g Gomega) *v1beta1.InferenceReplica {
		fresh := &v1beta1.InferenceReplica{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		g.Expect(equality.Semantic.DeepEqual(fresh.Spec, wantSpec)).To(BeTrue(), "spec of %s changed", replicaName)
		g.Expect(fresh.OwnerReferences).To(BeEmpty(), "a standalone replica has no owner")
		g.Expect(fresh.Annotations).To(BeEmpty(), "a standalone replica carries only the user's annotations")
		return fresh
	}

	// expectServiceMirror asserts the service reports the replica's counters
	// on its engine status and is Ready on them: the replica is its scale
	// target, and the block reports the service's own generation.
	expectServiceMirror := func(g Gomega, replicas int32) {
		c := freshISVC(g, ns.Name, serviceName)
		cs := c.Status.Components[v1beta1.EngineComponent]
		g.Expect(cs.Lifecycle).NotTo(BeNil())
		g.Expect(cs.Lifecycle.Replicas).To(Equal(replicas))
		g.Expect(cs.Lifecycle.ReadyReplicas).To(Equal(replicas))
		g.Expect(cs.Lifecycle.ObservedGeneration).To(Equal(c.Generation))
		g.Expect(cs.ScaleTargetRef).NotTo(BeNil())
		g.Expect(cs.ScaleTargetRef.Name).To(Equal(replicaName))
		g.Expect(c.Status.IsConditionReady(v1beta1.EngineReady)).To(BeTrue(), "EngineReady: %+v", c.Status.GetCondition(v1beta1.EngineReady))
		g.Expect(c.Status.IsConditionReady(knapis.ConditionReady)).To(BeTrue(), "Ready: %+v", c.Status.GetCondition(knapis.ConditionReady))
	}

	ginkgo.It("runs the replica to Ready with no service in front of it", func() {
		ir = standaloneReplica(ns.Name, replicaName, "fake-serving:v1", 2)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		wantSpec = *ir.Spec.DeepCopy()

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(fresh.Status.Replicas).To(Equal(int32(2)))
			g.Expect(fresh.Status.ReadyReplicas).To(Equal(int32(2)))
			expectReplicaPods(g, 2)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("fronts the replica with the engine's stable Service and mirrors its counters", func() {
		isvc = &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: ns.Name},
			Spec: v1beta1.InferenceServiceSpec{
				DeploymentMode: ptr.To(constants.OMENative),
				ReplicaRefs:    &v1beta1.ReplicaRefs{Engine: []string{replicaName}},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("keeping the stable Service selecting the replica's pods on the replica's port")
		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: engineService}, svc)).To(Succeed())
			owner := metav1.GetControllerOf(svc)
			g.Expect(owner).NotTo(BeNil(), "Service %s has no controller", engineService)
			g.Expect(owner.UID).To(Equal(isvc.UID), "Service %s must be controlled by the service", engineService)
			g.Expect(svc.Spec.Selector).To(Equal(map[string]string{
				constants.InferenceServicePodLabelKey: replicaName,
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			}))
			g.Expect(svc.Spec.Ports).To(HaveLen(1))
			g.Expect(svc.Spec.Ports[0].Name).To(Equal(fakeServingPorts()[0].Name))
			g.Expect(svc.Spec.Ports[0].Port).To(Equal(fakeServingPorts()[0].ContainerPort))
			g.Expect(readyEndpointCount(g, ns.Name, engineService)).To(Equal(2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("reporting the replica's counters and Ready")
		Eventually(func(g Gomega) {
			expectServiceMirror(g, 2)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The service projects no replica of its own; the one it fronts is as
		// the user wrote it.
		projected := client.ObjectKey{Namespace: ns.Name, Name: irprojector.InferenceReplicaName(serviceName, v1beta1.EngineComponent)}
		err := k8sClient.Get(suiteCtx, projected, &v1beta1.InferenceReplica{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the service must project no InferenceReplica %s; got err=%v", projected.Name, err)
		expectReplicaUntouched(Default)
	})

	ginkgo.It("follows the replica when the user scales it", func() {
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			patch := client.MergeFrom(fresh.DeepCopy())
			fresh.Spec.Replicas = ptr.To(int32(3))
			g.Expect(k8sClient.Patch(suiteCtx, fresh, patch)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		wantSpec.Replicas = ptr.To(int32(3))

		Eventually(func(g Gomega) {
			expectReplicaPods(g, 3)
			g.Expect(readyEndpointCount(g, ns.Name, engineService)).To(Equal(3))
			expectServiceMirror(g, 3)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		expectReplicaUntouched(Default)
	})

	ginkgo.It("leaves the replica serving when the service is deleted", func() {
		Expect(k8sClient.Delete(suiteCtx, isvc)).To(Succeed())

		// The stable Service goes with its controller owner; the replica's
		// pods are the replica's and stay.
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), &v1beta1.InferenceService{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the service should be fully deleted; got err=%v", err)
			err = k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: engineService}, &corev1.Service{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "Service %s should be garbage-collected with the service; got err=%v", engineService, err)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			expectReplicaPods(g, 3)
		}).WithTimeout(kindSteadyStateWindow).WithPolling(kindPollInterval).Should(Succeed())
		fresh := expectReplicaUntouched(Default)
		Expect(fresh.Status.ReadyReplicas).To(Equal(int32(3)))
	})
})
