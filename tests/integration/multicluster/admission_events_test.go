package multicluster

import (
	"context"
	"maps"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Member admission metadata events", func() {
	ginkgo.It("wakes the source when only instance admission changes", func() {
		skipIfRealKueue()
		namespace := uniqueName("admission-events")
		createNamespace(controlCluster, namespace)
		createNamespace(member1Cluster, namespace)
		source := keeperISVC(namespace, uniqueName("service"))
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, source) })
		derived := placement.DeriveISVC(source, controlPlaneID, "")
		mustCreate(member1Cluster, derived)
		ginkgo.DeferCleanup(func() { deleteIfExists(member1Cluster, derived) })
		ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: derived.Name + "-engine", Labels: maps.Clone(derived.Labels), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(derived, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: derived.Name}, Component: v1beta1.EngineComponent, Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example/model-server:test"}}}}}}}}
		mustCreate(member1Cluster, ir)
		ginkgo.DeferCleanup(func() { deleteIfExists(member1Cluster, ir) })
		ctx, cancel := context.WithCancel(member1Cluster.ctx)
		defer cancel()
		manager := workloadcluster.NewManager(member1Cluster.client.Scheme())
		gomega.Expect(manager.Connect(ctx, "member-a", kubeConfigBytesFor(member1Cluster))).To(gomega.Succeed())
		defer manager.Disconnect("member-a")
		cfg := placement.AdmissionFunnelConfigFor(controlPlaneID)
		cfg.ResyncInterval = eventuallyPoll
		cfg.BufferSize = 4
		funnel := workloadcluster.NewStatusFunnel(manager, cfg)
		done := make(chan error, 1)
		go func() { done <- funnel.Start(ctx) }()
		defer func() { cancel(); gomega.Eventually(done, eventuallyTimeout).Should(gomega.Receive(gomega.Succeed())) }()
		receive := func() {
			var got event.GenericEvent
			gomega.Eventually(funnel.Events(), eventuallyTimeout).Should(gomega.Receive(&got))
			gomega.Expect(cmp.Diff(client.ObjectKeyFromObject(source), client.ObjectKeyFromObject(got.Object))).To(gomega.BeEmpty())
		}
		receive()
		before := derived.DeepCopy()
		for _, admitted := range []bool{true, false} {
			gomega.Expect(member1Cluster.client.Get(ctx, client.ObjectKeyFromObject(ir), ir)).To(gomega.Succeed())
			gomega.Expect(util.WriteDenseInstanceStatuses(ctx, member1Cluster.client, ir, []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstancePending, Admitted: admitted}})).To(gomega.Succeed())
			receive()
			gomega.Expect(member1Cluster.client.Get(ctx, client.ObjectKeyFromObject(derived), derived)).To(gomega.Succeed())
			gomega.Expect(cmp.Diff(before, derived)).To(gomega.BeEmpty(), "member service must stay unchanged when only IR admission changes")
		}
	})
})
