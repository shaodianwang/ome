package multicluster

import (
	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

var _ = ginkgo.Describe("Multi-cluster routing cutoff", func() {
	const (
		acceleratorLabel = "gpu"
		acceleratorValue = "routing-test"
	)

	var (
		secretNS            string
		isvcNS              string
		secret1             string
		secret2             string
		wc1Name             string
		wc2Name             string
		isvcName            string
		publisherReconciler *placementendpoint.TrafficMapPublisherReconciler
	)

	ginkgo.BeforeEach(func() {
		skipIfRealKueue()
		publisherReconciler = nil
		secretNS = uniqueName("routing-secrets")
		isvcNS = uniqueName("routing-workload")
		secret1 = uniqueName("routing-kc-a")
		secret2 = uniqueName("routing-kc-b")
		wc1Name = uniqueName("routing-home-a")
		wc2Name = uniqueName("routing-home-b")
		isvcName = uniqueName("routing-svc")

		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret1))
		mustCreate(controlCluster, kubeconfigSecretFor(member2Cluster, secretNS, secret2))

		labels := map[string]string{acceleratorLabel: acceleratorValue}
		for _, wc := range []struct{ name, secret string }{{wc1Name, secret1}, {wc2Name, secret2}} {
			ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
				WorkloadClusterFor(wc.name, secretNS, wc.secret, labels),
				"WorkloadCluster Ready=True",
				func(o *v1beta1.WorkloadCluster) bool {
					return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
				})
		}
	})

	ginkgo.AfterEach(func() {
		if publisherReconciler != nil {
			cleanupTrafficMapPublication(
				publisherReconciler,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName},
			)
		}
		deleteIfExists(controlCluster, splitSourceISVC(
			isvcNS,
			isvcName,
			acceleratorLabel+"="+acceleratorValue,
			nil,
		))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	ginkgo.It("programs the Gateway route and zeroes an unready home", func() {
		routingReconciler := &placementrouting.Reconciler{
			Client:    controlCluster.client,
			APIReader: controlCluster.client,
			Log:       logr.Discard(),
			Config:    placementrouting.Config{Enabled: true},
		}
		publisher := placementendpoint.NewGatewayAPITrafficMapPublisher(
			controlCluster.client,
			controlCluster.client,
			placementendpoint.Config{
				GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.routing.example.com",
				GlobalGateway:      "gateway-system/shared-gateway",
				BackendPort:        443,
			},
		)
		publisherReconciler = &placementendpoint.TrafficMapPublisherReconciler{
			Client:    controlCluster.client,
			APIReader: controlCluster.client,
			Log:       logr.Discard(),
			Publisher: publisher,
			Active:    true,
		}
		key := types.NamespacedName{Namespace: isvcNS, Name: isvcName}

		ginkgo.By("fanning a split service out to both homes")
		mustCreate(controlCluster, splitSourceISVC(
			isvcNS,
			isvcName,
			acceleratorLabel+"="+acceleratorValue,
			&v1beta1.SplitSpec{Replicas: ptr.To[int32](2)},
		))
		gomega.Eventually(func(g gomega.Gomega) {
			_, found := getDerivedOn(member1Cluster, isvcNS, isvcName)
			g.Expect(found).To(gomega.BeTrue())
			_, found = getDerivedOn(member2Cluster, isvcNS, isvcName)
			g.Expect(found).To(gomega.BeTrue())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("admitting one ready replica with an endpoint on each home")
		setDerivedEndpointOn(member1Cluster, isvcNS, isvcName, "home-a.routing.example.com")
		setDerivedEndpointOn(member2Cluster, isvcNS, isvcName, "home-b.routing.example.com")
		upsertPlannedEngineStatusOn(member1Cluster, isvcNS, isvcName, admittedEngineInstances(1))
		upsertPlannedEngineStatusOn(member2Cluster, isvcNS, isvcName, admittedEngineInstances(1))
		setEngineReadyReplicasOn(member1Cluster, isvcNS, isvcName, 1)
		setEngineReadyReplicasOn(member2Cluster, isvcNS, isvcName, 1)

		ginkgo.By("waiting for placement to report both ready homes")
		gomega.Eventually(func(g gomega.Gomega) {
			placement := placementStatusOf(isvcNS, isvcName)
			g.Expect(placement).NotTo(gomega.BeNil())
			g.Expect(placement.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(placement.Candidates).To(gomega.HaveLen(2))
			for _, home := range []string{wc1Name, wc2Name} {
				candidate := routingCandidate(placement, home)
				g.Expect(candidate).NotTo(gomega.BeNil())
				g.Expect(candidate.AdmittedReplicas).To(gomega.Equal(int32(1)))
				g.Expect(candidate.ReadyReplicas).To(gomega.Equal(int32(1)))
				g.Expect(candidate.Endpoint).NotTo(gomega.BeNil())
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("generating the TrafficMap and programming its Gateway route")
		_, err := routingReconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		reconcileTrafficMapPublisher(publisherReconciler, key)
		gomega.Expect(publishedGatewayWeights(gomega.Default, isvcNS, isvcName)).To(gomega.Equal(
			map[string]int32{wc1Name: 1, wc2Name: 1},
		))

		ginkgo.By("making one admitted home unready")
		upsertPlannedEngineStatusOn(member2Cluster, isvcNS, isvcName, []v1beta1.OMENativeInstanceStatus{{
			Index:    0,
			Phase:    v1beta1.OMENativeInstancePending,
			Admitted: true,
		}})
		setEngineReadyReplicasOn(member2Cluster, isvcNS, isvcName, 0)

		ginkgo.By("waiting for placement to retain the admitted home with zero readiness")
		gomega.Eventually(func(g gomega.Gomega) {
			placement := placementStatusOf(isvcNS, isvcName)
			g.Expect(placement).NotTo(gomega.BeNil())
			unready := routingCandidate(placement, wc2Name)
			g.Expect(unready).NotTo(gomega.BeNil())
			g.Expect(unready.AdmittedReplicas).To(gomega.Equal(int32(1)))
			g.Expect(unready.ReadyReplicas).To(gomega.BeZero())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("cutting off the unready home in one routing and publisher reconcile")
		_, err = routingReconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		trafficMap := &v1beta1.TrafficMap{}
		gomega.Expect(controlCluster.client.Get(controlCluster.ctx, key, trafficMap)).To(gomega.Succeed())
		entries := make(map[string]v1beta1.TrafficMapEntry, len(trafficMap.Spec.Entries))
		for _, entry := range trafficMap.Spec.Entries {
			entries[entry.Cluster] = entry
		}
		gomega.Expect(entries).To(gomega.HaveLen(2))
		gomega.Expect(entries).To(gomega.HaveKey(wc1Name))
		gomega.Expect(entries).To(gomega.HaveKey(wc2Name))
		gomega.Expect(entries[wc1Name].Healthy).To(gomega.BeTrue())
		gomega.Expect(entries[wc1Name].Weight).To(gomega.Equal(int32(1)))
		gomega.Expect(entries[wc2Name].Healthy).To(gomega.BeFalse())
		gomega.Expect(entries[wc2Name].Weight).To(gomega.BeZero())

		_, err = publisherReconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(publishedGatewayWeights(gomega.Default, isvcNS, isvcName)).To(gomega.Equal(
			map[string]int32{wc1Name: 1, wc2Name: 0},
		))
	})
})

func setDerivedEndpointOn(member cluster, namespace, name, host string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		derived := &v1beta1.InferenceService{}
		key := types.NamespacedName{Namespace: namespace, Name: name}
		g.Expect(member.client.Get(member.ctx, key, derived)).To(gomega.Succeed())
		derived.Status.URL = apis.HTTPS(host)
		derived.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionTrue,
		})
		g.Expect(member.client.Status().Update(member.ctx, derived)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

func setEngineReadyReplicasOn(member cluster, namespace, name string, ready int32) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{
			Namespace: namespace,
			Name:      name + "-" + string(v1beta1.EngineComponent),
		}
		g.Expect(member.client.Get(member.ctx, key, ir)).To(gomega.Succeed())
		ir.Status.Replicas = 1
		ir.Status.ReadyReplicas = ready
		ir.Status.ServingReplicas = ready
		ir.Status.AvailableReplicas = ready
		g.Expect(member.client.Status().Update(member.ctx, ir)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

func routingCandidate(status *v1beta1.PlacementStatus, cluster string) *v1beta1.CandidatePlacement {
	if status == nil {
		return nil
	}
	for i := range status.Candidates {
		if status.Candidates[i].Cluster == cluster {
			return &status.Candidates[i]
		}
	}
	return nil
}

func reconcileTrafficMapPublisher(
	reconciler *placementendpoint.TrafficMapPublisherReconciler,
	key types.NamespacedName,
) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		_, err := reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		trafficMap := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, trafficMap)).To(gomega.Succeed())
		g.Expect(trafficMap.Status.Published).To(gomega.BeTrue())
		g.Expect(trafficMap.Status.ObservedTrafficMapGeneration).To(gomega.Equal(trafficMap.Generation))
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

func cleanupTrafficMapPublication(
	reconciler *placementendpoint.TrafficMapPublisherReconciler,
	key types.NamespacedName,
) {
	trafficMap := &v1beta1.TrafficMap{}
	if err := controlCluster.client.Get(controlCluster.ctx, key, trafficMap); apierrors.IsNotFound(err) {
		return
	} else {
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	}
	if trafficMap.DeletionTimestamp.IsZero() {
		gomega.ExpectWithOffset(1, controlCluster.client.Delete(controlCluster.ctx, trafficMap)).To(gomega.Succeed())
	}
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		_, err := reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		err = controlCluster.client.Get(controlCluster.ctx, key, &v1beta1.TrafficMap{})
		g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

func publishedGatewayWeights(g gomega.Gomega, namespace, name string) map[string]int32 {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	trafficMap := &v1beta1.TrafficMap{}
	g.Expect(controlCluster.client.Get(controlCluster.ctx, key, trafficMap)).To(gomega.Succeed())
	g.Expect(trafficMap.Status.Published).To(gomega.BeTrue())
	g.Expect(trafficMap.Status.ObservedTrafficMapGeneration).To(gomega.Equal(trafficMap.Generation))
	g.Expect(trafficMap.Status.GatewayRef).NotTo(gomega.BeNil())

	routeNamespace := trafficMap.Status.GatewayRef.Namespace
	g.Expect(routeNamespace).NotTo(gomega.BeEmpty())
	route := &gatewayapiv1.HTTPRoute{}
	g.Expect(controlCluster.client.Get(controlCluster.ctx, types.NamespacedName{
		Namespace: routeNamespace,
		Name:      trafficMap.Status.GatewayRef.Name,
	}, route)).To(gomega.Succeed())
	g.Expect(route.Spec.Rules).To(gomega.HaveLen(1))

	services := &corev1.ServiceList{}
	g.Expect(controlCluster.client.List(
		controlCluster.ctx,
		services,
		client.InNamespace(routeNamespace),
		client.MatchingLabels{
			placementendpoint.ManagedByLabel:                      placementendpoint.ManagedByValue,
			placementendpoint.PlacementEndpointISVCLabel:          name,
			placementendpoint.PlacementEndpointISVCNamespaceLabel: namespace,
		},
	)).To(gomega.Succeed())
	clusterByService := make(map[gatewayapiv1.ObjectName]string, len(services.Items))
	for i := range services.Items {
		service := &services.Items[i]
		cluster := service.Labels[placementendpoint.PlacementClusterLabel]
		g.Expect(cluster).NotTo(gomega.BeEmpty())
		clusterByService[gatewayapiv1.ObjectName(service.Name)] = cluster
	}

	refs := route.Spec.Rules[0].BackendRefs
	g.Expect(refs).To(gomega.HaveLen(len(clusterByService)))
	weights := make(map[string]int32, len(refs))
	for _, ref := range refs {
		cluster, found := clusterByService[ref.Name]
		g.Expect(found).To(gomega.BeTrue(), "HTTPRoute backend %q has no owned Service", ref.Name)
		g.Expect(ref.Weight).NotTo(gomega.BeNil())
		weights[cluster] = *ref.Weight
	}
	return weights
}
