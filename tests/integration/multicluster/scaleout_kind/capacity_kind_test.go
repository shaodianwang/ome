package scaleout_kind

import (
	"context"
	"encoding/json"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/acceleratorquota"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	"github.com/ome-projects/ome/pkg/quota/tree"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

const syntheticAccelerator = "example.com/placement-slot"

var _ = ginkgo.Describe("Hardware capacity placement (KIND)", func() {
	ginkgo.It("uses reported hardware independently of serving usage and rebalances within shared surge", func() {
		env := setupSpecEnv()
		label := uniqueName("capacity")
		for i, member := range env.members {
			setupCapacityHardware(member.c, label, []int64{4, 2}[i])
		}
		collect := capacityCollector(env)
		collect()
		model, runtime := env.seedModelRuntimeOnMembers()
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSplitByCapacity, 3)
		source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](3)}
		source.Spec.Placement.MaxSurge = ptr.To[int32](1)
		source.Spec.Engine.NodeSelector = map[string]string{syntheticAccelerator: label}
		resources := corev1.ResourceList{syntheticAccelerator: resource.MustParse("1")}
		source.Spec.Engine.Containers[0].Resources = corev1.ResourceRequirements{Requests: resources.DeepCopy(), Limits: resources}
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, member := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(member.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		publish := capacityRoutePublisher(env)
		key := client.ObjectKeyFromObject(source)
		await := func(want map[string]int32) {
			gomega.Eventually(func(g gomega.Gomega) {
				publish(g)
				g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
				g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
				g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
				got := map[string]int32{}
				for _, candidate := range source.Status.Placement.Candidates {
					got[candidate.Cluster] = candidate.Allocation.DesiredReplicas
					g.Expect(candidate.ReadyReplicas).To(gomega.Equal(want[candidate.Cluster]))
					g.Expect(candidate.Allocation.Capacity).NotTo(gomega.BeNil())
				}
				g.Expect(cmp.Diff(want, got)).To(gomega.BeEmpty())
				for _, m := range env.members {
					g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).To(gomega.Equal(int(want[m.wcName])))
				}
				g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		await(map[string]int32{env.members[0].wcName: 2, env.members[1].wcName: 1})
		accepted := source.Status.Placement.Plan.ID
		collect()
		await(map[string]int32{env.members[0].wcName: 2, env.members[1].wcName: 1})
		gomega.Expect(cmp.Diff(accepted, source.Status.Placement.Plan.ID)).To(gomega.BeEmpty(), "serving usage cannot reduce hardware-derived weights")
		for i, member := range env.members {
			setCapacityHardware(member.c, []int64{2, 4}[i])
		}
		collect()
		maxOccupied := 0
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			occupied := len(enginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)) + len(enginePodsOn(g, env.members[1].c, env.isvcNS, env.isvcName))
			maxOccupied = max(maxOccupied, occupied)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.ID).NotTo(gomega.Equal(accepted))
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(runningEnginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			g.Expect(runningEnginePodsOn(g, env.members[1].c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(maxOccupied).To(gomega.BeNumerically("<=", 4), "shared surge is one whole replica")
		await(map[string]int32{env.members[0].wcName: 1, env.members[1].wcName: 2})
		ginkgo.By("retiring a home while the source rendering changes")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{env.members[1].wcName}}}}}
			source.Spec.Engine.Containers[0].Env = []corev1.EnvVar{{Name: "SERVING_TEST_REVISION", Value: "updated"}}
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			occupied := len(enginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)) + len(enginePodsOn(g, env.members[1].c, env.isvcNS, env.isvcName))
			maxOccupied = max(maxOccupied, occupied)
			g.Expect(derivedSweptOn(env.members[0].c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
			g.Expect(runningEnginePodsOn(g, env.members[1].c, env.isvcNS, env.isvcName)).To(gomega.Equal(3))
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(maxOccupied).To(gomega.BeNumerically("<=", 4))
	})
})

func setupCapacityHardware(member cluster, label string, amount int64) {
	nodes := &corev1.NodeList{}
	gomega.Expect(member.client.List(member.ctx, nodes)).To(gomega.Succeed())
	gomega.Expect(nodes.Items).To(gomega.HaveLen(1))
	setupCapacityHardwareNodes(member, label, amount, nodes.Items)
}

func setupCapacityHardwareNodes(member cluster, label string, amount int64, nodes []corev1.Node) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace}
	gomega.Expect(member.client.Get(member.ctx, key, cm)).To(gomega.Succeed())
	before := cm.DeepCopy()
	raw, err := json.Marshal([]string{syntheticAccelerator})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cm.Data[controllerconfig.AcceleratorResourcesConfigName] = string(raw)
	gomega.Expect(member.client.Update(member.ctx, cm)).To(gomega.Succeed())
	flavor := &kueuev1beta2.ResourceFlavor{ObjectMeta: metav1.ObjectMeta{Name: label}, Spec: kueuev1beta2.ResourceFlavorSpec{NodeLabels: map[string]string{syntheticAccelerator: label}}}
	mustCreate(member, flavor)
	ginkgo.DeferCleanup(func() {
		deleteIfExists(member, flavor)
		gomega.Expect(member.client.Get(member.ctx, key, cm)).To(gomega.Succeed())
		cm.Data = before.Data
		gomega.Expect(member.client.Update(member.ctx, cm)).To(gomega.Succeed())
	})
	for _, node := range nodes {
		gomega.Expect(node.Labels).NotTo(gomega.HaveKey(syntheticAccelerator))
		gomega.Expect(node.Status.Capacity).NotTo(gomega.HaveKey(corev1.ResourceName(syntheticAccelerator)))
		base := node.DeepCopy()
		node.Labels[syntheticAccelerator] = label
		gomega.Expect(member.client.Patch(member.ctx, &node, client.MergeFrom(base))).To(gomega.Succeed())
		base = node.DeepCopy()
		node.Status.Capacity[syntheticAccelerator] = *resource.NewQuantity(amount, resource.DecimalSI)
		node.Status.Allocatable[syntheticAccelerator] = *resource.NewQuantity(amount, resource.DecimalSI)
		gomega.Expect(member.client.Status().Patch(member.ctx, &node, client.MergeFrom(base))).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			gomega.Expect(member.client.Get(member.ctx, client.ObjectKeyFromObject(&node), &node)).To(gomega.Succeed())
			base := node.DeepCopy()
			delete(node.Labels, syntheticAccelerator)
			gomega.Expect(member.client.Patch(member.ctx, &node, client.MergeFrom(base))).To(gomega.Succeed())
			base = node.DeepCopy()
			delete(node.Status.Capacity, syntheticAccelerator)
			delete(node.Status.Allocatable, syntheticAccelerator)
			gomega.Expect(member.client.Status().Patch(member.ctx, &node, client.MergeFrom(base))).To(gomega.Succeed())
		})
	}
}

func setCapacityHardware(member cluster, amount int64) {
	gomega.Eventually(func(g gomega.Gomega) {
		nodes := &corev1.NodeList{}
		g.Expect(member.client.List(member.ctx, nodes)).To(gomega.Succeed())
		g.Expect(nodes.Items).To(gomega.HaveLen(1))
		node := nodes.Items[0].DeepCopy()
		base := node.DeepCopy()
		node.Status.Capacity[syntheticAccelerator] = *resource.NewQuantity(amount, resource.DecimalSI)
		node.Status.Allocatable[syntheticAccelerator] = *resource.NewQuantity(amount, resource.DecimalSI)
		g.Expect(member.client.Status().Patch(member.ctx, node, client.MergeFrom(base))).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

func registerCapacityRootCleanup(ctx context.Context, cl client.Client, reconciler *acceleratorquota.Reconciler) {
	ginkgo.DeferCleanup(func() {
		root := &v1beta1.AcceleratorQuota{}
		key := client.ObjectKey{Name: v1beta1.AcceleratorQuotaRootName}
		if err := cl.Get(ctx, key, root); apierrors.IsNotFound(err) {
			return
		} else {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		gomega.Expect(cl.Delete(ctx, root)).To(gomega.Succeed())
		reconciler.Capacity = acceleratorquota.CapacityOptions{}
		gomega.Eventually(func(g gomega.Gomega) {
			_, err := reconciler.Reconcile(ctx, ctrl.Request{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(apierrors.IsNotFound(cl.Get(ctx, key, &v1beta1.AcceleratorQuota{}))).To(gomega.BeTrue())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
}

func capacityRoutePublisher(env *specEnv) func(gomega.Gomega) {
	gomega.Expect(kindutil.ApplyCRDs(controlCluster.ctx, controlCluster.cfg, []string{"tests/integration/dep-crds/gateway-api"})).To(gomega.Succeed())
	scheme := kindutil.BaseScheme()
	gomega.Expect(gatewayapiv1.Install(scheme)).To(gomega.Succeed())
	cl, err := client.New(controlCluster.cfg, client.Options{Scheme: scheme})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	routing := &placementrouting.Reconciler{Client: cl, APIReader: cl, Log: logr.Discard(), Config: placementrouting.Config{Enabled: true}}
	publisher := &placementendpoint.TrafficMapPublisherReconciler{Client: cl, APIReader: cl, Log: logr.Discard(), Active: true, Publisher: placementendpoint.NewGatewayAPITrafficMapPublisher(cl, cl, placementendpoint.Config{GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.example.com", GlobalGateway: env.isvcNS + "/gateway", BackendPort: 443})}
	key := client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName}
	ginkgo.DeferCleanup(func() {
		tm := &v1beta1.TrafficMap{}
		if err := cl.Get(controlCluster.ctx, key, tm); apierrors.IsNotFound(err) {
			return
		} else {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		gomega.Expect(cl.Delete(controlCluster.ctx, tm)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			_, err := publisher.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(apierrors.IsNotFound(cl.Get(controlCluster.ctx, key, &v1beta1.TrafficMap{}))).To(gomega.BeTrue())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
	return func(g gomega.Gomega) {
		_, err := routing.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = publisher.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func capacityCollector(env *specEnv) func() {
	var reporters []*acceleratorquota.Reconciler
	for _, m := range env.members {
		member := m.c
		reporter := &acceleratorquota.Reconciler{Client: member.client, APIReader: member.client, Scheme: member.client.Scheme(), Log: logr.Discard(), Options: tree.Options{RootName: v1beta1.AcceleratorQuotaRootName, MaxDepth: 5}, Capacity: acceleratorquota.CapacityOptions{Resources: []string{syntheticAccelerator}}}
		reporters = append(reporters, reporter)
		registerCapacityRootCleanup(member.ctx, member.client, reporter)
	}
	fleet := &acceleratorquota.Reconciler{Client: controlCluster.client, APIReader: controlCluster.client, Scheme: controlCluster.client.Scheme(), Log: logr.Discard(), Options: tree.Options{RootName: v1beta1.AcceleratorQuotaRootName, MaxDepth: 5}, Project: acceleratorquota.ProjectOptions{Clusters: controlCluster.clusters, Origin: controlPlaneID, FieldManager: "capacity-test"}}
	root := &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: v1beta1.AcceleratorQuotaRootName}, Spec: v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort}}
	mustCreate(controlCluster, root)
	registerCapacityRootCleanup(controlCluster.ctx, controlCluster.client, fleet)
	return func() {
		for i, reporter := range reporters {
			gomega.Eventually(func(g gomega.Gomega) {
				_, err := reporter.Reconcile(env.members[i].c.ctx, ctrl.Request{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
				r := &v1beta1.AcceleratorQuota{}
				g.Expect(env.members[i].c.client.Get(env.members[i].c.ctx, client.ObjectKey{Name: root.Name}, r)).To(gomega.Succeed())
				g.Expect(r.Status.Capacity).NotTo(gomega.BeEmpty())
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		gomega.Eventually(func(g gomega.Gomega) {
			_, err := fleet.Reconcile(controlCluster.ctx, ctrl.Request{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKey{Name: root.Name}, root)).To(gomega.Succeed())
			g.Expect(root.Status.Capacity).NotTo(gomega.BeEmpty())
			g.Expect(root.Status.Capacity[0].PerCluster).To(gomega.HaveLen(2))
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	}
}
